from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from multiprocessing import RLock
from struct import pack, unpack
import socket
import cbor
import traceback

import spacetime.utils.utils as utils
import spacetime.utils.enums as enums


MAX_THREADPOOL_WORKERS = 100

def receive_data(con, length):
    stack = list()
    while length:
        data = con.recv(length)
        stack.append(data)
        length = length - len(data) 
    
    return b"".join(stack)

def send_all(con, data):
    while data:
        sent = con.send(data)
        if len(data) == sent:
            break
        data = data[sent:]

def guarded(func):
    def guarded_func(self, *args):
        if self.server_connection is None:
            return False
        try:
            return func(self, *args)
        except OSError as e:
            # retry connection
            self.server_connection.close()
            self.server_connection = None
            return False
    return guarded_func


class ServingClient(Thread):
    def __init__(
            self, appname, server_port, address, client_sock,
            pull_call_back, push_call_back, confirm_pull_req, delete_client):
        self.logger = utils.get_logger("%s_SocketManager" % appname)

        # Call back function to process incoming pull requests. 
        self.pull_call_back = pull_call_back

        # Call back function to process incoming push requests.
        self.push_call_back = push_call_back

        # Call back function to confirm incoming pull requests to dataframe.
        self.confirm_pull_req = confirm_pull_req
        
        # The socket that can be used by children.
        self.server_port = server_port

        self.port = server_port

        self.address = address

        self.client_sock = client_sock

        self.delete_client = delete_client

        super().__init__()
        self.daemon = True

    def run(self):
        done = False
        while not done:
            done = self.incoming_connection(self.client_sock, self.address)
        self.client_sock.close()
        self.delete_client(self)
        self.logger.debug("Incoming Connection closed.")
     

    def incoming_connection(self, con, address):
        try:
            # Unpack message length
            self.logger.debug(
                "processing connection from %s, %d", address[0], address[1])
            # Get content length.
            content_length = unpack("!L", con.recv(4))[0]
            # Receive data
            raw_data = receive_data(con, content_length)
            self.logger.debug(
                "Recv raw data from %s, %d", address[0], address[1])
            data = cbor.loads(raw_data)
            self.logger.debug(
                "Converted data from %s, %d", address[0], address[1])
            # Get app name
            req_app = data[enums.TransferFields.AppName]
            # Versions
            versions = data[enums.TransferFields.Versions]
            # Received push request.
            if data[enums.TransferFields.RequestType] is enums.RequestType.Push:
                self.logger.debug("Processing push request.")
                # Actual payload
                package = data[enums.TransferFields.Data]
                # Send bool status back.
                succ = self.push_call_back(req_app, versions, package)
                con.send(pack("!?", succ))
                self.logger.debug("Push complete. sent back ack.")
            # Received pull request.
            elif data[enums.TransferFields.RequestType] is enums.RequestType.Pull:
                self.logger.debug("Processing pull request.")
                dict_to_send, new_versions = self.pull_call_back(req_app, versions)
                self.logger.debug("Pull call back complete. sending back data.")
                data_to_send = cbor.dumps({
                    enums.TransferFields.AppName: self.port,
                    enums.TransferFields.Data: dict_to_send,
                    enums.TransferFields.Versions: new_versions})
                con.send(pack("!L", len(data_to_send)))
                self.logger.debug("Pull complete. sent back data.")
                send_all(con, data_to_send)
                if unpack("!?", con.recv(1))[0]:
                    self.confirm_pull_req(req_app, new_versions)
                    self.logger.debug("Pull completed successfully. Recved ack.")
            return False
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            return True


class TSocketServer(Thread):
    def __init__(self, appname, server_port, pull_call_back, push_call_back, confirm_pull_req):
        self.appname = appname
        # Logger for SocketManager
        self.logger = utils.get_logger("%s_SocketManager" % appname)

        # Call back function to process incoming pull requests. 
        self.pull_call_back = pull_call_back

        # Call back function to process incoming push requests.
        self.push_call_back = push_call_back

        # Call back function to confirm incoming pull requests to dataframe.
        self.confirm_pull_req = confirm_pull_req
        
        # The socket that can be used by children.
        self.sync_socket = self.setup_socket(server_port)

        # The socket details.
        addr, port = self.sync_socket.getsockname()
        
        self.port = (addr if addr != "0.0.0.0" else "127.0.0.1", port)
        
        self.clients = set()
        super().__init__()
        self.daemon = True

    def setup_socket(self, server_port):
        sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sync_socket.settimeout(2)
        sync_socket.bind(("", server_port))
        sync_socket.listen()
        self.logger.debug("Socket bound")
        return sync_socket

    def has_parent_connection(self):
        return self.parent is not None

    def delete_client(self, client):
        self.clients.remove(client)

    def run(self):
        req_count = 0
        while True:
            try:
                con, addr = self.sync_socket.accept()
                self.logger.debug(
                    "Recv connection from %s, %d",
                    addr[0], addr[1])
                client_thread = ServingClient(
                    self.appname, self.port, addr, con,
                    self.pull_call_back, self.push_call_back,
                    self.confirm_pull_req, self.delete_client)

                self.clients.add(client_thread)
                client_thread.start()
            except Exception as e:
                print ("RUN", e)
                print(traceback.format_exc())
                raise


class TSocketConnector(object):
    @property
    def has_parent_connection(self):
        return self.parent is not None

    def __init__(self, appname, parent, details, types, version_by):
        self.appname = appname
        self.details = details
        self.parent = parent
        self.parent_version = None
        if version_by == enums.VersionBy.FULLSTATE:
            self.parent_version = "ROOT"
        elif version_by == enums.VersionBy.TYPE:
            self.parent_version = {
                tp.__r_meta__.name: "ROOT"
                for tp in types
            }
        elif version_by == enums.VersionBy.OBJECT_NOSTORE:
            self.parent_version = {
                tp.__r_meta__.name: dict()
                for tp in types
            }
        else:
            raise NotImplementedError()
        # Logger for SocketManager
        self.logger = utils.get_logger("%s_SocketConnector" % self.appname)
        self.version_by = version_by
        self.server_connection = self.connect_to_parent()

    def connect_to_parent(self):
        if self.parent is None:
            return None
        req_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # req_socket.settimeout(2)
        self.logger.debug("Connecting to parent.")
        req_socket.connect(self.parent)
        return req_socket
            

    def get_new_version(self, new_versions):
        if self.version_by == enums.VersionBy.FULLSTATE:
            return new_versions[1]
        elif self.version_by == enums.VersionBy.TYPE:
            versions = dict()
            versions.update(self.parent_version)
            versions.update({
                tpname: new_versions[tpname][1]
                for tpname in new_versions
            })
            return versions
        elif self.version_by == enums.VersionBy.OBJECT_NOSTORE:
            versions = dict()
            versions.update(self.parent_version)
            for tpname in new_versions:
                if tpname not in versions and new_versions[tpname]:
                    versions[tpname] = dict()
                for oid in new_versions[tpname]:
                    new_v = new_versions[tpname][oid][1]
                    if new_v == "END":
                        if oid in versions[tpname]:
                            del versions[tpname][oid]
                        continue
                    versions[tpname][oid] = new_v
            return versions
        else:
            raise NotImplementedError()

    @guarded
    def pull_req(self):
        try:
            data = cbor.dumps({
                enums.TransferFields.AppName: self.appname,
                enums.TransferFields.RequestType: enums.RequestType.Pull,
                enums.TransferFields.Versions: self.parent_version
            })
            self.logger.debug(
                "Client Connection successful, sending data (pull req)")
            self.server_connection.send(pack("!L", len(data)))
            send_all(self.server_connection, data)
            self.logger.debug("Data sent (pull req)")
            
            content_length = unpack("!L", self.server_connection.recv(4))[0]
            data = cbor.loads(
                receive_data(self.server_connection, content_length))
            self.logger.debug("Data received (pull req).")
            # Versions
            new_versions = data[enums.TransferFields.Versions]
            # Actual payload
            package = data[enums.TransferFields.Data]
            # Send bool status back.
            self.server_connection.send(pack("!?", True))
            self.logger.debug("Ack sent (pull req)")
            self.parent_version = self.get_new_version(new_versions)
            return package, new_versions
        except Exception as e:
            print ("PULL", e)
            print(traceback.format_exc())
            raise
        
    @guarded
    def push_req(self, diff_data, version):
        try:
            package = {
                enums.TransferFields.AppName: self.appname,
                enums.TransferFields.RequestType: enums.RequestType.Push,
                enums.TransferFields.Versions: version,
                enums.TransferFields.Data: diff_data
            }
            data = cbor.dumps(package)
            self.server_connection.send(pack("!L", len(data)))
            send_all(self.server_connection, data)
            self.logger.debug("Data sent (push req)")
            succ = unpack("!?", self.server_connection.recv(1))[0]
            self.logger.debug("Ack recv (push req)")
            self.parent_version = self.get_new_version(version)
            return succ
        except Exception as e:
            print ("PUSH", e)
            print(traceback.format_exc())
            raise
