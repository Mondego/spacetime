from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from multiprocessing import RLock
from struct import pack, unpack
import socket
import cbor
import traceback, time

import spacetime.utils.utils as utils
import spacetime.utils.enums as enums
from spacetime.utils.utils import instrument_func

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
        except Exception:
            self.server_connection = None
            self.shutdown = True
            raise
    return guarded_func


class ServingClient(Thread):
    def __init__(
            self, appname, server_port, address, client_sock,
            pull_call_back, push_call_back, confirm_pull_req, delete_client, instrument_q):
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

        self.instrument_record = instrument_q

        super().__init__()
        self.daemon = True

    def run(self):
        done = False
        while not done:
            # Unpack message length
            self.logger.debug(
                "processing connection from %s, %d", self.address[0], self.address[1])
            # Get content length.
            raw_cl = self.client_sock.recv(4)
            if not raw_cl:
                break
            content_length = unpack("!L", raw_cl)[0]
            done = self.incoming_connection(self.client_sock, self.address, content_length)
        self.client_sock.close()
        self.delete_client(self)
        self.logger.debug("Incoming Connection closed.")


    @instrument_func("handle_client")
    def incoming_connection(self, con, address, content_length):
        try:
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
                con.send(pack("!?", True))
                self.push_call_back(req_app, versions, package)
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
    def __init__(self, appname, server_port, pull_call_back, push_call_back, confirm_pull_req, instrument_q):
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

        self.shutdown = False

        self.port = (addr if addr != "0.0.0.0" else "127.0.0.1", port)
        self.instrument_record = instrument_q
        self.clients = set()
        super().__init__()
        self.daemon = True

    def setup_socket(self, server_port):
        sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sync_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sync_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # sync_socket.settimeout(2)
        sync_socket.bind(("", server_port))
        sync_socket.listen()
        self.logger.debug("Socket bound")
        return sync_socket

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
                    self.confirm_pull_req, self.delete_client,
                    self.instrument_record)

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

    def __init__(self, appname, parent, details, types, instrument_q):
        self.appname = appname
        self.details = details
        self.parent = parent
        self.parent_version = None
        self.parent_version = "ROOT"
        # Logger for SocketManager
        self.logger = utils.get_logger("%s_SocketConnector" % self.appname)
        self.instrument_record = instrument_q
        self.server_connection = self.connect_to_parent()

    def connect_to_parent(self):
        if self.parent is None:
            return None
        req_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        req_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        req_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # req_socket.settimeout(2)
        self.logger.debug("Connecting to parent.")
        req_socket.connect(self.parent)
        return req_socket


    def get_new_version(self, new_versions):
        return new_versions[1]

    # @instrument("socket_pull_req")
    @instrument_func("fetch")
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
            resp = receive_data(self.server_connection, content_length)
            data = cbor.loads(resp)
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

    # @instrument("socket_push_req")
    @instrument_func("send_push")
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
