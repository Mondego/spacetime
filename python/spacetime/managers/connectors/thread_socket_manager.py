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
    def guarded_func(self, *args, **kwargs):
        if self.server_connection is None:
            return False
        try:
            return func(self, *args, **kwargs)
        except TimeoutError:
            raise
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
        try:
            done = False
            while not done:
                # Unpack message length
                self.logger.debug(
                    "processing connection from %s, %d",
                    self.address[0], self.address[1])
                # Get content length.
                raw_cl = self.client_sock.recv(4)
                if not raw_cl:
                    break
                content_length = unpack("!L", raw_cl)[0]
                done = self.incoming_connection(
                    self.client_sock, self.address, content_length)
            self.client_sock.close()
            self.logger.debug("Incoming Connection closed.")
        finally:
            self.delete_client(self)


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
            wait = (
                data[enums.TransferFields.Wait]
                if enums.TransferFields.Wait in data else
                False)
            # Received push request.
            if data[enums.TransferFields.RequestType] is enums.RequestType.Push:
                versions = (
                    data[enums.TransferFields.StartVersion],
                    data[enums.TransferFields.EndVersion])
                self.logger.debug("Processing push request.")
                # Actual payload
                package = data[enums.TransferFields.Data]
                # Send bool status back.
                if not wait:
                    con.send(pack("!?", True))
                self.push_call_back(req_app, versions, package)
                if wait:
                    con.send(pack("!?", True))
                self.logger.debug("Push complete. sent back ack.")
            # Received pull request.
            elif data[enums.TransferFields.RequestType] is enums.RequestType.Pull:
                self.logger.debug("Processing pull request.")
                timeout = data[enums.TransferFields.WaitTimeout] if wait else 0
                req_types = data[enums.TransferFields.Types]
                from_version = data[enums.TransferFields.StartVersion]
                try:
                    dict_to_send, new_versions = self.pull_call_back(
                        req_app, from_version, req_types,
                        wait=wait, timeout=timeout)
                    self.logger.debug(
                        "Pull call back complete. sending back data.")
                    start_v, end_v = new_versions
                    data_to_send = cbor.dumps({
                        enums.TransferFields.AppName: self.port,
                        enums.TransferFields.Data: dict_to_send,
                        enums.TransferFields.StartVersion: start_v,
                        enums.TransferFields.EndVersion: end_v,
                        enums.TransferFields.Status: enums.StatusCode.Success})
                    con.send(pack("!L", len(data_to_send)))
                    self.logger.debug("Pull complete. sent back data.")
                    send_all(con, data_to_send)
                    if unpack("!?", con.recv(1))[0]:
                        self.confirm_pull_req(req_app, new_versions)
                        self.logger.debug(
                            "Pull completed successfully. Recved ack.")
                except TimeoutError:
                    self.logger.debug(
                        "Pull call back timed out. sending back timeout.")
                    data_to_send = cbor.dumps({
                        enums.TransferFields.AppName: self.port,
                        enums.TransferFields.Status: enums.StatusCode.Timeout})
                    con.send(pack("!L", len(data_to_send)))
                    self.logger.debug("Pull complete. sent back data.")
                    send_all(con, data_to_send)
            return False
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            return True


class TSocketServer(Thread):
    @property
    def client_count(self):
        return len(self.clients)

    def __init__(
            self, appname, server_port,
            pull_call_back, push_call_back, confirm_pull_req, instrument_q):
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
        self.tpnames = {
            tp.__r_meta__.name: tp.__r_meta__.name_chain
            for tp in types
        }
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


    # @instrument("socket_pull_req")
    @instrument_func("fetch")
    @guarded
    def pull_req(self, wait=False, timeout=0):
        try:
            data = cbor.dumps({
                enums.TransferFields.AppName: self.appname,
                enums.TransferFields.RequestType: enums.RequestType.Pull,
                enums.TransferFields.StartVersion: self.parent_version,
                enums.TransferFields.Wait: wait,
                enums.TransferFields.WaitTimeout: timeout,
                enums.TransferFields.Types: self.tpnames
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
            if (wait 
                    and timeout > 0 
                    and data[enums.TransferFields.Status] 
                        == enums.StatusCode.Timeout):
                raise TimeoutError(
                    "No new version received in time {0}".format(
                        timeout))
            # Versions
            new_versions = (
                data[enums.TransferFields.StartVersion],
                data[enums.TransferFields.EndVersion])
            # Actual payload
            package = data[enums.TransferFields.Data]
            # Send bool status back.
            self.server_connection.send(pack("!?", True))
            self.logger.debug("Ack sent (pull req)")
            self.parent_version = data[enums.TransferFields.EndVersion]
            return package, new_versions
        except TimeoutError:
            raise
        except Exception as e:
            print ("PULL", e)
            print(traceback.format_exc())
            raise

    # @instrument("socket_push_req")
    @instrument_func("send_push")
    @guarded
    def push_req(self, diff_data, versions, wait=False):
        try:
            start_v, end_v = versions
            package = {
                enums.TransferFields.AppName: self.appname,
                enums.TransferFields.RequestType: enums.RequestType.Push,
                enums.TransferFields.StartVersion: start_v,
                enums.TransferFields.EndVersion: end_v,
                enums.TransferFields.Data: diff_data,
                enums.TransferFields.Wait: wait
            }
            data = cbor.dumps(package)
            self.server_connection.send(pack("!L", len(data)))
            send_all(self.server_connection, data)
            self.logger.debug("Data sent (push req)")
            succ = unpack("!?", self.server_connection.recv(1))[0]
            self.logger.debug("Ack recv (push req)")
            self.parent_version = end_v
            return succ
        except Exception as e:
            print ("PUSH", e)
            print(traceback.format_exc())
            raise
