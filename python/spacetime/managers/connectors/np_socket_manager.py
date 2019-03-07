from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from multiprocessing import RLock
from struct import pack, unpack
import socket
import cbor
import traceback

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


class NPSocketServer(Thread):
    def __init__(self, appname, server_port, pull_call_back, push_call_back, confirm_pull_req, instrument_q):
        # Logger for SocketManager
        self.logger = utils.get_logger("%s_SocketManager" % appname)

        # Number of workers in pool for connection
        self.worker_count = MAX_THREADPOOL_WORKERS

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

        super().__init__()
        self.daemon = True
        self.instrument_record = instrument_q
        # Need to have a per app RLock, so that the app that just pushed data,
        # does not ask for data that has not completely been pushed in.
        # Potential memory leak?
        # Another way to do it would be to keep persistent sockets.
        # We'll see that later.
        self.app_lock = dict()

    def setup_socket(self, server_port):
        sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sync_socket.settimeout(2)
        sync_socket.bind(("", server_port))
        sync_socket.listen()
        self.logger.debug("Socket bound")
        return sync_socket

    def raise_excp(self, fut):
        exp = fut.exception()
        if exp:
            self.logger.info("Exception found {0}".format(exp))
        else:
            self.logger.debug("Completed request {0}".format(fut.result()))

    def run(self):
        req_count = 0
        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            self.logger.debug("Running ThreadPool. Waiting for connections.")
            while True:
                try:
                    con, addr = self.sync_socket.accept()
                    self.logger.debug(
                        "Recv connection from %s, %d (%d)", addr[0], addr[1], req_count)
                    fut = executor.submit(self.incoming_connection, con, addr, req_count)
                    fut.add_done_callback(self.raise_excp)
                    self.logger.debug(
                        "Submitted processing req from %s, %d (%d)", addr[0], addr[1], req_count)
                    req_count+=1

                except Exception as e:
                    print ("RUN", e)
                    print(traceback.format_exc())
                    raise

    @instrument_func("handle_client")
    def incoming_connection(self, con, address, req_count):
        try:
            # Unpack message length
            self.logger.debug(
                "processing connection from %s, %d (%d)", address[0], address[1], req_count)
            # Get content length.
            content_length = unpack("!L", con.recv(4))[0]
            # Receive data
            raw_data = receive_data(con, content_length)
            self.logger.debug(
                "Recv raw data from %s, %d (%d)", address[0], address[1], req_count)
            data = cbor.loads(raw_data)
            self.logger.debug(
                "Converted data from %s, %d (%d)", address[0], address[1], req_count)
            # Get app name
            req_app = data[enums.TransferFields.AppName]
            # Versions
            versions = data[enums.TransferFields.Versions]
            # Received push request.
            if data[enums.TransferFields.RequestType] is enums.RequestType.Push:
                self.logger.debug("Processing push request. (%d)", req_count)
                # Actual payload
                package = data[enums.TransferFields.Data]
                # Send bool status back.
                with self.app_lock.setdefault(req_app, RLock()):
                    succ = self.push_call_back(req_app, versions, package)
                con.send(pack("!?", succ))
                self.logger.debug("Push complete. sent back ack. (%d)", req_count)
            # Received pull request.
            elif data[enums.TransferFields.RequestType] is enums.RequestType.Pull:
                self.logger.debug("Processing pull request. (%d)", req_count)
                with self.app_lock.setdefault(req_app, RLock()):
                    dict_to_send, new_versions = self.pull_call_back(req_app, versions)
                    self.logger.debug("Pull call back complete. sending back data. (%d)", req_count)
                    data_to_send = cbor.dumps({
                        enums.TransferFields.AppName: self.port,
                        enums.TransferFields.Data: dict_to_send,
                        enums.TransferFields.Versions: new_versions})
                    con.send(pack("!L", len(data_to_send)))
                    self.logger.debug("Pull complete. sent back data. (%d)", req_count)
                    send_all(con, data_to_send)
                    if unpack("!?", con.recv(1))[0]:
                        self.confirm_pull_req(req_app, new_versions)
                        self.logger.debug("Pull completed successfully. Recved ack. (%d)", req_count)
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise
        con.close()
        self.logger.debug("Incoming Connection closed. (%d)", req_count)
        return req_count


class NPSocketConnector(object):
    @property
    def has_parent_connection(self):
        return self.parent is not None

    def __init__(self, appname, parent, details, types, instrument_q):
        self.appname = appname
        self.details = details
        self.parent = parent
        self.parent_version = None
        self.instrument_record = instrument_q
        self.parent_version = "ROOT"
        # Logger for SocketManager
        self.logger = utils.get_logger("%s_SocketConnector" % self.appname)

    def get_new_version(self, new_versions):
        return new_versions[1]

    @instrument_func("send_pull")
    def pull_req(self):
        try:
            data = cbor.dumps({
                enums.TransferFields.AppName: self.appname,
                enums.TransferFields.RequestType: enums.RequestType.Pull,
                enums.TransferFields.Versions: self.parent_version
            })
            req_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # req_socket.settimeout(2)
            self.logger.debug("Connecting to parent.")
            req_socket.connect(self.parent)
            self.logger.debug("Client Connection successful, sending data (pull req)")
            req_socket.send(pack("!L", len(data)))
            send_all(req_socket, data)
            self.logger.debug("Data sent (pull req)")

            content_length = unpack("!L", req_socket.recv(4))[0]
            data = cbor.loads(receive_data(req_socket, content_length))
            self.logger.debug("Data received (pull req).")
            # Versions
            new_versions = data[enums.TransferFields.Versions]
            # Actual payload
            package = data[enums.TransferFields.Data]
            # Send bool status back.
            req_socket.send(pack("!?", True))
            self.logger.debug("Ack sent (pull req)")
            req_socket.close()
            self.parent_version = self.get_new_version(new_versions)
            return package, new_versions
        except Exception as e:
            print ("PULL", e)
            print(traceback.format_exc())
            raise

    @instrument_func("send_push")
    def push_req(self, diff_data, version):
        try:
            package = {
                enums.TransferFields.AppName: self.appname,
                enums.TransferFields.RequestType: enums.RequestType.Push,
                enums.TransferFields.Versions: version,
                enums.TransferFields.Data: diff_data
            }
            data = cbor.dumps(package)
            req_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # req_socket.settimeout(2)
            self.logger.debug("Connecting to parent.")
            req_socket.connect(self.parent)
            self.logger.debug("Client Connection successful, sending data (push req)")
            req_socket.send(pack("!L", len(data)))
            send_all(req_socket, data)
            self.logger.debug("Data sent (push req)")
            succ = unpack("!?", req_socket.recv(1))[0]
            self.logger.debug("Ack recv (push req)")
            req_socket.close()
            self.parent_version = self.get_new_version(version)
            return succ
        except Exception as e:
            print ("PUSH", e)
            print(traceback.format_exc())
            raise
