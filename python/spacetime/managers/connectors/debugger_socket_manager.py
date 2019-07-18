from threading import Thread
import socket, traceback, cbor
import spacetime.utils.enums as enums
from struct import pack, unpack


class DebuggerSocketServer(Thread):

    def __init__(self, appname, server_port):
        self.appname = appname
        self.port = server_port
        self.sync_socket = self.setup_socket(self.port)
        self.data = cbor.dumps({enums.TransferFields.ParentAppName: self.appname})
        super().__init__()


    def setup_socket(self, server_port):
        sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sync_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sync_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # sync_socket.settimeout(2)
        sync_socket.bind(("", server_port))
        sync_socket.listen()
        return sync_socket

    def run(self):
        while True:
            try:
                con, addr = self.sync_socket.accept()
                con.send(pack("!L", len(self.data)))
                con.sendall(self.data)
                if unpack("!?", con.recv(1))[0]:
                    con.close()

            except Exception as e:
                print("RUN", e)
                print(traceback.format_exc())



class DebuggerSocketConnector(object):

    def __init__(self, appname, parent_details):
        self.parent_details = parent_details
        self.server_connection = self.connect_to_parent()
        pass

    def connect_to_parent(self):
        print("Parent details", self.parent_details)
        if self.parent_details is None:
            return None
        req_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        req_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        req_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # req_socket.settimeout(2)
        req_socket.connect(self.parent_details)
        return req_socket

    def get_parent_app_name(self):
        raw_cl = self.server_connection.recv(4)
        content_length = unpack("!L", raw_cl)[0]
        resp = self.server_connection.recv(content_length)
        data = cbor.loads(resp)
        parent_app_name = data[enums.TransferFields.ParentAppName]
        self.server_connection.send(pack("!?", True))
        return parent_app_name
