import socket
import cbor
import traceback
from threading import Thread
from struct import unpack, pack

from spacetime.utils.socket_utils import send_all, receive_data
from spacetime.utils import enums, utils

class Remote(Thread):
    @property
    def versions(self):
        return (self.version_from_self, self.version_from_remote)

    def __init__(
            self, ownname, remotename, version_graph, log_to_std=False):
        self.ownname = ownname
        self.remotename = remotename
        self.sock_as_client = None
        self.sock_as_server = None
        self.version_from_remote = "ROOT"
        self.version_from_self = "ROOT"
        self.version_graph = version_graph
        self.incoming_connection = False
        self.outgoing_connection = False
        self.logger = utils.get_logger(
            f"Remote_{ownname}<->{remotename}", log_to_std)
        super().__init__(daemon=True)

    def connect_as_client(self, location):
        req_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        req_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        req_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        req_socket.connect(location)
        self.sock_as_client = req_socket
        name = bytes(self.ownname, encoding="utf=8")
        self.sock_as_client.send(pack("!L", len(name)))
        send_all(self.sock_as_client, name)
        self.outgoing_connection = True

    def set_sock_as_server(self, socket_as_server):
        self.sock_as_server = socket_as_server
        self.incoming_connection = True
        self.start()

    def incoming_close(self):
        self.sock_as_server.close()
        self.incoming_connection = False

    def outgoing_close(self):
        self.sock_as_client.close()
        self.outgoing_connection = False

    def run(self):
        try:
            done = False
            while not done:
                # Unpack message length
                raw_cl = self.sock_as_server.recv(4)
                if not raw_cl:
                    break
                content_length = unpack("!L", raw_cl)[0]
                done = self.process_incoming_connection(
                    self.sock_as_server, content_length)
            self.sock_as_server.close()
        finally:
            self.delete_client()

    def delete_client(self):
        if self.sock_as_client:
            self.sock_as_client.close()
        if self.sock_as_server:
            self.sock_as_server.close()

    def process_incoming_connection(self, con, content_length):
        try:
            # Receive data
            raw_data = receive_data(con, content_length)
            data = cbor.loads(raw_data)
            # Get app name
            req_app = data[enums.TransferFields.AppName]
            # # Versions
            # versions = data[enums.TransferFields.Versions]
            wait = (
                data[enums.TransferFields.Wait]
                if enums.TransferFields.Wait in data else
                False)
            # Received push request.
            if data[enums.TransferFields.RequestType] is enums.RequestType.Push:
                # Actual payload
                package = data[enums.TransferFields.Data]
                remote_head = data[enums.TransferFields.Versions]
                # Send bool status back.
                if not wait:
                    con.send(pack("!?", True))
                self.accept_push(req_app, remote_head, package)
                if wait:
                    con.send(pack("!?", True))
                self.version_from_remote = remote_head
            # Received pull request.
            elif (data[enums.TransferFields.RequestType]
                  is enums.RequestType.Pull):
                timeout = data[enums.TransferFields.WaitTimeout] if wait else 0
                versions = data[enums.TransferFields.Versions]
                try:
                    dict_to_send, head, remote_refs = self.accept_fetch(
                        req_app, versions, wait=wait, timeout=timeout)
                    data_to_send = cbor.dumps({
                        enums.TransferFields.AppName: self.name,
                        enums.TransferFields.Data: {
                            "DATA": dict_to_send,
                            "REFS": remote_refs
                        },
                        enums.TransferFields.Status: enums.StatusCode.Success,
                        enums.TransferFields.Versions: head})
                    con.send(pack("!L", len(data_to_send)))
                    send_all(con, data_to_send)
                    if unpack("!?", con.recv(1))[0]:
                        self.version_graph.confirm_fetch(
                            "R-{0}".format(req_app), head)
                        self.version_from_remote = head
                except TimeoutError:
                    data_to_send = cbor.dumps({
                        enums.TransferFields.AppName: self.name,
                        enums.TransferFields.Status: enums.StatusCode.Timeout})
                    con.send(pack("!L", len(data_to_send)))
                    send_all(con, data_to_send)
            return False
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            return True


    def push(self, wait=False):
        diff_data, head, remote_refs = self.version_graph.get(
            self.remotename, self.versions)
        try:
            package = {
                enums.TransferFields.AppName: self.ownname,
                enums.TransferFields.RequestType: enums.RequestType.Push,
                enums.TransferFields.Versions: head,
                enums.TransferFields.Data: {
                    "DATA": diff_data,
                    "REFS": remote_refs
                },
                enums.TransferFields.Wait: wait
            }
            data = cbor.dumps(package)
            self.sock_as_client.send(pack("!L", len(data)))
            send_all(self.sock_as_client, data)
            succ = unpack("!?", self.sock_as_client.recv(1))[0]
            self.version_graph.confirm_fetch(
                "R-{0}".format(self.remotename), head)
            self.version_from_self = head
            return succ
        except Exception as e:
            print ("PUSH", e)
            print(traceback.format_exc())
            raise

    def fetch(self, wait=False, timeout=0):
        try:
            data = cbor.dumps({
                enums.TransferFields.AppName: self.ownname,
                enums.TransferFields.RequestType: enums.RequestType.Pull,
                enums.TransferFields.Versions: self.versions,
                enums.TransferFields.Wait: wait,
                enums.TransferFields.WaitTimeout: timeout,
            })
            self.sock_as_client.send(pack("!L", len(data)))
            send_all(self.sock_as_client, data)

            content_length = unpack("!L", self.sock_as_client.recv(4))[0]
            resp = receive_data(self.sock_as_client, content_length)
            data = cbor.loads(resp)
            if (wait and timeout > 0
                    and data[enums.TransferFields.Status]
                    == enums.StatusCode.Timeout):
                raise TimeoutError(
                    "No new version received in time {0}".format(
                        timeout))
            # Versions
            remote_head = data[enums.TransferFields.Versions]
            # Actual payload
            package = data[enums.TransferFields.Data]
            # Send bool status back.
            self.sock_as_client.send(pack("!?", True))
            self.version_from_self = remote_head
            self.version_graph.put(package["REFS"], package["DATA"])
        except TimeoutError:
            raise
        except Exception as e:
            print ("PULL", e)
            print(traceback.format_exc())
            raise

    def accept_push(self, req_app, remote_head, package):
        self.version_graph.put(package["REFS"], package["DATA"])

    def accept_fetch(self, req_app, versions, wait=False, timeout=0):
        if wait:
            self.version_graph.wait_for_change(versions, timeout=timeout)
        return self.version_graph.get(req_app, versions)
