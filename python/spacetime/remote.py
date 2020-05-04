import socket
import cbor
import traceback
from threading import Thread, RLock
from struct import unpack, pack

from spacetime.utils.socket_utils import send_all, receive_data
from spacetime.utils import enums, utils

class Remote(Thread):
    @property
    def versions(self):
        return (self.read_version, self.write_version)

    def __init__(
            self, ownname, remotename, version_graph,
            log_to_std=False, log_to_file=False):
        self.ownname = ownname
        self.remotename = remotename
        self.sock_as_client = None
        self.sock_as_server = None
        self.read_version = "ROOT"
        self.write_version = "ROOT"
        self.version_graph = version_graph
        self.incoming_connection = False
        self.outgoing_connection = False
        self.logger = utils.get_logger(
            f"Remote_{ownname}<->{remotename}", log_to_std, log_to_file)
        self.confirmed_transactions = list()
        self.connection_lock = RLock()
        self.socket_protector = RLock()
        self.shutdown = False
        super().__init__(daemon=True)

    def connect_as_client(self, location):
        req_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        req_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        req_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        req_socket.connect(location)
        self.sock_as_client = req_socket
        print("##### In connect_as_client", req_socket)
        name = bytes(self.ownname, encoding="utf=8")
        self.sock_as_client.send(pack("!L", len(name)))
        send_all(self.sock_as_client, name)
        self.outgoing_connection = True

    def set_sock_as_server(self, socket_as_server):
        old_socket = self.sock_as_server
        self.sock_as_server = socket_as_server
        if old_socket:
            old_socket.shutdown(socket.SHUT_RDWR)
        self.incoming_connection = True
        if not old_socket:
            self.start()

    def incoming_close(self):
        self.sock_as_server.close()
        self.incoming_connection = False

    def outgoing_close(self):
        self.sock_as_client.close()
        self.outgoing_connection = False

    def run(self):
        while not self.shutdown:
            # Unpack message length
            raw_cl = self.sock_as_server.recv(4)
            if not raw_cl:
                break
            with self.socket_protector:
                content_length = unpack("!L", raw_cl)[0]
                self.process_incoming_connection(
                    self.sock_as_server, content_length)

    def close(self):
        self.shutdown = True
        if self.sock_as_server:
            with self.socket_protector:
                self.sock_as_server.shutdown(socket.SHUT_RDWR)
        self.join()

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
                r_tid = data[enums.TransferFields.TransactionId]
                # Send bool status back.
                if not wait:
                    with self.connection_lock:
                        resp = cbor.dumps(self.confirmed_transactions)
                        self.confirmed_transactions = list()
                    con.send(pack("!L", len(resp)))
                    send_all(con, resp)
                for c_tid in data[enums.TransferFields.Confirmed]:
                    self.version_graph._delete_old_reference(
                        self.remotename, c_tid)
                self.logger.info(
                    f"Accept Push, {req_app}, {remote_head}, {package['REFS']}, {package['DATA']}")
                self.accept_push(req_app, remote_head, package)
                with self.connection_lock:
                    self.confirmed_transactions.append(r_tid)
                if wait:
                    with self.connection_lock:
                        resp = cbor.dumps(self.confirmed_transactions)
                        self.confirmed_transactions = list()
                    con.send(pack("!L", len(resp)))
                    send_all(con, resp)
                self.write_version = (
                    package["REFS"]["W-{0}-{1}".format(
                        self.remotename, self.ownname)])
            
            # Received pull request.
            elif (data[enums.TransferFields.RequestType]
                  is enums.RequestType.Pull):
                timeout = data[enums.TransferFields.WaitTimeout] if wait else 0
                versions = data[enums.TransferFields.Versions]
                for c_tid in data[enums.TransferFields.Confirmed]:
                    self.version_graph._delete_old_reference(
                        self.remotename, c_tid)
                try:
                    dict_to_send, head, remote_refs, tid = self.accept_fetch(
                        req_app, versions, wait=wait, timeout=timeout)
                    with self.connection_lock:
                        confirmed = self.confirmed_transactions
                        self.confirmed_transactions = list()
                    data_to_send = cbor.dumps({
                        enums.TransferFields.AppName: self.name,
                        enums.TransferFields.Data: {
                            "DATA": dict_to_send,
                            "REFS": remote_refs
                        },
                        enums.TransferFields.Status: enums.StatusCode.Success,
                        enums.TransferFields.Versions: head,
                        enums.TransferFields.TransactionId: tid, 
                        enums.TransferFields.Confirmed: confirmed})

                    self.logger.info(
                        f"Accept Fetch, {req_app}, {versions}, "
                        f"{head}, {remote_refs}")
                    con.send(pack("!L", len(data_to_send)))
                    send_all(con, data_to_send)
                    if unpack("!?", con.recv(1))[0]:
                        # self.version_graph.confirm_fetch(
                        #     "R-{0}".format(req_app), head)
                        self.read_version = (
                            package["REFS"]["W-{0}-{1}".format(
                                self.ownname, self.remotename)])
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
        diff_data, head, remote_refs, tid = self.version_graph.get(
            self.remotename)
        try:
            with self.connection_lock:
                confirmed = self.confirmed_transactions
                self.confirmed_transactions = list()
            package = {
                enums.TransferFields.AppName: self.ownname,
                enums.TransferFields.RequestType: enums.RequestType.Push,
                enums.TransferFields.Versions: head,
                enums.TransferFields.Data: {
                    "DATA": diff_data,
                    "REFS": remote_refs
                },
                enums.TransferFields.Wait: wait,
                enums.TransferFields.TransactionId: tid,
                enums.TransferFields.Confirmed: confirmed
            }
            
            data = cbor.dumps(package)
            self.logger.info(f"Push, {self.remotename}, {head}, {remote_refs}")
            # print("##### In push")
            self.sock_as_client.send(pack("!L", len(data)))
            send_all(self.sock_as_client, data)
            resp_length = unpack("!L", self.sock_as_client.recv(4))[0]
            resp = cbor.loads(receive_data(self.sock_as_client, resp_length))
            for r_tid in resp:
                self.version_graph._delete_old_reference(self.remotename, r_tid)
            # self.version_graph.confirm_fetch(
            #     "R-{0}".format(self.remotename), head)
            self.read_version = (
                remote_refs["W-{0}-{1}".format(self.ownname, self.remotename)])
        except Exception as e:
            print ("PUSH", e)
            print(traceback.format_exc())
            raise

    def fetch(self, wait=False, timeout=0):
        try:
            with self.connection_lock:
                confirmed = self.confirmed_transactions
                self.confirmed_transactions = list()

            data = cbor.dumps({
                enums.TransferFields.AppName: self.ownname,
                enums.TransferFields.RequestType: enums.RequestType.Pull,
                enums.TransferFields.Versions: self.versions,
                enums.TransferFields.Wait: wait,
                enums.TransferFields.WaitTimeout: timeout,
                enums.TransferFields.Confirmed: confirmed
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
            r_tid = data[enums.TransferFields.TransactionId]
            for c_tid in data[enums.TransferFields.Confirmed]:
                self.version_graph._delete_old_reference(self.remotename, c_tid)
            self.sock_as_client.send(pack("!?", True))
            self.write_version = (
                package["REFS"]["W-{0}-{1}".format(
                    self.remotename, self.ownname)])
            self.logger.info(
                f"Fetch, {self.versions}, {self.remotename}, "
                f"{remote_head}, {package['REFS']}")
            self.version_graph.put(
                self.remotename, package["REFS"], package["DATA"])
            with self.connection_lock:
                self.confirmed_transactions.append(r_tid)
        except TimeoutError:
            raise
        except Exception as e:
            print ("PULL", e)
            print(traceback.format_exc())
            raise

    def accept_push(self, req_app, remote_head, package):
        self.version_graph.put(req_app, package["REFS"], package["DATA"])

    def accept_fetch(self, req_app, versions, wait=False, timeout=0):
        if wait:
            self.version_graph.wait_for_change(versions, timeout=timeout)
        return self.version_graph.get(req_app, versions)
