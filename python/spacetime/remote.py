import socket
import cbor
import traceback
from threading import Thread, RLock
from struct import unpack, pack
from multiprocessing import Event

from spacetime.utils.socket_utils import send_all, receive_data
from spacetime.utils import enums, utils

class Remote(Thread):
    @property
    def versions(self):
        return (self.read_version, self.write_version)

    def __init__(
            self, ownname, remotename, version_graph, random,
            log_to_std=False, log_to_file=False):
        self.ownname = ownname
        self.remotename = remotename
        self.random = random
        self.remote_random = None
        self.sock_as_client = None
        self.sock_as_server = None
        self.read_version = "ROOT"
        self.write_version = "ROOT"
        self.version_graph = version_graph
        self.incoming_connection = False
        self.outgoing_connection = False
        self.logger = utils.get_logger(
            f"Remote_{ownname}<->{remotename}", log_to_std, log_to_file)
        self.transactions = dict()
        self.socket_protector = RLock()
        self.access_lock = RLock()
        self.incoming_free = Event()
        self.incoming_free.set()
        self.shutdown = False
        super().__init__(daemon=True)

    def connect_as_client(self, location):
        req_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        req_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        req_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        req_socket.connect(location)
        self.sock_as_client = req_socket
        #print("##### In connect_as_client", req_socket)
        name = cbor.dumps({self.ownname: self.random})
        self.sock_as_client.send(pack("!L", len(name)))
        send_all(self.sock_as_client, name)
        self.logger.info(f"Sent {self.ownname}: {self.random}")

    def receive_server_info(self):
        self.remote_random = unpack("!d", self.sock_as_client.recv(8))[0]
        self.logger.info(f"Received {self.remote_random}")
        self.outgoing_connection = True

    def set_sock_as_server(self, socket_as_server, remote_random):
        old_socket = self.sock_as_server
        self.sock_as_server = socket_as_server
        if old_socket:
            try:
                old_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.remote_random = remote_random
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
            try:
                raw_cl = self.sock_as_server.recv(4)
                if not raw_cl:
                    break
            except ConnectionResetError:
                break
            with self.socket_protector:
                content_length = unpack("!L", raw_cl)[0]
                if self.access_lock.acquire(self.remote_random < self.random):
                    try:
                        self.process_incoming_connection(content_length)
                    finally:
                        self.access_lock.release()
                else:
                    self.accept_and_reject(content_length)

    def accept_and_reject(self, content_length):
        # RECV REQ (FETCH 1) || REQ (PUSH 1)
        _ = receive_data(self.sock_as_server, content_length)
        # SEND LOCKREJECT (FETCH 2) (PUSH 2)
        data_to_send = cbor.dumps({
            enums.TransferFields.AppName: self.name,
            enums.TransferFields.Status: enums.StatusCode.LockReject})
        self.sock_as_server.send(pack("!L", len(data_to_send)))
        send_all(self.sock_as_server, data_to_send)
        self.logger.info("Rejected request for Lock.")

    def close(self):
        self.shutdown = True
        if self.sock_as_server:
            with self.socket_protector:
                try:
                    self.sock_as_server.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        try:
            self.join()
        except RuntimeError:
            pass

    def process_incoming_connection(self, content_length):
        try:
            # Receive data
            # RECV REQ (FETCH 1) || (PUSH 1)
            raw_data = receive_data(self.sock_as_server, content_length)
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
                if r_tid:
                    self.transactions[remote_head] = r_tid
                # Send bool status back.
                if not wait:
                    confirmed_v = self.version_graph.get_confirmed(req_app)
                    confirmed_t = list()
                    for v in confirmed_v:
                        if v in self.transactions:
                            self.logger.info(
                                f"Confirming Transation {self.transactions[v]} PUSH BEFORE")
                            confirmed_t.append(self.transactions[v])
                            del self.transactions[v]
                    data_to_send = cbor.dumps({
                        enums.TransferFields.AppName: self.name,
                        enums.TransferFields.Status: enums.StatusCode.Success,
                        enums.TransferFields.Confirmed: confirmed_t})
                    # SEND ACK NO WAIT (PUSH 2)
                    self.sock_as_server.send(pack("!L", len(data_to_send)))
                    send_all(self.sock_as_server, data_to_send)
                for c_tid in data[enums.TransferFields.Confirmed]:
                    self.version_graph._delete_old_reference(
                        self.remotename, c_tid)
                # if data[enums.TransferFields.Confirmed]:
                #     self.version_graph.pre_gc()
                self.logger.info(
                    f"Accept Push, {req_app}, {remote_head}, {package['REFS']}, {package['DATA']}")
                self.accept_push(req_app, remote_head, package)
                if wait:
                    confirmed_v = self.version_graph.get_confirmed(req_app)
                    confirmed_t = list()
                    for v in confirmed_v:
                        if v in self.transactions:
                            self.logger.info(
                                f"Confirming Transation {self.transactions[v]} PUSH AFTER")
                            confirmed_t.append(self.transactions[v])
                            del self.transactions[v]
                    data_to_send = cbor.dumps({
                        enums.TransferFields.AppName: self.name,
                        enums.TransferFields.Status: enums.StatusCode.Success,
                        enums.TransferFields.Confirmed: confirmed_t})
                    # SEND ACK WAIT (PUSH 2)
                    self.sock_as_server.send(pack("!L", len(data_to_send)))
                    send_all(self.sock_as_server, data_to_send)
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
                    dict_to_send, head, remote_refs, tid, _ = self.accept_fetch(
                        req_app, versions, wait=wait, timeout=timeout)
                    confirmed_v = self.version_graph.get_confirmed(req_app)
                    confirmed_t = list()
                    for v in confirmed_v:
                        if v in self.transactions:
                            self.logger.info(
                                f"Confirming Transation {self.transactions[v]} PULL")
                            confirmed_t.append(self.transactions[v])
                            del self.transactions[v]
                    data_to_send = cbor.dumps({
                        enums.TransferFields.AppName: self.name,
                        enums.TransferFields.Data: {
                            "DATA": dict_to_send,
                            "REFS": remote_refs
                        },
                        enums.TransferFields.Status: enums.StatusCode.Success,
                        enums.TransferFields.Versions: head,
                        enums.TransferFields.TransactionId: tid, 
                        enums.TransferFields.Confirmed: confirmed_t})

                    self.logger.info(
                        f"Accept Fetch, {req_app}, {versions}, "
                        f"{head}, {remote_refs}, {cbor.loads(data_to_send)}")
                    # SEND RESP (FETCH 2)
                    self.sock_as_server.send(pack("!L", len(data_to_send)))
                    send_all(self.sock_as_server, data_to_send)
                    # RECV ACK (FETCH 3)
                    if unpack("!?", self.sock_as_server.recv(1))[0]:
                        # self.version_graph.confirm_fetch(
                        #     "R-{0}".format(req_app), head)
                        self.read_version = (
                            package["REFS"]["W-{0}-{1}".format(
                                self.ownname, self.remotename)])
                except TimeoutError:
                    # SEND TIMEOUTER (FETCH 2)
                    data_to_send = cbor.dumps({
                        enums.TransferFields.AppName: self.name,
                        enums.TransferFields.Status: enums.StatusCode.Timeout})
                    self.sock_as_server.send(pack("!L", len(data_to_send)))
                    send_all(self.sock_as_server, data_to_send)
            return False
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            return True

    def push(self, wait=False):
        if not self.outgoing_connection:
            return
        confirmed = list()
        while not self._push(confirmed, wait=wait):
            self.logger.info("Retrying push")

    def _push(self, confirmed, wait=False):
        with self.access_lock:
            diff_data, head, remote_refs, tid, prev = self.version_graph.get(
                self.remotename)
            try:
                confirmed_v = self.version_graph.get_confirmed(self.remotename)
                for v in confirmed_v:
                    if v in self.transactions:
                        self.logger.info(
                            f"Confirming Transation {self.transactions[v]} SEND PUSH")
                        confirmed.append(self.transactions[v])
                        del self.transactions[v]
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

                # Send REQ (PUSH 1)
                self.sock_as_client.send(pack("!L", len(data)))
                send_all(self.sock_as_client, data)

                # RECV ACK WAIT || NO WAIT || LOCKREJECT (PUSH 2)
                resp_length = unpack("!L", self.sock_as_client.recv(4))[0]
                resp = cbor.loads(receive_data(self.sock_as_client, resp_length))
                if resp[enums.TransferFields.Status] == enums.StatusCode.LockReject:
                    self.version_graph.reset_get(self.remotename, tid, prev)
                    self.logger.info("Lock Rejected on push")
                    return False
                for r_tid in resp[enums.TransferFields.Confirmed]:
                    self.version_graph._delete_old_reference(self.remotename, r_tid)
                # self.version_graph.confirm_fetch(
                #     "R-{0}".format(self.remotename), head)
                self.read_version = (
                    remote_refs["W-{0}-{1}".format(self.ownname, self.remotename)])
            except Exception as e:
                print ("PUSH", e)
                print(traceback.format_exc())
                raise
        return True

    def fetch(self, wait=False, timeout=0):
        if not self.outgoing_connection:
            return
        confirmed = list()
        while not self._fetch(confirmed, wait=wait, timeout=timeout):
            self.logger.info("Retrying fetch")

    def _fetch(self, confirmed, wait=False, timeout=0):
        with self.access_lock:
            try:
                confirmed_v = self.version_graph.get_confirmed(self.remotename)
                for v in confirmed_v:
                    if v in self.transactions:
                        self.logger.info(
                            f"Confirming Transation {self.transactions[v]} SEND FETCH")
                        confirmed.append(self.transactions[v])
                        del self.transactions[v]
                data = cbor.dumps({
                    enums.TransferFields.AppName: self.ownname,
                    enums.TransferFields.RequestType: enums.RequestType.Pull,
                    enums.TransferFields.Versions: self.versions,
                    enums.TransferFields.Wait: wait,
                    enums.TransferFields.WaitTimeout: timeout,
                    enums.TransferFields.Confirmed: confirmed
                })

                # Send REQ (FETCH 1)
                self.sock_as_client.send(pack("!L", len(data)))
                send_all(self.sock_as_client, data)

                # RECV RESP (FETCH 2) || TIMEOUTER (FETCH 2) || LOCKREJECT (FETCH 2)
                content_length = unpack("!L", self.sock_as_client.recv(4))[0]
                resp = cbor.loads(
                    receive_data(self.sock_as_client, content_length))
                try:
                    if resp[enums.TransferFields.Status] == enums.StatusCode.LockReject:
                        self.logger.info("Lock Rejected on fetch")
                        return False
                except Exception:
                    self.logger.info(f"DATA {resp}")
                    raise

                if (wait and timeout > 0
                        and resp[enums.TransferFields.Status]
                        == enums.StatusCode.Timeout):
                    raise TimeoutError(
                        "No new version received in time {0}".format(
                            timeout))
                # Versions
                remote_head = resp[enums.TransferFields.Versions]
                # Actual payload
                package = resp[enums.TransferFields.Data]
                # Send bool status back.
                r_tid = resp[enums.TransferFields.TransactionId]
                if r_tid:
                    self.transactions[remote_head] = r_tid
                for c_tid in resp[enums.TransferFields.Confirmed]:
                    self.version_graph._delete_old_reference(self.remotename, c_tid)
                # SEND ACK (FETCH 3)
                self.sock_as_client.send(pack("!?", True))
                self.write_version = (
                    package["REFS"]["W-{0}-{1}".format(
                        self.remotename, self.ownname)])
                self.logger.info(
                    f"Fetch, {self.versions}, {self.remotename}, "
                    f"{remote_head}, {package['REFS']}")
                # if data[enums.TransferFields.Confirmed]:
                #     self.version_graph.pre_gc()
                self.version_graph.put(
                    self.remotename, package["REFS"], package["DATA"])
            except TimeoutError:
                raise
            except Exception as e:
                print ("PULL", e)
                print(traceback.format_exc())
                raise
        return True

    def accept_push(self, req_app, remote_head, package):
        self.version_graph.put(req_app, package["REFS"], package["DATA"])

    def accept_fetch(self, req_app, versions, wait=False, timeout=0):
        if wait:
            self.version_graph.wait_for_change(versions, timeout=timeout)
        return self.version_graph.get(req_app, versions)

    def wait_for_any_incoming(self):
        self.incoming_free.wait()