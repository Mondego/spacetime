import socket
import random
import cbor
from threading import Thread, RLock
from struct import pack, unpack

from spacetime.remote import Remote
from spacetime.version_graph import VersionGraph
from spacetime.heap import Heap
from spacetime.utils.socket_utils import receive_data, send_data
from spacetime.utils import utils


class Dataframe(Thread):
    @property
    def details(self):
        return self.main_socket.getsockname()

    def __init__(
            self, appname, types, server_port=0,
            remotes=None, resolver=None, log_to_std=False, log_to_file=False):
        self.appname = appname
        self.types = types
        self.shutdown = False
        self.log_to_std = log_to_std
        self.log_to_file = log_to_file
        self.logger = utils.get_logger(
            "Dataframe", self.log_to_std, self.log_to_file)
        self.random = random.random()
        super().__init__(daemon=True)
        # Set up socket that receives connections initiated by remotes.
        self.main_socket = self.setup_main_socket(server_port)
        self.server_port = self.main_socket.getsockname()[1]

        self.version_graph = VersionGraph(
            self.appname, self.types, resolver=resolver,
            log_to_std=log_to_std, log_to_file=log_to_file)

        self.remote_lock = RLock()
        # Initiates all connections to remotes.
        # map from Remote name -> Remote Obj
        with self.remote_lock:
            self.remotes = self._connect_to(remotes)
        for remote in self.remotes:
            self.remotes[remote].receive_server_info()

        self.heap = Heap(self.appname, types, self.version_graph)
        self.start()

    def setup_main_socket(self, server_port):
        sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sync_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sync_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sync_socket.bind(("", server_port))
        sync_socket.listen()
        return sync_socket

    def add_remote(self, name, details):
        self.logger.info(f"Adding new server {name}")
        with self.remote_lock:
            if name in self.remotes and self.remotes[name].outgoing_connection:
                self.remotes[name].outgoing_close()
                self.logger.info(f"Reset server {name}")
            if name not in self.remotes:
                self.remotes.update(self._connect_to({name: details}))
                self.logger.info(f"Connected to new server {name}")
            else:
                self.remotes[name].connect_as_client(details)
                self.logger.info(f"Reestablished server {name}")
        self.remotes[name].receive_server_info()

    def _connect_to(self, remotes):
        if not remotes:
            # No remote to connect to.
            return dict()
        return {
            remote: self._create_connection(remote, details)
            for remote, details in remotes.items()}

    def _create_connection(self, remote, details):
        remote_obj = Remote(
            self.appname, remote, self.version_graph, self.random,
            log_to_std=self.log_to_std, log_to_file=self.log_to_file)
        remote_obj.connect_as_client(details)
        self.logger.info(f"Obtained a new server {remote}")
        return remote_obj

    def run(self):
        self.listen()
        self.logger.info("Shutting down dataframe.")
        self.main_socket.close()
        for remote in self.remotes.values():
            remote.close()

    def listen(self):
        while not self.shutdown:
            con_socket, _ = self.main_socket.accept()
            with self.remote_lock:
                raw_nl = con_socket.recv(4)
                print (raw_nl)
                if not raw_nl:
                    continue
                name_length = unpack("!L", raw_nl)[0]
                name_dict = cbor.loads(receive_data(con_socket, name_length))
                self.logger.info(f"Received {name_dict}")
                con_socket.send(pack("!d", self.random))
                self.logger.info(f"Sent {self.random}")
                name = list(name_dict.keys()).pop()
                remote_random = name_dict[name]

                self.logger.info(f"Obtained a new client {name} {remote_random}")
                if name in self.remotes and self.remotes[name].incoming_connection:
                    self.remotes[name].incoming_close()
                    self.logger.info(f"Reset client {name}")
                if name not in self.remotes:
                    self.remotes[name] = Remote(
                        self.appname, name, self.version_graph, self.random,
                        log_to_std=self.log_to_std,
                        log_to_file=self.log_to_file)
                    self.logger.info(f"Created new client {name}")
                self.remotes[name].set_sock_as_server(con_socket, remote_random)

    # Heap Functions
    def add_one(self, dtype, obj):
        self.heap.add(dtype, [obj])

    def add_many(self, dtype, objs):
        self.heap.add(dtype, objs)

    def delete_one(self, dtype, obj):
        self.heap.delete(dtype, [obj])

    def delete_many(self, dtype, objs):
        self.heap.delete(dtype, objs)

    def delete_all(self, dtype):
        self.heap.delete(dtype, self.read_all(dtype))

    def read_one(self, dtype, oid):
        return self.heap.read_one(dtype, oid)

    def read_all(self, dtype):
        return self.heap.read_all(dtype)

    def commit(self):
        self.heap.commit()

    def checkout(self):
        self.heap.checkout()

    def checkout_await(self, timeout=0):
        self.heap.checkout(wait=True, timeout=timeout)

    def push(self, remote=None):
        if remote and remote in self.remotes:
            self.remotes[remote].push()
            return
        for remote_obj in self.remotes.values():
            remote_obj.push()

    def push_await(self, remote=None):
        if remote and remote in self.remotes:
            self.remotes[remote].push(wait=True)
            return
        for remote_obj in self.remotes.values():
            remote_obj.push(wait=True)

    def fetch(self, remote=None):
        if remote and remote in self.remotes:
            self.remotes[remote].fetch()
            return
        for remote_obj in self.remotes.values():
            remote_obj.fetch()

    def fetch_await(self, remote=None, timeout=0):
        if remote and remote in self.remotes:
            self.remotes[remote].fetch(wait=True, timeout=timeout)
            return
        for remote_obj in self.remotes.values():
            remote_obj.fetch(wait=True, timeout=timeout)

    def pull(self, remote=None):
        self.fetch(remote=remote)
        self.checkout()

    def pull_await(self, remote=None, timeout=0):
        self.fetch_await(remote=remote, timeout=timeout)
        self.checkout()

    def sync(self):
        self.commit()
        self.push()
        self.pull()

    def close(self):
        self.shutdown = True
        close_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        close_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        close_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        close_socket.connect(("127.0.0.1", self.server_port))
        close_socket.close()
        self.join()
