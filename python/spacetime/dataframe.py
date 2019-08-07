from multiprocessing import RLock, Queue, Process
from threading import Thread, Event
import queue
import traceback
import time


from spacetime.managers.connectors.np_socket_manager import NPSocketServer, NPSocketConnector
from spacetime.managers.connectors.asyncio_socket_manager import AIOSocketServer, AIOSocketConnector
from spacetime.managers.connectors.thread_socket_manager import TSocketServer, TSocketConnector
from spacetime.managers.version_manager import FullStateVersionManager
from spacetime.managers.managed_heap import ManagedHeap
from spacetime.managers.diff import Diff
import spacetime.utils.enums as enums
import spacetime.utils.utils as utils
from spacetime.utils.utils import instrument_func

from spacetime.managers.connectors.debugger_socket_manager import DebuggerSocketServer, DebuggerSocketConnector

class Dataframe(object):
    @property
    def details(self):
        return self.socket_server.port

    @property
    def shutdown(self):
        return self._shutdown

    @property
    def client_count(self):
        return self.socket_server.client_count

    def write_stats(self):
        if self.instrument_record:
            self.instrument_record.put("DONE")
            self.instrument_writer.join()

    def __init__(
            self, appname, types, details=None, server_port=0,
            connection_as=enums.ConnectionStyle.TSocket,
            instrument=None, dump_graph=None, resolver=None,
            autoresolve=enums.AutoResolve.FullResolve,
            mem_instrument=False, use_debugger_sockets=False):
        self.appname = appname
        self.logger = utils.get_logger("%s_Dataframe" % appname)
        self.instrument = instrument

        if self.instrument:
            self.instrument_done = False
            self.instrument_record = Queue()
            self.instrument_writer = Process(target=self._save_instruments)
            self.instrument_writer.daemon = True
            self.instrument_writer.start()
        else:
            self.instrument_record = None

        self._shutdown = False
        self.types = types
        self.type_map = {
            tp.__r_meta__.name: tp for tp in self.types}

        # THis is the local snapshot.
        self.local_heap = ManagedHeap(types)
        self.write_lock = RLock()

        self.versioned_heap = None
        self.communication_queue = queue.Queue()
        self.communication_thread = Thread(target=self.communication_run)
        self.communication_thread.start()

        # This is the dataframe's versioned graph.
        self.versioned_heap = FullStateVersionManager(
            self.appname, types, dump_graph,
            self.instrument_record, resolver, autoresolve, mem_instrument, debug=use_debugger_sockets)
        if use_debugger_sockets:
            self.socket_server = DebuggerSocketServer(self.appname, server_port)
            self.socket_connector = None
            if details:
                self.socket_connector = DebuggerSocketConnector(self.appname, details)
            self.socket_server.start()
        else:
            if connection_as == enums.ConnectionStyle.TSocket:
                SocketServer, SocketConnector = TSocketServer, TSocketConnector
            elif connection_as == enums.ConnectionStyle.NPSocket:
                SocketServer, SocketConnector = NPSocketServer, NPSocketConnector
            elif connection_as == enums.ConnectionStyle.AIOSocket:
                SocketServer, SocketConnector = AIOSocketServer, AIOSocketConnector
            else:
                raise NotImplementedError()

            self.socket_server = SocketServer(
            self.appname, server_port,
            self.fetch_call_back, self.push_call_back, self.confirm_fetch_req,
            self.instrument_record)

            self.socket_connector = SocketConnector(
            self.appname, details, self.details, types,
            self.instrument_record)

            self.socket_server.start()
            print(self.appname, self.socket_server.getName())
            if self.socket_connector.has_parent_connection:
                self.pull()

    def communication_run(self):
        while True:
            req = self.communication_queue.get()
            if req[0] == "FETCH":
                self._execute_fetch()
                req[1].set()

            elif req[0] == "PUSH":
                self._execute_push()
                req[1].set()

    # Suppport Functions

    def _save_instruments(self):
        import os
        ifile = open(self.instrument + ".tsv", "w")
        mfile = open(self.instrument + ".vg.tsv", "w")
        while not self.instrument_done:
            record =  self.instrument_record.get()
            if record == "DONE":
                break
            if record[0] == "MEMORY":
                mfile.write(record[1])
                mfile.flush()
                os.fsync(mfile.fileno())
            else:
                ifile.write(record)
                ifile.flush()
                os.fsync(ifile.fileno())

    def _mem_usage(self):
        if self.versioned_heap.mem_instrument:
            return self.versioned_heap.mem_usage
        return list()

    def _create_package(self, appname, diff, start_version):
        return appname, [start_version, diff.version], diff

    def _check_updated_since(self, version, timeout):
        # TODO: Using a time loop for now. Should improve it later.
        stime = time.perf_counter()
        while version == self.versioned_heap.head:
            time.sleep(0.1)
            #print ("Server", timeout, version, self.versioned_heap.head)
            if timeout > 0 and (time.perf_counter() - stime) > timeout:
                raise TimeoutError(
                    "No new version received in time {0}".format(
                        timeout))


    # Object Create Add and Delete
    def add_one(self, dtype, obj):
        '''Adds one object to the staging.'''
        self.local_heap.add_one(dtype, obj)

    def add_many(self, dtype, objs):
        '''Adds many objects to the staging.'''
        self.local_heap.add_many(dtype, objs)

    def read_one(self, dtype, oid):
        '''Reads one object either from staging or
           last forked version if it exists.
           Returns None if no object is found.'''
        return self.local_heap.read_one(dtype, oid)

    def read_all(self, dtype):
        '''Returns a list of all objects of given type either
           from staging or last forked version.
           Returns empty list if no objects are found.'''
        return self.local_heap.read_all(dtype)

    def delete_one(self, dtype, obj):
        '''Deletes obj from staging first. If it exists
           in previous version, adds a delete record.'''
        self.local_heap.delete_one(dtype, obj)

    def delete_all(self, dtype):
        self.local_heap.delete_all(dtype)

    # Fork and Join

    @instrument_func("checkout")
    def checkout(self):
        with self.write_lock:
            data, versions = self.versioned_heap.retrieve_data(
                self.appname,
                self.local_heap.version)
        if self.local_heap.receive_data(data, versions):
            # Can be carefully made Async.
            with self.write_lock:
                self.versioned_heap.data_sent_confirmed(
                    self.appname, versions)

    @instrument_func("checkout_await")
    def checkout_await(self, timeout=0):
        self._check_updated_since(self.local_heap.version, timeout)
        self.checkout()

    @instrument_func("commit")
    def commit(self):
        data, versions = self.local_heap.retreive_data()
        if versions:
            with self.write_lock:
                succ = self.versioned_heap.receive_data(
                    self.appname, versions, data, from_external=False)
                self.garbage_collect(self.appname, versions[1])
            if succ:
                self.local_heap.data_sent_confirmed(versions)

    def sync(self):
        self.commit()
        if self.socket_connector.has_parent_connection:
            self.push()
            self.pull()
        self.checkout()
        return True

    # Push and Pull
    def push(self):
        e = Event()
        self.communication_queue.put(("PUSH", e))
        e.wait()

    @instrument_func("push")
    def _execute_push(self):
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Push request started.")
            with self.write_lock:
                data, version = self.versioned_heap.retrieve_data(
                    "SOCKETPARENT", self.socket_connector.parent_version)
                if version[0] == version[1]:
                    self.logger.debug(
                        "Push not required, "
                        "parent already has the information.")
                    return

            if self.socket_connector.push_req(data, version):
                self.logger.debug("Push request completed.")
                with self.write_lock:
                    self.versioned_heap.data_sent_confirmed(
                        "SOCKETPARENT", version)
                self.logger.debug("Push request registered.")
    
    @instrument_func("push_await")
    def push_await(self):
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Push request started.")
            with self.write_lock:
                data, version = self.versioned_heap.retrieve_data(
                    "SOCKETPARENT", self.socket_connector.parent_version)
                if version[0] == version[1]:
                    self.logger.debug(
                        "Push not required, "
                        "parent already has the information.")
                    return

            if self.socket_connector.push_req(
                    data, version, wait=True):
                self.logger.debug("Push request completed.")
                with self.write_lock:
                    self.versioned_heap.data_sent_confirmed(
                        "SOCKETPARENT", version)
                self.logger.debug("Push request registered.")

    def fetch(self):
        e = Event()
        self.communication_queue.put(("FETCH", e))
        e.wait()

    @instrument_func("fetch")
    def _execute_fetch(self):
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Pull request started.")
            package, version = self.socket_connector.pull_req()
            self.logger.debug("Pull request completed.")
            with self.write_lock:
                self.versioned_heap.receive_data(
                    "SOCKETPARENT",
                    version, package)
                self.garbage_collect("SOCKETPARENT", version[1])
            self.logger.debug("Pull request applied.")

    @instrument_func("fetch_await")
    def fetch_await(self, timeout=0):
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Pull request started.")
            try:
                package, version = self.socket_connector.pull_req(
                    wait=True, timeout=timeout)
            except TimeoutError:
                self.logger.debug("Timed out fetch request.")
                raise
            self.logger.debug("Pull request completed.")
            with self.write_lock:
                self.versioned_heap.receive_data(
                    "SOCKETPARENT",
                    version, package)
                self.garbage_collect("SOCKETPARENT", version[1])
            self.logger.debug("Pull request applied.")


    @instrument_func("pull")
    def pull(self):
        self.fetch()
        self.checkout()

    @instrument_func("pull_await")
    def pull_await(self, timeout=0):
        self.fetch_await(timeout=timeout)
        self.checkout()

    # Functions that respond to external requests

    @instrument_func("accept_fetch")
    def fetch_call_back(self, appname, version, wait=False, timeout=0):
        try:
            if wait:
                self._check_updated_since(version, timeout)
            with self.write_lock:
                return self.versioned_heap.retrieve_data(appname, version)
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise

    @instrument_func("confirm_fetch")
    def confirm_fetch_req(self, appname, version):
        try:
            with self.write_lock:
                self.versioned_heap.data_sent_confirmed(
                    appname, version)
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise

    @instrument_func("accept_push")
    def push_call_back(self, appname, versions, data):
        try:
            with self.write_lock:
                return_value = self.versioned_heap.receive_data(
                    appname, versions, data)
                self.garbage_collect(appname, versions[1])
                return return_value
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise

    def garbage_collect(self, appname, end_v):
        self.versioned_heap.maintain(appname,end_v)
