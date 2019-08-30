from multiprocessing import Queue, Process
from threading import Thread, Condition
import traceback
import time

from spacetime.managers.connectors.np_socket_manager import NPSocketServer, NPSocketConnector
from spacetime.managers.connectors.asyncio_socket_manager import AIOSocketServer, AIOSocketConnector
from spacetime.managers.connectors.thread_socket_manager import TSocketServer, TSocketConnector
from spacetime.managers.version_manager import VersionManager
from spacetime.managers.managed_heap import ManagedHeap
from spacetime.managers.diff import Diff
import spacetime.utils.enums as enums
import spacetime.utils.utils as utils
from spacetime.utils.utils import instrument_func

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
            mem_instrument=False):
        self.appname = appname
        self.logger = utils.get_logger("%s_Dataframe" % appname)
        self.instrument = instrument

        self.instrument_record = None

        self._shutdown = False
        if connection_as == enums.ConnectionStyle.TSocket:
            SocketServer, SocketConnector = TSocketServer, TSocketConnector
        elif connection_as == enums.ConnectionStyle.NPSocket:
            SocketServer, SocketConnector = NPSocketServer, NPSocketConnector
        elif connection_as == enums.ConnectionStyle.AIOSocket:
            SocketServer, SocketConnector = AIOSocketServer, AIOSocketConnector
        else:
            raise NotImplementedError()

        self.graph_change_event = Condition()
        self.socket_server = SocketServer(
            self.appname, server_port,
            self.fetch_call_back, self.push_call_back, self.confirm_fetch_req,
            self.instrument_record)

        self.socket_connector = SocketConnector(
            self.appname, details, self.details, types,
            self.instrument_record)

        self.types = types
        self.type_map = {
            tp.__r_meta__.name: tp for tp in self.types}

        # THis is the local snapshot.
        self.local_heap = ManagedHeap(types)
        self.versioned_heap = None

        # This is the dataframe's versioned graph.
        self.versioned_heap = VersionManager(
            self.appname, types, resolver, autoresolve, instrument=instrument)
        self.socket_server.start()
        if self.socket_connector.has_parent_connection:
            self.pull()

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

    def _create_package(self, appname, diff, start_version):
        return appname, [start_version, diff.version], diff

    def _check_updated_since(self, version, timeout):
        success = True
        with self.graph_change_event:
            success = self.graph_change_event.wait_for(
                lambda: version != self.versioned_heap.head,
                timeout if timeout > 0 else None)
        if not success:
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
        data, versions = self.versioned_heap.retrieve_data(
            self.appname,
            self.local_heap.version)
        if self.local_heap.receive_data(data, versions):
            # Can be carefully made Async.
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
            succ = self.versioned_heap.receive_data(
                self.appname, versions, data, from_external=False)
            with self.graph_change_event:
                self.graph_change_event.notify_all()
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
    @instrument_func("push")
    def push(self):
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Push request started.")
            data, version = self.versioned_heap.retrieve_data(
                "SOCKETPARENT", self.socket_connector.parent_version)
            if version[0] == version[1]:
                self.logger.debug(
                    "Push not required, "
                    "parent already has the information.")
                return

            if self.socket_connector.push_req(data, version):
                self.logger.debug("Push request completed.")
                self.versioned_heap.data_sent_confirmed(
                    "SOCKETPARENT", version)
                self.logger.debug("Push request registered.")
    
    @instrument_func("push_await")
    def push_await(self):
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Push request started.")
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
                self.versioned_heap.data_sent_confirmed(
                    "SOCKETPARENT", version)
                self.logger.debug("Push request registered.")

    @instrument_func("fetch")
    def fetch(self):
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Pull request started.")
            package, version = self.socket_connector.pull_req()
            self.logger.debug("Pull request completed.")
            self.versioned_heap.receive_data(
                "SOCKETPARENT",
                version, package)
            self.logger.debug("Pull request applied.")
            with self.graph_change_event:
                self.graph_change_event.notify_all()

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
            self.versioned_heap.receive_data(
                "SOCKETPARENT",
                version, package)
            self.logger.debug("Pull request applied.")
            with self.graph_change_event:
                self.graph_change_event.notify_all()


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
    def fetch_call_back(
            self, appname, version, req_types, wait=False, timeout=0):
        try:
            if wait:
                self._check_updated_since(version, timeout)
            return self.versioned_heap.retrieve_data(
                appname, version, req_types)
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise

    @instrument_func("confirm_fetch")
    def confirm_fetch_req(self, appname, version):
        try:
            self.versioned_heap.data_sent_confirmed(appname, version)
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise

    @instrument_func("accept_push")
    def push_call_back(self, appname, versions, data):
        try:
            resp = self.versioned_heap.receive_data(appname, versions, data)
            with self.graph_change_event:
                self.graph_change_event.notify_all()
            return resp
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise
