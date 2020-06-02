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
import cbor
from spacetime.utils.utils import instrument_func
from spacetime.utils.enums import Event
import repository


class DataframeCPP(object):
    @property
    def details(self):
        return "127.0.0.1", self.server_port

    @property
    def shutdown(self):
        return self._shutdown

    def write_stats(self):
        if self.instrument_record:
            self.instrument_record.put("DONE")
            self.instrument_writer.join()

    def __del__(self):
        print("deleting DF")
        # del self.repository

    def force_del_repo(self):
        print("force deleting DF")
        del self.repository

    def __init__(
            self, appname, types, details=None, server_port=0,
            connection_as=enums.ConnectionStyle.TSocket,
            instrument=None, dump_graph=None, resolver=None,
            autoresolve=enums.AutoResolve.FullResolve,
            mem_instrument=False):

        self.server_port = server_port
        self.appname = appname
        self.logger = utils.get_logger("%s_Dataframe" % appname)
        self.instrument = instrument

        self.instrument_record = None

        self._shutdown = False
        if connection_as != enums.ConnectionStyle.TSocket:
            raise NotImplementedError()

        self.types = types
        self.type_map = {
            tp.__r_meta__.name: tp for tp in self.types}

        type_info = {
            tp.__r_meta__.name: (
                [dimname for dimname in tp.__r_meta__.dimmap],
                tp.__r_meta__.name_chain
            )
            for tp in self.types
        }

        self.resolver = resolver

        # This is the local snapshot.
        self.local_heap = ManagedHeap(types)

        # if resolver is None:
        #     self.repository = repository.Repository(
        #         appname,
        #         None,
        #         None,
        #         cbor.dumps(type_info),
        #         autoresolve
        #     )
        # else:
        #     self.repository = repository.Repository(
        #         appname,
        #         lambda tpname: self.__has_resolver_for_type__(tpname),
        #         lambda dtpname, oid, original_data, new_data, conflicting_data, new_obj_change, conf_obj_change:
        #         cbor.dumps(self.__custom_merge__(dtpname, oid,
        #                                          None if original_data is None else cbor.loads(original_data),
        #                                          None if new_data is None else cbor.loads(new_data),
        #                                          None if conflicting_data is None else cbor.loads(conflicting_data),
        #                                          None if new_obj_change is None else cbor.loads(new_obj_change),
        #                                          None if conf_obj_change is None else cbor.loads(conf_obj_change),
        #                                          )),
        #         cbor.dumps(type_info),
        #         autoresolve
        #     )

        if resolver is None:
            self.repository = repository.Repository(
                appname,
                None,
                cbor.dumps(type_info),
                autoresolve
            )
        else:
            self.repository = repository.Repository(
                appname,
                self,
                cbor.dumps(type_info),
                autoresolve
            )

        if server_port != 0:
            self.repository.start_server(server_port, 10)
        if details is not None:
            self.repository.connect_to(str(details[0]), details[1])
        if self.repository.is_connected():
            self.pull()
        # This is the dataframe's versioned graph.
        # self.versioned_heap = VersionManager(
        #     self.appname, types, resolver, autoresolve, instrument=instrument)
        # self.socket_server.start()
        # if self.socket_connector.has_parent_connection:
        #     self.pull()

    # Suppport Functions

    # def _save_instruments(self):
    #     import os
    #     ifile = open(self.instrument + ".tsv", "w")
    #     mfile = open(self.instrument + ".vg.tsv", "w")
    #     while not self.instrument_done:
    #         record = self.instrument_record.get()
    #         if record == "DONE":
    #             break
    #         if record[0] == "MEMORY":
    #             mfile.write(record[1])
    #             mfile.flush()
    #             os.fsync(mfile.fileno())
    #         else:
    #             ifile.write(record)
    #             ifile.flush()
    #             os.fsync(ifile.fileno())

    def _check_updated_since(self, version, timeout):
        success = self.repository.wait_version(version, timeout)
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
        # print(dtype)
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
        data, start_v, end_v = cbor.loads(self.repository.retrieve_data(self.appname, self.local_heap.version))
        if self.local_heap.receive_data(data, (start_v, end_v)):
            self.repository.data_sent_confirmed(self.appname, start_v, end_v)
        # data, versions = self.versioned_heap.retrieve_data(
        #     self.appname,
        #     self.local_heap.version)
        # if self.local_heap.receive_data(data, versions):
        #     # Can be carefully made Async.
        #     self.versioned_heap.data_sent_confirmed(
        #         self.appname, versions)

    # @instrument_func("checkout_await")
    def checkout_await(self, timeout=0):
        self._check_updated_since(self.local_heap.version, timeout)
        self.checkout()

    # @instrument_func("commit")
    def commit(self):
        data, versions = self.local_heap.retreive_data()
        if versions:
            succ = self.repository.receive_data(self.appname, versions[0], versions[1], cbor.dumps(data), False)
            # succ = self.versioned_heap.receive_data(
            #     self.appname, versions, data, from_external=False)
            # with self.graph_change_event:
            #     self.graph_change_event.notify_all()
            if succ:
                self.local_heap.data_sent_confirmed(versions)

    def sync(self):
        self.commit()
        if self.repository.is_connected():
            self.push()
            self.pull()
        self.checkout()
        return True

    # Push and Pull
    # @instrument_func("push")
    def push(self):
        if not self.repository.is_connected():
            return
        self.repository.push()
        # if self.socket_connector.has_parent_connection:
        #     self.logger.debug("Push request started.")
        #     data, version = self.versioned_heap.retrieve_data(
        #         "SOCKETPARENT", self.socket_connector.parent_version)
        #     if version[0] == version[1]:
        #         self.logger.debug(
        #             "Push not required, "
        #             "parent already has the information.")
        #         return
        #
        #     if self.socket_connector.push_req(data, version):
        #         self.logger.debug("Push request completed.")
        #         self.versioned_heap.data_sent_confirmed(
        #             "SOCKETPARENT", version)
        #         self.logger.debug("Push request registered.")

    # @instrument_func("push_await")
    def push_await(self):
        self.repository.push_await()
        # if self.socket_connector.has_parent_connection:
        #     self.logger.debug("Push request started.")
        #     data, version = self.versioned_heap.retrieve_data(
        #         "SOCKETPARENT", self.socket_connector.parent_version)
        #     if version[0] == version[1]:
        #         self.logger.debug(
        #             "Push not required, "
        #             "parent already has the information.")
        #         return
        #
        #     if self.socket_connector.push_req(
        #             data, version, wait=True):
        #         self.logger.debug("Push request completed.")
        #         self.versioned_heap.data_sent_confirmed(
        #             "SOCKETPARENT", version)
        #         self.logger.debug("Push request registered.")

    # @instrument_func("fetch")
    def fetch(self, instrument=False):
        if instrument:
            raise NotImplementedError
        self.repository.fetch()
        # if self.socket_connector.has_parent_connection:
        #     self.logger.debug("Pull request started.")
        #     package, version = self.socket_connector.pull_req()
        #     self.logger.debug("Pull request completed.")
        #     self.versioned_heap.receive_data(
        #         "SOCKETPARENT",
        #         version, package)
        #     self.logger.debug("Pull request applied.")
        #     with self.graph_change_event:
        #         self.graph_change_event.notify_all()
        #     if instrument:
        #         return package

    # @instrument_func("fetch_await")
    def fetch_await(self, timeout=0):
        self.repository.fetch_await(float(timeout))
        # if self.socket_connector.has_parent_connection:
        #     self.logger.debug("Pull request started.")
        #     try:
        #         package, version = self.socket_connector.pull_req(
        #             wait=True, timeout=timeout)
        #     except TimeoutError:
        #         self.logger.debug("Timed out fetch request.")
        #         raise
        #     self.logger.debug("Pull request completed.")
        #     self.versioned_heap.receive_data(
        #         "SOCKETPARENT",
        #         version, package)
        #     self.logger.debug("Pull request applied.")
        #     with self.graph_change_event:
        #         self.graph_change_event.notify_all()

    # @instrument_func("pull")
    def pull(self, instrument=False):
        if instrument:
            raise NotImplementedError
        self.fetch()
        self.checkout()
        # delta = self.fetch(instrument=instrument)
        # self.checkout()
        # if instrument:
        #     return delta

    # @instrument_func("pull_await")
    def pull_await(self, timeout=0):
        self.fetch_await(timeout=timeout)
        self.checkout()

    # Functions that respond to external requests
    #
    #     @instrument_func("accept_fetch")
    #     def fetch_call_back(
    #             self, appname, version, req_types, wait=False, timeout=0):
    #         try:
    #             if wait:
    #                 self._check_updated_since(version, timeout)
    #             return self.versioned_heap.retrieve_data(
    #                 appname, version, req_types)
    #         except Exception as e:
    #             print(e)
    #             print(traceback.format_exc())
    #             raise
    #
    #     @instrument_func("confirm_fetch")
    #     def confirm_fetch_req(self, appname, version):
    #         try:
    #             self.versioned_heap.data_sent_confirmed(appname, version)
    #         except Exception as e:
    #             print(e)
    #             print(traceback.format_exc())
    #             raise
    #
    #     @instrument_func("accept_push")
    #     def push_call_back(self, appname, versions, data):
    #         try:
    # j

    # Functions for cpp binding callback
    def name_pred(self, tpname):
        return self.__has_resolver_for_type__(tpname)

    def run_merge(self, dtpname, oid, original_data, new_data, conflicting_data, new_obj_change, conf_obj_change):
        return cbor.dumps(self.__custom_merge__(dtpname, oid,
                                                None if original_data is None else cbor.loads(original_data),
                                                None if new_data is None else cbor.loads(new_data),
                                                None if conflicting_data is None else cbor.loads(conflicting_data),
                                                None if new_obj_change is None else cbor.loads(new_obj_change),
                                                None if conf_obj_change is None else cbor.loads(conf_obj_change),
                                                ))

    # Functions for cpp binding callback
    def __has_resolver_for_type__(self, tpname):
        print("trying if there is resolver for " + tpname)
        print(self.resolver)
        result = tpname in self.type_map and self.type_map[tpname] in self.resolver
        print("trying result: " + str(result))
        return result

    @staticmethod
    def __construct_temp__(dtype, oid, temp_data):
        obj = utils.container()
        obj.__class__ = dtype
        obj.__r_oid__ = str(oid)
        obj.__r_temp__ = temp_data
        dtype.__r_table__.store_as_temp[oid] = dict()
        return obj

    def __custom_merge__(self, dtpname, oid, original_data, new_data, conflicting_data, new_obj_change,
                         conf_obj_change):
        dtype = self.type_map[dtpname]
        print("custom merging: " + dtype)
        original = None if original_data is None else DataframeCPP.__construct_temp__(
            dtype, oid, original_data)
        new = None if new_data is None else DataframeCPP.__construct_temp__(
            dtype, oid, new_data)
        conflicting = None if conflicting_data is None else DataframeCPP.__construct_temp__(
            dtype, oid, conflicting_data)

        obj = self.resolver[dtype](original, new, conflicting)
        if obj:
            obj.__r_temp__.update(dtype.__r_table__.store_as_temp[oid])
            changes = {
                "dims": obj.__r_temp__, "types": dict()}
            changes["types"][dtpname] = (
                Event.Modification if original is not None else Event.New)

            del dtype.__r_table__.store_as_temp[oid]
            return (self.dim_diff(dtpname, new_obj_change, changes),
                    self.dim_diff(dtpname, conf_obj_change, changes))
        else:
            # Object was deleted.
            return (
                {"types": {dtpname: Event.Delete}},
                {"types": {dtpname: Event.Delete}})

    def dim_diff(self, dtpname, original, new):
        # return dims that are in new but not in/different in the original.
        change = {"dims": dict(), "types": dict()}
        if not (original and "dims" in original):
            return new
        for dim in new["dims"]:
            if (dim not in original["dims"]
                    or original["dims"][dim] != new["dims"][dim]):
                # The dim is not in original or the dim is there,
                # but the values are different.
                # copy it.
                change["dims"][dim] = new["dims"][dim]
        change["types"].update(original["types"])
        change["types"].update(new["types"])
        change["types"][dtpname] = Event.Modification
        return change
