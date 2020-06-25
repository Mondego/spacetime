from spacetime.repository import Repository
from spacetime.managers.managed_heap import ManagedHeap
import spacetime.utils.enums as enums
import spacetime.utils.utils as utils
import cbor
import sys
from spacetime.utils.utils import instrument_func
from spacetime.utils.enums import Event

class DataframeCPP(object):
    @property
    def details(self):
        return "127.0.0.1", self.server_port

    @property
    def shutdown(self):
        return self._shutdown

    def __del__(self):
        pass
        # print("deleting DF")
        # del self.repository

    def force_del_repo(self):
        del self.repository

    def __init__(
            self, appname, types,
            details=None, server_port=0, resolver=None, is_server=False):
        self.appname = appname
        self.logger = utils.get_logger("%s_Dataframe" % appname)

        self._shutdown = False

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

        if resolver is None:
            self.repository = Repository(
                appname, None, cbor.dumps(type_info))
        else:
            self.repository = Repository(
                appname, self, cbor.dumps(type_info))

        self.server_port = (
            self.repository.start_server(server_port, 20)
            if is_server else
            server_port)
        if details is not None:
            host, port = details
            self.repository.connect_to(host, port)
        if self.repository.is_connected():
            self.pull()

    # Suppport Functions

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

    def checkout(self):
        data, start_v, end_v = cbor.loads(
            self.repository.retrieve_data(
                self.appname, self.local_heap.version))
        if self.local_heap.receive_data(data, (start_v, end_v)):
            self.repository.data_sent_confirmed(self.appname, start_v, end_v)
        # data, versions = self.versioned_heap.retrieve_data(
        #     self.appname,
        #     self.local_heap.version)
        # if self.local_heap.receive_data(data, versions):
        #     # Can be carefully made Async.
        #     self.versioned_heap.data_sent_confirmed(
        #         self.appname, versions)

    def checkout_await(self, timeout=0):
        self._check_updated_since(self.local_heap.version, timeout)
        self.checkout()

    def commit(self):
        data, versions = self.local_heap.retreive_data()
        if versions:
            succ = self.repository.receive_data(
                self.appname, versions[0], versions[1],
                cbor.dumps(data), False)
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

    def pull(self, instrument=False):
        if instrument:
            raise NotImplementedError
        self.fetch()
        self.checkout()
        # delta = self.fetch(instrument=instrument)
        # self.checkout()
        # if instrument:
        #     return delta

    def pull_await(self, timeout=0):
        self.fetch_await(timeout=timeout)
        self.checkout()

    # Functions for cpp binding callback
    def has_merge_func(self, tpname):
        return self.__has_resolver_for_type(tpname)

    def run_merge(
            self, dtpname, oid, original_data,
            new_data, conflicting_data,
            new_obj_change, conf_obj_change):
        return cbor.dumps(self.__custom_merge(
            dtpname, oid,
            None if original_data is None else cbor.loads(original_data),
            None if new_data is None else cbor.loads(new_data),
            None if conflicting_data is None else cbor.loads(conflicting_data),
            None if new_obj_change is None else cbor.loads(new_obj_change),
            None if conf_obj_change is None else cbor.loads(conf_obj_change)))

    # Functions for cpp binding callback
    def __has_resolver_for_type(self, tpname):
        return tpname in self.type_map and self.type_map[tpname] in self.resolver

    @staticmethod
    def __construct_temp(dtype, oid, temp_data):
        obj = utils.container()
        obj.__class__ = dtype
        obj.__r_oid__ = str(oid)
        obj.__r_temp__ = temp_data
        dtype.__r_table__.store_as_temp[oid] = dict()
        return obj

    def __custom_merge(
            self, dtpname, oid, original_data, new_data,
            conflicting_data, new_obj_change,
            conf_obj_change):
        dtype = self.type_map[dtpname]
        original = (
            None
            if original_data is None else
            DataframeCPP.__construct_temp(dtype, oid, original_data))
        new = (
            None
            if new_data is None else
            DataframeCPP.__construct_temp(dtype, oid, new_data))
        conflicting = (
            None
            if conflicting_data is None else
            DataframeCPP.__construct_temp(dtype, oid, conflicting_data))

        obj = self.resolver[dtype](original, new, conflicting)
        if obj:
            obj.__r_temp__.update(dtype.__r_table__.store_as_temp[oid])
            changes = {
                "dims": obj.__r_temp__, "types": dict()}
            changes["types"][dtpname] = (
                Event.Modification if original is not None else Event.New)

            del dtype.__r_table__.store_as_temp[oid]
            return (self.__dim_diff(dtpname, new_obj_change, changes),
                    self.__dim_diff(dtpname, conf_obj_change, changes))
        else:
            # Object was deleted.
            return (
                {"types": {dtpname: Event.Delete}},
                {"types": {dtpname: Event.Delete}})

    def __dim_diff(self, dtpname, original, new):
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
