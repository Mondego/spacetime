from multiprocessing import RLock
import traceback

from spacetime.managers.connectors.np_socket_manager import NPSocketServer, NPSocketConnector
from spacetime.managers.connectors.asyncio_socket_manager import AIOSocketServer, AIOSocketConnector
from spacetime.managers.connectors.thread_socket_manager import TSocketServer, TSocketConnector
from spacetime.managers.version_manager import FullStateVersionManager, TypeVersionManager, ObjectVersionManagerVersionSent, VersionManagerProcess
from spacetime.managers.managed_heap import ManagedHeap
from spacetime.managers.diff import Diff
import spacetime.utils.enums as enums
import spacetime.utils.utils as utils


class Dataframe(object):
    @property
    def details(self):
        return self.socket_server.port

    def __init__(
            self, appname, types, details=None, server_port=0,
            version_by=enums.VersionBy.FULLSTATE, separate_dag=False,
            connection_as=enums.ConnectionStyle.TSocket):
        self.appname = appname
        self.logger = utils.get_logger("%s_Dataframe" % appname)
        self.version_by = version_by

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
            self.pull_call_back, self.push_call_back, self.confirm_pull_req)

        self.socket_connector = SocketConnector(
            self.appname, details, self.details, types, version_by)

        self.types = types
        self.type_map = {
            tp.__r_meta__.name: tp for tp in self.types}
        self.local_heap = ManagedHeap(types, version_by)
        self.versioned_heap = None
        
        if separate_dag:
            self.versioned_heap = VersionManagerProcess(
                self.appname, types, version_by)
            self.versioned_heap.start()
        elif version_by == enums.VersionBy.FULLSTATE:
            self.versioned_heap = FullStateVersionManager(self.appname, types)
        elif version_by == enums.VersionBy.TYPE:
            self.versioned_heap = TypeVersionManager(self.appname, types)
        elif version_by == enums.VersionBy.OBJECT_NOSTORE:
            self.versioned_heap = ObjectVersionManagerVersionSent(
                self.appname, types)
        else:
            raise NotImplementedError()
        self.write_lock = RLock()
        
        self.socket_server.start()
        if self.socket_connector.has_parent_connection:
            self.pull()

    # Suppport Functions

    def _create_package(self, appname, diff, start_version):
        return appname, [start_version, diff.version], diff

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

    def checkout(self):
        data, versions = self.versioned_heap.retrieve_data(
            self.appname,
            self.local_heap.version)
        if self.local_heap.receive_data(data, versions):
            # Can be carefully made Async.
            with self.write_lock:
                self.versioned_heap.data_sent_confirmed(
                    self.appname, versions)

    def commit(self):
        data, versions = self.local_heap.retreive_data()
        if versions:
            with self.write_lock:
                succ = self.versioned_heap.receive_data(
                    self.appname, versions, data, from_external=False)
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
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Push request started.")
            with self.write_lock:
                data, version = self.versioned_heap.retrieve_data(
                    "SOCKETPARENT", self.socket_connector.parent_version)
                if self.version_by == enums.VersionBy.FULLSTATE:
                    if version[0] == version[1]:
                        self.logger.debug(
                            "Push not required, "
                            "parent already has the information.")
                        return
                elif self.version_by == enums.VersionBy.TYPE:
                    something_different = False
                    for tpname in version:
                        if version[tpname][0] != version[tpname][1]:
                            something_different = True
                    if not something_different:
                        return
                elif self.version_by == enums.VersionBy.OBJECT_NOSTORE:
                    something_different = False
                    for tpname in version:
                        for oid in version[tpname]:
                            if version[tpname][oid][0] != version[tpname][oid][1]:
                                something_different = True
                    if not something_different:
                        return
                else:
                    raise NotImplementedError()
                
            if self.socket_connector.push_req(data, version):
                self.logger.debug("Push request completed.")
                with self.write_lock:
                    self.versioned_heap.data_sent_confirmed(
                        "SOCKETPARENT", version)
                self.logger.debug("Push request registered.")

    def pull(self):
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Pull request started.")
            package, version = self.socket_connector.pull_req()
            self.logger.debug("Pull request completed.")
            with self.write_lock:
                self.versioned_heap.receive_data(
                    "SOCKETPARENT",
                    version, package)
            self.logger.debug("Pull request applied.")

    # Functions that respond to external requests

    def pull_call_back(self, appname, version):
        try:
            with self.write_lock:
                return self.versioned_heap.retrieve_data(appname, version)
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise
    
    def confirm_pull_req(self, appname, version):
        try:
            with self.write_lock:
                self.versioned_heap.data_sent_confirmed(
                    appname, version)
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise
            
    def push_call_back(self, appname, versions, data):
        try:
            with self.write_lock:
                return self.versioned_heap.receive_data(
                    appname, versions, data)
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise

