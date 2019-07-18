from spacetime.dataframe import Dataframe
from threading import Thread
from spacetime.debugger.debugger_types import CommitObj,FetchObj, AcceptFetchObj
from spacetime.managers.connectors.debugger_socket_manager import DebuggerSocketServer, DebuggerSocketConnector
import traceback
import cbor

class DebugDataframe(Thread):

    @property
    def details(self):
        return self.application_df.details

    def __init__(self, df, appname, types, server_port, parent_details):
        print(appname, types, server_port, parent_details)
        self.application_df = Dataframe(appname, types, details=parent_details, server_port=server_port,
                                        use_debugger_sockets=True)
        print("application dataframe details", self.application_df.details)
        self.debugger_df = df
        print("debugger dataframe details", self.debugger_df.details)
        self.parent_app_name = None
        if parent_details:
            self.parent_app_name = self.application_df.socket_connector.get_parent_app_name()
        print("Parent App Name", self.parent_app_name)
        self.from_version = "ROOT"
        self.existing_obj = dict()
        super().__init__()
        self.daemon = True
        super().run()

    def run(self):
        while True:
            self.debugger_df.pull()
            for tp in [AcceptFetchObj]:
                new_obj = self.debugger_df.read_all(eval(tp))
                for obj in new_obj:
                        if isinstance(obj, AcceptFetchObj):
                            if obj.state == obj.AcceptFetchState.START:
                                data, version_change = self.fetch_call_back(obj.requestor_node, obj.from_version)
                                obj.delta = data
                                obj.to_version = version_change[1]
                                obj.complete_SEND()
                            elif obj.state == obj.AcceptFetchState.GCSTART:
                                self.application_df.garbage_collect(obj.requestor_node, obj.to_version)
                                obj.complete_GC()
                            self.debugger_df.commit()
                            self.debugger_df.push()



    def add_one(self, dtype, obj):
        self.application_df.add_one(dtype, obj)

    def add_many(self, dtype, objs):
        self.application_df.add_many(dtype.obj)

    def read_one(self, dtype, oid):
        print(self.application_df.read_one(dtype, oid))
        return self.application_df.read_one(dtype, oid)

    def read_all(self, dtype):
       return self.application_df.read_all(dtype)

    def delete_one(self, dtype, obj):
        self.application_df.delete_one(dtype, obj)

    def delete_all(self, dtype):
        self.application_df.delete_all(dtype)

    def commit(self):
        print("the node wants to commit")

        data, versions = self.application_df.local_heap.retreive_data()
        commitObj = CommitObj(self.application_df.appname, versions[0], versions[1], cbor.dumps(data))
        self.debugger_df.add_one(CommitObj, commitObj)
        self.debugger_df.commit()
        self.debugger_df.push()
        while commitObj.state != commitObj.CommitState.START: #Wait till the CDN gives the command to start
            print("Waiting for CDN to give go ahead to start commit", commitObj.state)
            print(commitObj.state)
            self.debugger_df.pull()
        print(" Go ahead from CDN to start commit")
        if versions:
            with self.application_df.write_lock:
                succ = self.application_df.versioned_heap.receive_data(
                    self.application_df.appname, versions, data, from_external=False)

            if succ:
                commitObj.complete_commit()
                print("commit complete")
                print(commitObj.state)
                self.debugger_df.commit()
                self.debugger_df.push() # To Let the CDN know commit is complete
                while commitObj.state != commitObj.CommitState.GCSTART: # Wait till the CDN gives the command to start GC
                    #print("Waiting for CDN to give the go ahead for GC", commitObj.state)
                    self.debugger_df.pull()
                print("Go ahead from CDN to start GC")
                self.application_df.garbage_collect(self.application_df.appname, versions[1])
                commitObj.complete_GC() # To Let the CDN know GC is complete
                print("GC Complete")
                self.debugger_df.commit()
                self.debugger_df.push()

                self.application_df.local_heap.data_sent_confirmed(versions)


    def sync(self):
         self.application_df.sync()

    def fetch(self):
        if self.application_df.socket_connector.has_parent_connection:
            fetchObj = FetchObj(self.application_df.appname, self.parent_app_name, self.from_version, None, None, "")
            self.debugger_df.add_one(fetchObj, FetchObj)
            while fetchObj.state != fetchObj.FetchState.START:  # Wait till the CDN gives the command to start
                self.debugger_df.pull_await()
            package, to_version = fetchObj.delta_dict, fetchObj.to_version
            with self.application_df.write_lock:
              succ =  self.application_df.versioned_heap.receive_data(
                    "SOCKETPARENT",
                    [fetchObj.from_version, to_version], package)
              if succ:
                  fetchObj.complete_FETCH()
                  self.debugger_df.commit()
                  self.debugger_df.push()
                  while fetchObj.state != fetchObj.CommitState.GCSTART:  # Wait till the CDN gives the command to start GC
                    self.debugger_df.pull_await()
                  self.application_df.garbage_collect(self.application_df.appname, fetchObj.to_version)
                  fetchObj.complete_GC()  # To Let the CDN know GC is complete
                  self.from_version = fetchObj.to_version
                  self.debugger_df.commit()
                  self.debugger_df.push()

    def fetch_call_back(self, appname, version, wait=False, timeout=0):
        try:
            with self.application_df.write_lock:
                return self.application_df.versioned_heap.retrieve_data(appname, version)
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise




