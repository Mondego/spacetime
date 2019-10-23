from spacetime.dataframe import Dataframe
from threading import Thread, RLock
from spacetime.debugger.debugger_types import CommitObj,FetchObj, AcceptFetchObj, CheckoutObj, PushObj, AcceptPushObj, Parent, AppToState
from spacetime.managers.connectors.debugger_socket_manager import DebuggerSocketServer, DebuggerSocketConnector
import traceback
import cbor

class DebugDataframe(object):

    @property
    def appname(self):
        return self.application_df.appname

    @property
    def details(self):
        return self.application_df.details

    def listen_func(self):
        while True:
            self.debugger_df.pull()
            for tp in [AcceptFetchObj, AcceptPushObj]:
                new_objs = self.debugger_df.read_all(tp)
                for obj in new_objs:
                    if isinstance(obj, AcceptPushObj) and obj.state == AcceptPushObj.AcceptPushState.FINISHED:
                        self.debugger_df.delete_one(AcceptPushObj, obj)
                        self.debugger_df.commit()
                        self.debugger_df.push()
                        continue
                    if isinstance(obj, AcceptFetchObj) and obj.state == AcceptFetchObj.AcceptFetchState.FINISHED:
                        self.debugger_df.delete_one(AcceptFetchObj, obj)
                        self.debugger_df.commit()
                        self.debugger_df.push()
                        continue
                        
                    obj.client_execute(self)
                    with self.debug_df_lock:
                        self.debugger_df.commit()
                        self.debugger_df.push()

    def __init__(self, df, appname, types, server_port, parent_details):
        self.debug_df_lock = RLock()
        print(appname, types, server_port, parent_details)
        self.debugger_df = df
        df.add_one(AppToState, AppToState(appname))
        self.application_df = Dataframe(appname, types, details=parent_details, server_port=server_port,
                                        use_debugger_sockets=df)
        print("application dataframe details", self.application_df.details)
        print("debugger dataframe details", self.debugger_df.details)
        self.parent_app_name = None
        if parent_details:
            self.parent_app_name = self.application_df.socket_connector.get_parent_app_name()
        print("Parent App Name", self.parent_app_name)
        self.debugger_df.add_one(Parent, Parent(appname, self.parent_app_name if self.parent_app_name else ""))
        self.debugger_df.commit()
        self.debugger_df.push()
        self.from_version = "ROOT"
        self.listening_thread = Thread(target=self.listen_func, daemon=True)
        print(appname, self.listening_thread.getName())
        self.listening_thread.start()


    def add_one(self, dtype, obj):
        self.application_df.add_one(dtype, obj)

    def add_many(self, dtype, objs):
        self.application_df.add_many(dtype, objs)

    def read_one(self, dtype, oid):
        print(self.application_df.read_one(dtype, oid))
        return self.application_df.read_one(dtype, oid)

    def read_all(self, dtype):
       return self.application_df.read_all(dtype)

    def delete_one(self, dtype, obj):
        self.application_df.delete_one(dtype, obj)

    def delete_all(self, dtype):
        self.application_df.delete_all(dtype)

    def checkout(self):
        print("the node wants to checkout")
        checkoutObj = CheckoutObj(self.application_df.appname, self.application_df.local_heap.version, "", None)
        self.debugger_df.add_one(CheckoutObj, checkoutObj)
        self.debugger_df.commit()
        self.debugger_df.push()
        while checkoutObj.state != checkoutObj.CheckoutState.START: #Wait till the CDN gives the command to start
            #print("Waiting for CDN to give permission to start checkout", checkoutObj.state)
            self.debugger_df.pull()
        print("Received permission from CDN to start checkout")
        with self.application_df.write_lock:
            data, versions = self.application_df.versioned_heap.retrieve_data(
                checkoutObj.node,
                checkoutObj.from_version)
        result = self.application_df.local_heap.receive_data(data, versions)
        #if result:
        checkoutObj.delta = cbor.dumps(data)
        checkoutObj.to_version = versions[1]
        checkoutObj.complete_checkout()
        print("Checkout completed")
        self.debugger_df.commit()
        self.debugger_df.push()
        while checkoutObj.state != checkoutObj.CheckoutState.GCSTART:  # Wait till the CDN gives the command to start GC
            # print("Waiting for CDN to give the go ahead for GC", checkoutObj.state)
            self.debugger_df.pull()
        print("Go ahead from CDN to start GC")
        with self.application_df.write_lock:
            if checkoutObj.from_version != checkoutObj.to_version:
                self.application_df.garbage_collect(checkoutObj.node, checkoutObj.to_version)
            checkoutObj.complete_GC()  # To Let the CDN know GC is complete
            print("GC is Complete")
            self.debugger_df.commit()
            self.debugger_df.push()
        while checkoutObj.state != checkoutObj.CheckoutState.FINISHED: # Wait till the CDN gives the command to Finish
            self.debugger_df.pull()
        print("Go ahead from CDN to Finish")
        self.debugger_df.delete_one(CheckoutObj, checkoutObj)
        self.debugger_df.commit()
        self.debugger_df.push()

    def commit(self):
        print(f"{self.appname} the node wants to commit")
        data, versions = self.application_df.local_heap.retreive_data()
        commitObj = CommitObj(self.application_df.appname, versions[0], versions[1], data)
        self.debugger_df.add_one(CommitObj, commitObj)
        self.debugger_df.commit()
        self.debugger_df.push()
        print (f"{self.appname} Completed push await, server confirmed")
        while commitObj.state != commitObj.CommitState.START: #Wait till the CDN gives the command to start
            #print("Waiting for CDN to give go ahead to start commit", commitObj.state)
            self.debugger_df.pull()
        print(" Go ahead from CDN to start commit")
        if versions:
            with self.application_df.write_lock:
                succ = self.application_df.versioned_heap.receive_data(
                    self.application_df.appname, versions, data, from_external=False)


        commitObj.complete_commit()
        print("commit complete")
        self.debugger_df.commit()
        self.debugger_df.push() # To Let the CDN know commit is complete
        while commitObj.state != commitObj.CommitState.GCSTART: # Wait till the CDN gives the command to start GC
            #print("Waiting for CDN to give the go ahead for GC", commitObj.state)
            self.debugger_df.pull()
        print("Go ahead from CDN to start GC")
        if versions:
            self.application_df.garbage_collect(self.application_df.appname, versions[1])
        commitObj.complete_GC() # To Let the CDN know GC is complete
        print("GC Complete")
        self.debugger_df.commit()
        self.debugger_df.push()
        if versions:
            self.application_df.local_heap.data_sent_confirmed(versions)
        while commitObj.state != commitObj.CommitState.FINISHED: # Wait till the CDN gives the command to Finish
            self.debugger_df.pull()
        print("Go ahead from CDN to Finish")
        self.debugger_df.delete_one(CommitObj, commitObj)
        self.debugger_df.commit()
        self.debugger_df.push()
        print ("Delete commit object", commitObj.oid)        

    def sync(self):
        self.application_df.sync()

    def push(self):
        if self.parent_app_name:
            print("Node creates a push object")
            pushObj = PushObj(self.application_df.appname, self.parent_app_name, "", "", b"")
            self.debugger_df.add_one(PushObj, pushObj)
            self.debugger_df.commit()
            self.debugger_df.push()
            while pushObj.state != pushObj.PushState.START:  # Wait till the CDN gives the command to start
                #print("Node is waiting for CDN to give the go ahead to start push")
                self.debugger_df.pull()
            with self.application_df.write_lock:
                data, version = self.application_df.versioned_heap.retrieve_data(
                    "SOCKETPARENT", self.from_version)
                # TODO
                # if version[0] == version[1]:
                #     pushObj.set_empty()
                #     self.debugger_df.commit()
                #     self.debugger_df.push()
                #     return
                pushObj.from_version, pushObj.to_version = version[0], version[1]
                pushObj.delta = cbor.dumps(data)
                pushObj.completed_FETCHDELTA()
                self.debugger_df.commit()
                self.debugger_df.push()
            while pushObj.state != pushObj.PushState.GCSTART:
                self.debugger_df.pull()
            print("Sender starting garbage collect")
            self.from_version = pushObj.to_version #TODO is this correct?
            with self.application_df.write_lock:
                if pushObj.from_version != pushObj.to_version:
                    self.application_df.garbage_collect(
                        "SOCKETPARENT", pushObj.to_version)
            pushObj.complete_GC()
            self.debugger_df.commit()
            self.debugger_df.push()
            while pushObj.state != pushObj.PushState.FINISHED: # Wait till the CDN gives the command to Finish
                self.debugger_df.pull()
            self.debugger_df.delete_one(PushObj, pushObj)
            self.debugger_df.commit()
            self.debugger_df.push()

    def fetch(self):
        if self.parent_app_name:
            print("Node creates a fetch object")
            fetchObj = FetchObj(self.application_df.appname, self.parent_app_name, self.from_version, "", b"")
            self.debugger_df.add_one(FetchObj, fetchObj)
            self.debugger_df.commit()
            self.debugger_df.push()
            while fetchObj.state != fetchObj.FetchState.START:  # Wait till the CDN gives the command to start
                #print("Node is waiting for CDN to give the go ahead to start fetch")
                self.debugger_df.pull()
            package, to_version = fetchObj.delta_dict, fetchObj.to_version
            with self.application_df.write_lock:
                succ =  self.application_df.versioned_heap.receive_data(
                    "SOCKETPARENT",
                    [fetchObj.from_version, to_version], package)
                fetchObj.complete_FETCH()
                self.debugger_df.commit()
                self.debugger_df.push()
                while fetchObj.state != fetchObj.FetchState.GCSTART:  # Wait till the CDN gives the command to start GC
                    self.debugger_df.pull()
                print("Requestor starting garbage collect")
                self.application_df.garbage_collect("SOCKETPARENT", fetchObj.to_version)
                print("Requestor completed garbage collect")
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

    def confirm_fetch_req(self, appname, version):
        try:
            with self.application_df.write_lock:
                if version[0] != version[1]:
                    self.application_df.garbage_collect(appname, version[1])
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise

    def push_call_back(self, appname, versions, data):
        try:
            with self.application_df.write_lock:
                return_value = self.application_df.versioned_heap.receive_data(
                    appname, versions, data)
                #self.garbage_collect(appname, versions[1])
                return return_value
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise


