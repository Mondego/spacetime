import time
import datetime
from uuid import uuid4
from rtypes import pcc_set, primarykey, dimension, merge
import cbor,uuid
from enum import Enum
import traceback

@pcc_set
class Register(object):
    appname = dimension(str)
    port = dimension(int)

    def __init__(self, appname):
        self.appname = appname
        self.port = 0




@pcc_set
class CheckoutObj(object):

    class CheckoutState(object):
        INIT = 0
        NEW = 1
        START = 2
        CHECKOUTCOMPLETE = 3
        GCSTART = 4
        GCCOMPLETE = 5
        FINISHED = 6
        ABORT = -1

    @property
    def delta_dict(self):
        if hasattr(self, "_deltadict") and self._deltahash == hash(self.delta):
            return self._deltadict
        self._deltadict = cbor.loads(self.delta)
        self._deltahash = hash(self.delta)
        return self._deltadict

    oid = primarykey(str)

    # Logistics
    node = dimension(str)
    state = dimension(int)
    # Payload
    from_version = dimension(str)
    to_version = dimension(str)
    delta = dimension(bytes)

    def __init__(self, node, from_version, to_version, delta):
        self.oid = str(uuid.uuid4())
        self.node = node
        self.from_version = from_version
        self.to_version = to_version
        self.delta = cbor.dumps(delta)
        self.state = self.CheckoutState.INIT

    def start(self):
        self.state = self.CheckoutState.START

    def complete_checkout(self):
        self.state = self.CheckoutState.CHECKOUTCOMPLETE

    def start_GC(self):
        self.state = self.CheckoutState.GCSTART

    def complete_GC(self):
        self.state = self.CheckoutState.GCCOMPLETE

    def finish(self):
        self.state = self.CheckoutState.FINISHED

@pcc_set
class CommitObj(object):

    class CommitState(object):
        INIT = 0
        NEW = 1
        START = 2
        COMMITCOMPLETE = 3
        GCSTART = 4
        GCCOMPLETE = 5
        FINISHED = 6

        ABORT = -1

    @property
    def delta_dict(self):
        if hasattr(self, "_deltadict") and self._deltahash == hash(self.delta):
            return self._deltadict
        self._deltadict = cbor.loads(self.delta)
        self._deltahash = hash(self.delta)
        return self._deltadict

    oid = primarykey(str)

    # Logistics
    node = dimension(str)
    state = dimension(int)
    # Payload
    from_version = dimension(str)
    to_version = dimension(str)
    delta = dimension(bytes)

    def __init__(self, node, from_version, to_version, delta):
        self.oid = str(uuid.uuid4())
        self.node = node
        self.from_version = from_version
        self.to_version = to_version
        self.delta = cbor.dumps(delta)
        self.state = self.CommitState.INIT

    def start(self):
        self.state = self.CommitState.START

    def complete_commit(self):
        self.state = self.CommitState.COMMITCOMPLETE

    def start_GC(self):
        self.state = self.CommitState.GCSTART

    def complete_GC(self):
        self.state = self.CommitState.GCCOMPLETE

    def finish(self):
        self.state = self.CommitState.FINISHED




@pcc_set
class FetchObj(object):
    class FetchState(object):
        INIT = 0
        NEW = 1
        WAIT = 2
        RECEIVEDCHANGES = 3
        START = 4
        FETCHCOMPLETE = 5
        GCSTART = 6
        GCCOMPLETE = 7
        FINISHED = 8

        ABORT = -1

    @property
    def delta_dict(self):
        if not self.delta:
            return dict()
        if hasattr(self, "_deltadict") and self._deltahash == hash(self.delta):
            return self._deltadict
        self._deltadict = cbor.loads(self.delta)
        self._deltahash = hash(self.delta)
        return self._deltadict


    oid = primarykey(str)

    # Logistics
    requestor_node = dimension(str)
    requestee_node = dimension(str)
    state = dimension(int)
    # Initial payload
    from_version = dimension(str)

    # Response to request
    to_version = dimension(str)
    delta = dimension(bytes)
    accept_fetch_obj_oid = dimension(str)

    def __init__(self, requestor_node, requestee_node, from_version, to_version, delta):
        self.oid = str(uuid.uuid4())
        self.requestor_node = requestor_node
        self.requestee_node = requestee_node
        self.from_version = from_version
        self.to_version = to_version
        self.delta = delta
        self.state = self.FetchState.INIT
        self.parent_df = None
        # write out these states

    def start(self):
        self.state = self.FetchState.START

    def wait(self):
        self.state = self.FetchState.WAIT

    def receive_changes(self):
        self.state = self.FetchState.RECEIVEDCHANGES

    def complete_FETCH(self):
        self.state = self.FetchState.FETCHCOMPLETE

    def start_GC(self):
        self.state = self.FetchState.GCSTART

    def complete_GC(self):
        self.state = self.FetchState.GCCOMPLETE

    def finish(self):
        self.state = self.FetchState.FINISHED

@pcc_set
class AcceptFetchObj(object):
    class AcceptFetchState(object):
        INIT = 0
        START = 1
        SENDCOMPLETE = 2
        WAIT = 3
        RECEIVEDCONFIRMATION = 4
        GCSTART = 5
        GCCOMPLETE = 6
        FINISHED = 7

        ABORT = -1

    @property
    def delta_dict(self):
        if not self.delta:
            return dict()
        if hasattr(self, "_deltadict") and self._deltahash == hash(self.delta):
            return self._deltadict
        self._deltadict = cbor.loads(self.delta)
        self._deltahash = hash(self.delta)
        return self._deltadict

    oid = primarykey(str)


    requestor_node = dimension(str)
    requestee_node = dimension(str)
    state = dimension(int)

    from_version = dimension(str)


    to_version = dimension(str)
    delta = dimension(bytes)
    fetch_obj_oid = dimension(str)

    def __init__(self, requestor_node, requestee_node, from_version, to_version, delta, fetch_obj_oid):
        self.oid = str(uuid.uuid4())
        self.requestor_node = requestor_node
        self.requestee_node = requestee_node
        self.from_version = from_version
        self.to_version = to_version
        self.delta = delta
        self.state = self.AcceptFetchState.INIT
        self.sender_df = None
        self.fetch_obj_oid = fetch_obj_oid
        # write out these states

    def start(self):
        self.state = self.AcceptFetchState.START

    def complete_SEND(self):
        self.state = self.AcceptFetchState.SENDCOMPLETE

    def wait(self):
        self.state = self.AcceptFetchState.WAIT

    def receive_confirmation(self):
        self.state = self.AcceptFetchState.RECEIVEDCONFIRMATION

    def start_GC(self):
        self.state = self.AcceptFetchState.GCSTART

    def complete_GC(self):
        self.state = self.AcceptFetchState.GCCOMPLETE

    def finish(self):
        self.state = self.AcceptFetchState.FINISHED

    def client_execute(self, df):
        if self.state == self.AcceptFetchState.START:
            print("calling fetch call back")
            data, version_change = df.fetch_call_back(self.requestor_node, self.from_version)
            self.delta = cbor.dumps(data)
            self.to_version = version_change[1]
            self.complete_SEND()
            print("completed fetch call back")
        elif self.state == self.AcceptFetchState.GCSTART:
            print("Requestee starting garbage collect")
            df.confirm_fetch_req(self.requestor_node, [self.from_version, self.to_version])
            print("Requestee completed garbage collect")
            self.complete_GC()

@pcc_set
class PushObj(object):
    class PushState(object):
        INIT = 0
        NEW = 1
        START = 2
        FETCHDELTACOMPLETE = 3
        WAIT = 4
        PUSHCOMPLETE = 5
        GCSTART = 6
        GCCOMPLETE = 7
        FINISHED = 8

        ABORT = -1

    @property
    def delta_dict(self):
        if not self.delta:
            return dict()
        if hasattr(self, "_deltadict") and self._deltahash == hash(self.delta):
            return self._deltadict
        self._deltadict = cbor.loads(self.delta)
        self._deltahash = hash(self.delta)
        return self._deltadict


    oid = primarykey(str)

    # Logistics
    sender_node = dimension(str)
    receiver_node = dimension(str)
    state = dimension(int)
    # Initial payload
    from_version = dimension(str)

    # Response to request
    to_version = dimension(str)
    delta = dimension(bytes)
    accept_fetch_obj_oid = dimension(str)

    def __repr__(self):
        return "Push Obj" + " , "+ str(self.sender_node) +" , "+ str(self.receiver_node)+ " , "+ str(self.oid) +\
               " ,"+ str(self.state)

    def __init__(self, sender_node, receiver_node, from_version, to_version, delta):
        self.oid = str(uuid.uuid4())
        self.sender_node = sender_node
        self.receiver_node = receiver_node
        self.from_version = from_version
        self.to_version = to_version
        self.delta = delta
        self.state = self.PushState.INIT
        self.parent_df = None
        # write out these states

    def start(self):
        self.state = self.PushState.START

    def completed_FETCHDELTA(self):
        self.state = self.PushState.FETCHDELTACOMPLETE

    def wait(self):
        self.state = self.PushState.WAIT

    def complete_PUSH(self):
        self.state = self.PushState.PUSHCOMPLETE

    def start_GC(self):
        self.state = self.PushState.GCSTART

    def complete_GC(self):
        self.state = self.PushState.GCCOMPLETE

    def finish(self):
        self.state = self.PushState.FINISHED

@pcc_set
class AcceptPushObj(object):
        class AcceptPushState(object):
            INIT = 0
            NEW = 1
            START = 2
            RECEIVECOMPLETE = 3
            WAIT = 4
            GCSTART = 5
            GCCOMPLETE = 6
            FINISHED = 7

            ABORT = -1

        def __repr__(self):
                return ("Accept Push Obj" + " , "+ str(self.sender_node) +" , "+ str(self.receiver_node) + " , "+str(
                    self.oid) + " , "+ str(self.push_obj_oid) + "," + str(self.state))

        @property
        def delta_dict(self):
            if not self.delta:
                return dict()
            if hasattr(self, "_deltadict") and self._deltahash == hash(self.delta):
                return self._deltadict
            self._deltadict = cbor.loads(self.delta)
            self._deltahash = hash(self.delta)
            return self._deltadict

        oid = primarykey(str)


        sender_node = dimension(str)
        receiver_node = dimension(str)
        state = dimension(int)

        from_version = dimension(str)


        to_version = dimension(str)
        delta = dimension(bytes)
        fetch_obj_oid = dimension(str)
        push_obj_oid = dimension(str)

        def __init__(self, sender_node, receiver_node, from_version, to_version, delta, push_obj_oid):
            self.oid = str(uuid.uuid4())
            #self.oid = push_obj_oid
            self.sender_node = sender_node
            self.receiver_node = receiver_node
            self.from_version = from_version
            self.to_version = to_version
            self.delta = delta
            self.state = self.AcceptPushState.INIT
            self.sender_df = None
            self.push_obj_oid = push_obj_oid
            # write out these states

        def start(self):
            self.state = self.AcceptPushState.START

        def complete_RECEIVE(self):
            self.state = self.AcceptPushState.RECEIVECOMPLETE

        def wait(self):
            self.state = self.AcceptPushState.WAIT

        def start_GC(self):
            self.state = self.AcceptPushState.GCSTART

        def complete_GC(self):
            self.state = self.AcceptPushState.GCCOMPLETE

        def finish(self):
            self.state = self.AcceptPushState.FINISHED

        def client_execute(self, df):
            if self.state == self.AcceptPushState.START:
                print("calling push call back")
                df.push_call_back(self.sender_node, [self.from_version, self.to_version], self.delta_dict)
                self.complete_RECEIVE()
                print("completed push call back")

            if self.state == self.AcceptPushState.GCSTART:
                print("Receiver starting garbage collect")
                try:
                    with df.application_df.write_lock:
                        df.application_df.garbage_collect(self.sender_node, self.to_version)
                        print("Receiver completed garbage collect")
                        self.complete_GC()
                except Exception as e:
                    print(e)
                    print(traceback.format_exc())
                    raise

@pcc_set
class Parent(object):
    singleton = "SINGLETON"
    oid = primarykey(str)
    app = dimension(str)
    parent_app = dimension(str)

    def __init__(self, app, parent_app):
        self.oid = Parent.singleton
        self.app = app
        self.parent_app = parent_app




