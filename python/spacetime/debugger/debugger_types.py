import time
import datetime
from uuid import uuid4
from rtypes import pcc_set, primarykey, dimension, merge
import cbor,uuid
from enum import Enum

@pcc_set
class Register(object):
    appname = dimension(str)
    port = dimension(int)

    def __init__(self, appname):
        self.appname = appname
        self.port = 0




@pcc_set
class CommitObj(object):

    class CommitState(object):
        INIT = 0
        START = 1
        COMMITCOMPLETE = 2
        GCSTART = 3
        GCCOMPLETE = 4
        FINISH = 5

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


@pcc_set
class FetchObj(object):
    class FetchState(object):
        INIT = 0
        START = 1
        FETCHCOMPLETE = 2
        GCSTART = 3
        GCCOMPLETE = 4
        FINISH = 5

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

    def __init__(self, requestor_node, requestee_node, from_version, to_version, delta, accept_fetch_obj_oid):
        self.oid = str(uuid.uuid4())
        self.requestor_node = requestor_node
        self.requestee_node = requestee_node
        self.from_version = from_version
        self.to_version = to_version
        self.delta = delta
        self.accept_fetch_obj_oid = accept_fetch_obj_oid
        # write out these states

    def start(self):
        self.state = self.FetchState.START

    def complete_FETCH(self):
        self.state = self.FetchState.FETCHCOMPLETE

    def start_GC(self):
        self.state = self.FetchState.GCSTART

    def complete_GC(self):
        self.state = self.FetchState.GCCOMPLETE

@pcc_set
class AcceptFetchObj(object):
        class AcceptFetchState(object):
            INIT = 0
            START = 1
            SENDCOMPLETE = 2
            GCSTART = 3
            GCCOMPLETE = 4
            FINISH = 5

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
            self.state = self.FetchState.INIT
            self.fetch_obj_oid = fetch_obj_oid
            # write out these states

        def start(self):
            self.state = self.FetchState.START

        def complete_SEND(self):
            self.state = self.FetchState.SENDCOMPLETE

        def start_GC(self):
            self.state = self.FetchState.GCSTART

        def complete_GC(self):
            self.state = self.FetchState.GCCOMPLETE


    # Other functions


