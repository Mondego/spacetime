class RequestType(object):
    Pull = 0
    Push = 1

class TransferFields(object):
    AppName = 0
    Data = 1
    RequestType = 2
    Versions = 3
    Wait = 4
    WaitTimeout = 5
    Status = 6
    Types = 7
    TransactionId = 8
    Confirmed = 9

class Event(object):
    New = 0
    Modification = 1
    Delete = 2

class VersionBy(object):
    FULLSTATE = 0
    TYPE = 1
    OBJECT_NOSTORE = 2
    DIMENSION = 3

class ConnectionStyle(object):
    TSocket = 0
    NPSocket = 1
    AIOSocket = 2

class AutoResolve(object):
    FullResolve = 0
    BranchConflicts = 1
    BranchExternalPush = 2

class StatusCode(object):
    Success = 200

    GeneralException = 400
    Timeout = 401