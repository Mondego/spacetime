class RequestType(object):
    Pull = 0
    Push = 1

class TransferFields(object):
    AppName = 0
    Data = 1
    RequestType = 2
    Versions = 3

class Event(object):
    New = 0
    Modification = 1
    Delete = 2

class VersionBy(object):
    FULLSTATE = 0
    TYPE = 1
    OBJECT_NOSTORE = 2
    DIMENSION = 3
