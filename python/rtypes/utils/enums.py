class Datatype(object):
    INTEGER = 0
    STRING = 1
    FLOAT = 2
    BOOLEAN = 3
    BYTES = 4

    FOREIGNKEY = 10
    NPARRAY = 11

    TUPLE = 20
    LIST = 21
    
    BASICTYPES = set([
        INTEGER, STRING, FLOAT, BOOLEAN, BYTES])

class Rtype(object):
    SET = "set"