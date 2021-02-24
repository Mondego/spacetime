import time
from rtypes import dimension, primarykey, pcc_set


@pcc_set
class BasicObject():
    oid = primarykey(int)
    create_ts = dimension(float)

    def __init__(self, oid):
        self.oid, self.create_ts = oid, time.time()

@pcc_set
class Stop():
    oid = primarykey(int)
    accepted = dimension(bool)
    start = dimension(bool)

    def __init__(self, index):
        self.oid = index
        self.accepted = False
        self.start = False

@pcc_set
class BasicCounter():
    oid = primarykey(int)
    count = dimension(int)
    create_ts = dimension(float)

    def __init__(self, oid):
        self.oid, self.count, self.create_ts = oid, 0, time.time()

def counter_merge(original, yours, theirs):
    if original:
        yours.count = yours.count + theirs.count - original.count
    else:
        yours.count = yours.count + theirs.count
    return yours