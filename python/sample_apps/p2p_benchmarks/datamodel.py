import sys
import os
import uuid
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from rtypes import primarykey, dimension, pcc_set

@pcc_set
class BasicType:
    oid = primarykey(str)
    prop1 = dimension(str)
    prop2 = dimension(int)

    def __str__(selfs):
        return "objy"

    def __init__(self):
        self.oid = str(uuid.uuid4())
        self.prop1 = str(uuid.uuid4())
