import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from rtypes import primarykey, dimension, pcc_set

@pcc_set
class Counter:
    # Counter is the dtype
    pkey = primarykey(str)
    count = dimension(int) # dimension name
    users = dimension(str)

    def __init__(self):
        self.pkey = "countDocument" # countDocument = oid SET1
        self.count = 0 # SET2
        self.users = "[]"



# c = Counter()
# print("c.count =", c.count) # get
# c.count += 1
# print("c.count + 1 =", c.count)
