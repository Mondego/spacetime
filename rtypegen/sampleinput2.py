from rtypes import primarykey, dimension, pcc_set

@pcc_set
class Counter:
    pkey = primarykey(str)
    count = dimension(int)
    def handle_conflicts(self): pass