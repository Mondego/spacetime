from rtypes import primarykey, dimension, pcc_set

@pcc_set
class hello:
    intvar = primarykey(int)
    boolvar = dimension(bool)

@pcc_set
class hellonoprimary:
    anotherboolean = dimension(bool)