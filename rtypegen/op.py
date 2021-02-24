from rtypes import primarykey, dimension, pcc_set

@pcc_set
class grade:
    student_id = primarykey(int)
    points = dimension(int)
    def handle_conflicts(self): pass

@pcc_set
class hellonoprimary:
    anotherboolean = dimension(bool)
    def mergy(self): pass