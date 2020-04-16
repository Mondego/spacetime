from rtypes.attributes import Dimension
from rtypes.metadata import SetMetadata
from rtypes.utils.enums import Rtype
from rtypes.table import RtypesTable


def set_dimension(cls, dim):
    dim_obj = cls.__dict__[dim]
    dim_obj.dimname = dim
    return dim_obj

def set_metadata(cls):
    cls.__r_table__ = RtypesTable(cls)
    dims = list()
    for attr in dir(cls):
        if attr in cls.__dict__ and isinstance(cls.__dict__[attr], Dimension):
            dims.append(attr)
    dimmap = {dim: set_dimension(cls, dim) for dim in dims}

    meta = SetMetadata(Rtype.SET, cls, dims, dimmap)
    if hasattr(cls, "__r_meta__"):
        raise TypeError("How is this possible?")
    cls.__r_meta__ = meta
    cls.__del__ = delete_obj(cls)

def delete_obj(cls):
    def deleter(self):
        if not hasattr(self, "__r_oid__"):
            return
        oid = self.__r_oid__
        cls.__r_table__.delete_obj(oid)
    return deleter

def pcc_set(cls):
    set_metadata(cls)
    return cls
