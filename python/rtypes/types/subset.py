from rtypes.attributes import PredicateFunction
from rtypes.metadata import SubsetMetadata
from rtypes.utils.enums import Rtype

def set_metadata(cls, parent):
    cls.__r_table__ = parent.__r_table__
    pred_func = None
    for attr in dir(cls):
        if isinstance(getattr(cls, attr), PredicateFunction):
            pred_func = getattr(cls, attr)

    meta = SubsetMetadata(Rtype.SUBSET, cls, parent, pred_func)
    if hasattr(cls, "__r_meta__"):
        TypeError("How am I here?")
    else:
        cls.__r_meta__ = meta


class subset(object):
    def __init__(self, parent_cls):
        self.parent = parent_cls

    def __call__(self, cls):
        set_metadata(cls, self.parent)
        return cls
