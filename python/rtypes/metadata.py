from abc import ABCMeta

from rtypes.type_graph import TypeManager

class Metadata():
    __metaclass__ = ABCMeta
    @property
    def name_chain(self):
        return self.type_graph.name_chain[self.name]

    def __init__(self, rtype, cls):
        self.cls = cls
        self.name = "{0}.{1}".format(cls.__module__, cls.__name__)
        self.rtype = rtype
        self.type_graph = None


class SetMetadata(Metadata):
    def __init__(self, rtype, cls, dims, dimmap):
        super().__init__(rtype, cls)
        self.dimnames = dims
        self.dimmap = dimmap
        self.type_graph = TypeManager(self)


class SubsetMetadata(Metadata):
    def __init__(self, rtype, cls, parent_cls, pred_func):
        super().__init__(rtype, cls)
        parent_meta = parent_cls.__r_meta__
        self.dimnames = parent_meta.dimnames
        self.dimmap = parent_meta.dimmap
        self.parent = parent_meta
        self.pred_func = pred_func
        self.type_graph = parent_meta.type_graph
        self.type_graph.add_subset(self, self.parent)
