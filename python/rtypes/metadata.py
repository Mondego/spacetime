class Metadata(object):
    def __init__(self, rtype, cls, dims, dimmap, merge_func):
        self.cls = cls
        self.name = "{0}.{1}".format(cls.__module__, cls.__name__)
        self.rtype = rtype
        self.parents = list()
        self.dimnames = dims
        self.dimmap = dimmap
        self.merge = merge_func

    def compose(self, metadata):
        pass
