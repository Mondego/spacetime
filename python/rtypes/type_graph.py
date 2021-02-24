from rtypes.utils.enums import Rtype


class TypeManager():
    def __init__(self, root_meta):
        self.root = root_meta
        self.dependents = {
            root_meta: list()
        }
        self.name_chain = {
            root_meta.name: [root_meta.name]
        }

    def add_subset(self, tp_meta, parent_meta):
        self.dependents[parent_meta].append(tp_meta)
        self.dependents[tp_meta] = list()
        self.name_chain[tp_meta.name] = (
            [tp_meta.name] + self.name_chain[parent_meta.name])
