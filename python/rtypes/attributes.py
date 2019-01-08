class Dimension(object):
    def __init__(self, dim_type, is_primary):
        self.dim_type = dim_type
        self.is_primary = is_primary

class MergeFunction(object):
    def __init__(self, func):
        self.func = func

    def __call__(self, original, modified, conflicting):
        return self.func(original, modified, conflicting)

def dimension(dim_type):
    return Dimension(dim_type, False)

def primarykey(dim_type):
    return Dimension(dim_type, True)

def merge(func):
    return MergeFunction(func)