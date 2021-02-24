class Dimension():
    def __init__(self, dim_type, is_primary):
        self.dim_type = dim_type
        self.is_primary = is_primary


class PredicateFunction():
    def __init__(self, func, dims):
        self.func = func
        self.dims = dims

    def __call__(self, *args):
        return self.func(*args)


def dimension(dim_type):
    return Dimension(dim_type, False)

def primarykey(dim_type):
    return Dimension(dim_type, True)


class predicate():
    def __init__(self, *dims):
        self.dims = dims

    def __call__(self, func):
        return PredicateFunction(func, self.dims)
