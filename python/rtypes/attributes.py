class Dimension(object):
    def __init__(self, dim_type, is_primary):
        self.dim_type = dim_type
        self.is_primary = is_primary

def dimension(dim_type):
    return Dimension(dim_type, False)

def primarykey(dim_type):
    return Dimension(dim_type, True)

