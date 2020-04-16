class Dimension():
    def __init__(self, dim_type, is_primary, custom_diff):
        self.dim_type = dim_type
        self.is_primary = is_primary
        self.custom_diff = custom_diff
        self.dimname = None

    def __get__(self, obj, cls):
        if not obj:
            return self
        oid = obj.__r_oid__ if hasattr(obj, "__r_oid__") else None
        if hasattr(obj, "__r_df__") and obj.__r_df__ is not None:
            return obj.__r_df__.read_dimension(cls, oid, self.dimname)
        if hasattr(obj, "__r_temp__") and obj.__r_temp__ is not None:
            return obj.__r_temp__[self.dimname]
        return cls.__r_table__.get(oid, self.dimname)

    def __set__(self, obj, value):
        cls = type(obj)
        oid = obj.__r_oid__ if hasattr(obj, "__r_oid__") else None
        df_attached = hasattr(obj, "__r_df__") and obj.__r_df__ is not None
        if self.is_primary:
            if df_attached:
                raise RuntimeError(
                    "Primary key cannot be reassigned when "
                    "obj is tracked by a dataframe.")
            cls.__r_table__.set_primarykey(oid, self.dimname, value)
            obj.__r_oid__ = value
        else:
            if df_attached:
                if oid is None:
                    raise RuntimeError(
                        "Object primarykey has not been "
                        "set but dataframe is attached.")
                obj.__r_df__.write_dimension(
                    cls, oid, self.dimname, value)
            else:
                obj.__r_oid__ = cls.__r_table__.set(oid, self.dimname, value)
        if hasattr(obj, "__r_temp__") and obj.__r_temp__ is not None:
            obj.__r_temp__[self.dimname] = value


class PredicateFunction():
    def __init__(self, func, dims):
        self.func = func
        self.dims = dims

    def __call__(self, *args):
        return self.func(*args)

class predicate():
    def __init__(self, *dims):
        self.dims = dims

    def __call__(self, func):
        return PredicateFunction(func, self.dims)

class DiffRecord(object):
    def __init__(self, record_type):
        self.record_type = record_type
        self.apply_func = None
        self.merge_func = None
        self.new_func = None
        self.record = record_type()
        self.dim_obj = None 

    def __get__(self, obj, cls):
        if not obj:
            # Accessed from class, not object
            return self
        # Accessed from obj.
        if hasattr(obj, "__r_df__") and obj.__r_df__:
            return obj.__r_df__.get_writeable_record(
                self.dim_obj.dimname, obj.__r_oid__, self.dim_obj.dimname)
        return self.record

    def __set__(self, obj, value):
        self.record = value

    def apply(self, func):
        self.apply_func = func
        return self

    def merge(self, func):
        self.merge_func = func
        return self

    def new(self, func):
        self.new_func = func

class MergeFunction(object):
    def __init__(self, func):
        self.func = func

    def __call__(self, original, modified, conflicting):
        return self.func(original, modified, conflicting)

def diff_record(record_type):
    return DiffRecord(record_type)

def merge(func):
    return MergeFunction(func)

def dimension(dim_type, custom_diff=None):
    return Dimension(dim_type, False, custom_diff)

def primarykey(dim_type):
    return Dimension(dim_type, True, None)
