from rtypes.attributes import Dimension, MergeFunction
from rtypes.metadata import Metadata
from rtypes.utils.enums import Rtype
from rtypes.utils.converter import convert, unconvert
from rtypes.table import RtypesTable

def get_property(cls, dimname, dim_obj):
    @property
    def prop(self):
        oid = self.__r_oid__ if hasattr(self, "__r_oid__") else None
        if hasattr(self, "__r_df__") and self.__r_df__ is not None:
            return unconvert(
                self.__r_df__.read_dimension(
                    cls, oid, dimname), dim_obj.dim_type, self.__r_df__)
        if hasattr(self, "__r_temp__") and self.__r_temp__ is not None:
            return unconvert(self.__r_temp__[dimname], dim_obj.dim_type)
        return cls.__r_table__.get(oid, dimname, dim_obj)

    @prop.setter
    def prop(self, value):
        oid = self.__r_oid__ if hasattr(self, "__r_oid__") else None
        df_attached = hasattr(self, "__r_df__") and self.__r_df__ is not None
        if dim_obj.is_primary:
            if df_attached:
                self.__r_df__.reset_primary_key(
                    cls, oid, dimname, convert(dim_obj.dim_type, value))
            else:
                cls.__r_table__.set_primarykey(
                    oid, dimname, dim_obj, value)
            self.__r_oid__ = value
        else:
            if df_attached:
                if oid is None:
                    raise RuntimeError(
                        "Objhect primarykey has not been"
                        " set but dataframe is attached.")
                self.__r_df__.write_dimension(
                    cls, oid, dimname, convert(dim_obj.dim_type, value))
            else:
                self.__r_oid__ = cls.__r_table__.set(
                    oid, dimname, dim_obj, value)
        if hasattr(self, "__r_temp__") and self.__r_temp__ is not None:
            self.__r_temp__[dimname] = convert(dim_obj.dim_type, value)

    return prop

def set_dimension(cls, dim):
    dim_obj = getattr(cls, dim)
    setattr(cls, dim, get_property(cls, dim, dim_obj))
    return dim_obj

def set_metadata(cls):
    cls.__r_table__ = RtypesTable(cls)
    dims = list()
    merge_func = None
    for attr in dir(cls):
        if isinstance(getattr(cls, attr), Dimension):
            dims.append(attr)
        if isinstance(getattr(cls, attr), MergeFunction):
            merge_func = getattr(cls, attr)

    dimmap = {dim: set_dimension(cls, dim) for dim in dims}

    meta = Metadata(Rtype.SET, cls, dims, dimmap, merge_func)
    if hasattr(cls, "__r_meta__"):
        cls.__r_meta__.compose(meta)
    else:
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
