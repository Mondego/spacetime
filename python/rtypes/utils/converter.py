from rtypes.utils.enums import Datatype
from pickle import loads, dumps

class _container(object):
    pass

def convert(dim_type, value):
    if dim_type is str:
        return {
            "type": Datatype.STRING,
            "value": value
        }
    if dim_type is int:
        return {
            "type": Datatype.INTEGER,
            "value": value
        }
    if dim_type is float:
        return {
            "type": Datatype.FLOAT,
            "value": value
        }
    if dim_type is bool:
        return {
            "type": Datatype.BOOLEAN,
            "value": value
        }
    if hasattr(dim_type, "__r_meta__") and dim_type.__r_meta__:
        # This is one of our own type. Make it a foreign key.
        return {
            "type": Datatype.FOREIGNKEY,
            "value": value.__r_oid__
        }

def unconvert(value, dim_type, df=None):
    if value is None:
        return None
    if value["type"] in Datatype.BASICTYPES:
        return value["value"]
    if value["type"] == Datatype.FOREIGNKEY:
        obj = None
        if df:
            obj = df.read_one(dim_type, value["value"])
        if not obj:
            obj = _container()
            obj.__r_oid__ = value["value"]
            obj.__class__ = dim_type
        return obj
    return None
