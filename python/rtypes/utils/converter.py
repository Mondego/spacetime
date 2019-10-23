from rtypes.utils.enums import Datatype
from pickle import loads, dumps
import json

try:
    import numpy as np
    HASNUMPY = True
except ModuleNotFoundError:
    HASNUMPY = False


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
    if dim_type is bytes:
        return {
            "type": Datatype.BYTES,
            "value": value
        }
    if HASNUMPY and dim_type is np.array:
        return {
            "type": Datatype.NPARRAY,
            "value": {
                "data": value.tostring(),
                "shape": value.shape,
                "type": dumps(value.dtype)
            }
        }
    if HASNUMPY and dim_type is np.ndarray:
        return {
            "type": Datatype.NPARRAY,
            "value": {
                "data": value.tostring(),
                "shape": value.shape,
                "type": dumps(value.dtype)
            }
        }
    if hasattr(dim_type, "__r_meta__") and dim_type.__r_meta__:
        # This is one of our own type. Make it a foreign key.
        return {
            "type": Datatype.FOREIGNKEY,
            "value": value.__r_oid__
        }
    if dim_type is tuple:
        return {
            "type": Datatype.TUPLE,
            "value": [convert(type(v), v) for v in value]
        }
    if dim_type is list:
        return {
            "type": Datatype.LIST,
            "value": [convert(type(v), v) for v in value]
        }
    if dim_type is json:
        # Making heavy assumption that this dict is jsonable.
        return {
            "type": Datatype.JSON,
            "value": value
        }


def unconvert(value, dim_type, df=None):
    if value is None:
        return None
    if value["type"] in Datatype.BASICTYPES:
        return value["value"]
    if value["type"] == Datatype.NPARRAY:
        if not HASNUMPY:
            global np
            import numpy as np
        return np.frombuffer(
            value["value"]["data"],
            dtype=loads(
                value["value"]["type"])).reshape(value["value"]["shape"])
        
    if value["type"] == Datatype.FOREIGNKEY:
        obj = None
        if df:
            obj = df.read_one(dim_type, value["value"])
        if not obj:
            obj = _container()
            obj.__r_oid__ = value["value"]
            obj.__class__ = dim_type
        return obj
    if value["type"] == Datatype.TUPLE:
        return tuple(unconvert(item, None, None) for item in value["value"])
    if value["type"] == Datatype.LIST:
        return list(unconvert(item, None, None) for item in value["value"])
    if value["type"] == Datatype.JSON:
        return value["value"]
    return None
