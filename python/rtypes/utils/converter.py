from rtypes.utils.enums import Datatype

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

def unconvert(value):
    if value is None:
        return None
    if value["type"] in Datatype.BASICTYPES:
        return value["value"]
    return None
