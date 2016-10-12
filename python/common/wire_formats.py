from pcc.dataframe_changes.dataframe_changes_json import DataframeChanges as DC_json
from pcc.dataframe_changes.dataframe_changes_bson import DataframeChanges as DC_bson
from pcc.dataframe_changes.dataframe_changes_cbor import DataframeChanges as DC_cbor

FORMATS = {
    "json": (DC_json, "application/json"),
    "bson": (DC_bson, "application/octet-stream"),
    "cbor": (DC_cbor, "application/octet-stream")
}
