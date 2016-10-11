from pcc.dataframe_changes_json import DataframeChanges as DC_json
from pcc.dataframe_changes_bson import DataframeChanges as DC_bson

FORMATS = {
    "json": (DC_json, "application/json"),
    "bson": (DC_bson, "application/octet-stream")
}
