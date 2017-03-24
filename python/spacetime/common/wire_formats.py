FORMATS = dict()

# All these are optional. If they havent installed cbor, assume they are not running cbor for the server.
# Avoids having to install unnecessary packages.
try:
	from pcc.dataframe_changes.dataframe_changes_json import DataframeChanges as DC_json
	FORMATS["json"] = (DC_json, "application/json")
except ImportError:
	pass

try:
	from pcc.dataframe_changes.dataframe_changes_bson import DataframeChanges as DC_bson
	FORMATS["json"] = (DC_bson, "application/octet-stream")
except ImportError:
	pass

try:	
	from pcc.dataframe_changes.dataframe_changes_cbor import DataframeChanges as DC_cbor
	FORMATS["cbor"] = (DC_cbor, "application/octet-stream")
except ImportError:
	pass
