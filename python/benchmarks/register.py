REGISTERED = list()
VERSIONBY = {
    0: "FULLSTATE",
    1: "TYPES",
    2: "OBJECT_NOSTORE"
}
def register(app):
    REGISTERED.append(app)
    return app