REGISTERED = list()
VERSIONBY = {
    0: "FULLSTATE",
    1: "TYPES"
}
def register(app):
    REGISTERED.append(app)
    return app