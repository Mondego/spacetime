REGISTERED = list()
VERSIONBY = {
    0: "FULLSTATE"
#    1: "TYPES",
#    2: "OBJECT_NOSTORE"
}

class register(object):
    def __init__(self, name):
        self.name = name
    def __call__(self, app):
        REGISTERED.append((self.name, app))
        return app