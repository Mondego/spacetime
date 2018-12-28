REGISTERED = list()
def register(app):
    REGISTERED.append(app)
    return app