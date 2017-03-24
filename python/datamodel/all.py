'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import pkgutil
import importlib
import inspect

DATAMODEL_TYPES = []
def load_all_sets(reload_modules=False):
    global DATAMODEL_TYPES
    OLD_DATAMODEL_TYPES = DATAMODEL_TYPES
    DATAMODEL_TYPES = []

    module_list = []
    datamodel_list = []

    for _, name, ispkg in pkgutil.iter_modules(['datamodel']):
        if ispkg:
            try:
                mod = importlib.import_module('datamodel.' + name)
                module_list.append(mod)
                if reload_modules:
                    reload(mod)
            except:
                print "Failed to load module datamodel.%s" % name
                raise

    for module in module_list:
        for _, name, _ in pkgutil.iter_modules(module.__path__):
            #try:
            mod = importlib.import_module(module.__name__ + '.' + name)
            datamodel_list.append(mod)
            if reload_modules:
                reload(mod)
            #except:
            #    print "Failed to load module %s.%s" % (module.__name__, name)

    for module in datamodel_list:
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if hasattr(cls, "__dependent_type__"):
                DATAMODEL_TYPES.append(cls)
    DATAMODEL_TYPES = list(set(DATAMODEL_TYPES).difference(OLD_DATAMODEL_TYPES))

if not DATAMODEL_TYPES:
    load_all_sets()
