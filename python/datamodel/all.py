'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import pkgutil
import importlib
import inspect

module_list = []
datamodel_list = []
DATAMODEL_TYPES = []

for _, name, ispkg in pkgutil.iter_modules(['datamodel']):
    if ispkg:
        try:
            module_list.append(importlib.import_module('datamodel.' + name))
        except:
            print "Failed to load module datamodel.%s" % name

for module in module_list:
    for _, name, _ in pkgutil.iter_modules(module.__path__):
        try:
            datamodel_list.append(importlib.import_module(module.__name__ + '.' + name))
        except:
            print "Failed to load module %s.%s" % (module.__name__, name)

for module in datamodel_list:
    for name, cls in inspect.getmembers(module, inspect.isclass):
        if hasattr(cls, "__dependent_type__"):
            DATAMODEL_TYPES.append(cls)
