'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from common.modes import Modes

class DataAgent(object):
    def __init__(self, keywords):
        if 'host' in keywords:
            self.host = (keywords['host'] + ("/" if keywords['host'][-1] != "/" else ""))
        else:
            self.host = "default"

class Producer(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ == None:
                actual_class.__declaration_map__ = {}
            actual_class.__declaration_map__.setdefault(self.host, {})[Modes.Producing] = self.types
        return actual_class

class Tracker(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ == None:
                actual_class.__declaration_map__ = {}
            actual_class.__declaration_map__.setdefault(self.host, {})[Modes.Tracker] = self.types
        return actual_class

class Getter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ == None:
                actual_class.__declaration_map__ = {}
            actual_class.__declaration_map__.setdefault(self.host, {})[Modes.Getter] = self.types
        return actual_class

class GetterSetter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ == None:
                actual_class.__declaration_map__ = {}
            actual_class.__declaration_map__.setdefault(self.host, {})[Modes.GetterSetter] = self.types
        return actual_class

class Deleter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ == None:
                actual_class.__declaration_map__ = {}
            actual_class.__declaration_map__.setdefault(self.host, {})[Modes.Deleter] = self.types
        return actual_class

class Setter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        if self.types:
            if actual_class.__declaration_map__ == None:
                actual_class.__declaration_map__ = {}
            actual_class.__declaration_map__.setdefault(self.host, {})[Modes.Setter] = self.types
        return actual_class
