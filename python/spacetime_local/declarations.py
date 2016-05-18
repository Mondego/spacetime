'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

class DataAgent(object):
    def __init__(self, keywords):
        if 'host' in keywords:
            self.host = keywords['host']
        else:
            self.host = 'http://127.0.0.1:12000'

class Producer(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        actual_class.__pcc_producing__ = self.types
        return actual_class

class Tracker(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        actual_class.__pcc_tracking__ = self.types
        return actual_class

class Getter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        actual_class.__pcc_getting__ = self.types
        return actual_class

class GetterSetter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        actual_class.__pcc_gettingsetting__ = self.types
        return actual_class

class Deleter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        actual_class.__pcc_deleting__ = self.types
        return actual_class

class Setter(DataAgent):
    def __init__(self, *types, **keywords):
        self.types = set(types)
        DataAgent.__init__(self, keywords)

    def __call__(self, actual_class):
        actual_class.__pcc_setting__ = self.types
        return actual_class
