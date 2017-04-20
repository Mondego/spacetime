'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from abc import ABCMeta, abstractmethod
import re

class IApplication(object):
    __metaclass__ = ABCMeta
    __declaration_map__ = None
    __special_wire_format__ = None
    
    @property
    def app_id(self):
        try:
            return self.__app_id
        except AttributeError:
            import uuid
            self.__app_id = str(uuid.uuid4())
            return self.__app_id
    @app_id.setter
    def app_id(self, v): 
        self.__app_id = re.sub(r"\s+", "_", v, flags = re.DOTALL | re.UNICODE)

    @property
    def done(self):
        try:
            return self.__done
        except AttributeError:
            return False

    @done.setter
    def done(self, value):
        self.__done = value

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass
