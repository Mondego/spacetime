'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from abc import ABCMeta, abstractmethod

class IApplication(object):
    __metaclass__ = ABCMeta
    __declaration_map__ = None
    __special_wire_format__ = None

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
