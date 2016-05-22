'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from abc import ABCMeta, abstractmethod

class IApplication(object):
    __metaclass__ = ABCMeta
    __declaration_map__ = None

    @property
    def done(self):
        return False

    @done.setter
    @abstractmethod
    def done(self, value):
        pass

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass
