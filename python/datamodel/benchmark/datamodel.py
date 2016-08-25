'''
Created on Apr 28, 2016

@author: Arthur Valadares
'''
from __future__ import absolute_import

from pcc.set import pcc_set
from pcc.attributes import primarykey, dimension
from pcc.projection import projection
from datamodel.common.datamodel import Vector3
from pcc import subset, join, parameter
import random

@pcc_set
class BaseSet(object):
    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(str)
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    @dimension(int)
    def Number(self):
        return self._Number

    @Number.setter
    def Number(self, value):
        self._Number = value

    def __init__(self, num):
        self.ID = None
        self.Name = ""
        self.Number = num

@subset(BaseSet)
class Subset(BaseSet.Class()):
    @staticmethod
    def __query__(base):
        return [o for o in base if Subset.__predicate__(o)]

    @staticmethod
    def __predicate__(o):
        return o.Number % 2 == 0

@join(BaseSet, BaseSet)
class Join(object):

    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(BaseSet)
    def b1(self):
        return self._b1

    @b1.setter
    def b1(self, value):
        self._b1 = value

    @dimension(BaseSet)
    def b2(self):
        return self._b2

    @b2.setter
    def b2(self, value):
        self._b2 = value

    def __init__(self, p, c):
        self.car = c
        self.pedestrian = p

    @staticmethod
    def __query__(b1s, b2s):
        return [(b1, b2) for b1 in b1s for b2 in b2s if Join.__predicate__(b1, b2)]

    @staticmethod
    def __predicate__(b1, b2):
        if b1 == b2:
            return b1.Number % 2 == 0

@parameter(BaseSet)
@subset(BaseSet)
class Parameter(BaseSet.Class()):
    @staticmethod
    def __query__(b1s, b2s):
        return [b1 for b1 in b1s if Parameter.__predicate__(b1, b2s)]

    @staticmethod
    def __predicate__(b1, b2s):
        return b1.Number % 2 == 0
