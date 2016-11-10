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
class NullSet(object):
    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

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

    @dimension(list)
    def List(self):
        return self._List

    @List.setter
    def List(self, value):
        self._List = value

    @dimension(dict)
    def Dictionary(self):
        return self._Dictionary

    @Dictionary.setter
    def Dictionary(self, value):
        self._Dictionary = value

    #@dimension(set)
    #def Set(self):
    #    return self._Set

    #@Set.setter
    #def Set(self, value):
    #    self._Set = value

    @dimension(str)
    def Property1(self):
        return self._Property1

    @Property1.setter
    def Property1(self, value):
        self._Property1 = value

    @dimension(str)
    def Property2(self):
        return self._Property2

    @Property2.setter
    def Property2(self, value):
        self._Property2 = value

    @dimension(str)
    def Property3(self):
        return self._Property3

    @Property3.setter
    def Property3(self, value):
        self._Property3 = value

    @dimension(str)
    def Property4(self):
        return self._Property4

    @Property4.setter
    def Property4(self, value):
        self._Property4 = value

    @dimension(str)
    def Property5(self):
        return self._Property5

    @Property5.setter
    def Property5(self, value):
        self._Property5 = value

    @dimension(str)
    def Property6(self):
        return self._Property6

    @Property6.setter
    def Property6(self, value):
        self._Property6 = value

    def __init__(self, num):
        self.ID = None
        self.Name = ""
        self.Number = num
        self.List = [i for i in xrange(20)]
        #self.Set = set([i for i in xrange(20)])
        self.Dictionary = { str(k) : k for k in xrange(20)}
        self.Property1 = "Property 1"
        self.Property2 = "Property 2"
        self.Property3 = "Property 3"
        self.Property4 = "Property 4"
        self.Property5 = "Property 5"


@projection(BaseSet, BaseSet.ID, BaseSet.Name)
class BaseSetProjection(object):
    @property
    def DecoratedName(self):
        return "** " + self.Name + " **"


@subset(BaseSet)
class SubsetHalf(BaseSet):
    @staticmethod
    def __query__(base):
        return [o for o in base if SubsetHalf.__predicate__(o)]

    @staticmethod
    def __predicate__(o):
        return o.Number % 2 == 0

@subset(BaseSet)
class SubsetAll(BaseSet):
    @staticmethod
    def __query__(base):
        return [o for o in base if SubsetAll.__predicate__(o)]

    @staticmethod
    def __predicate__(o):
        return True

@join(BaseSet, BaseSet)
class JoinHalf(object):

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

    def __init__(self, b1, b2):
        self.b1 = b1
        self.b2 = b2

    @staticmethod
    def __query__(b1s, b2s):
        return [(b1, b2) for b1 in b1s for b2 in b2s if JoinHalf.__predicate__(b1, b2)]

    @staticmethod
    def __predicate__(b1, b2):
        if b1 == b2:
            return b1.Number % 2 == 0

@join(BaseSet, BaseSet)
class JoinAll(object):

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

    def __init__(self, b1, b2):
        self.b1 = b1
        self.b2 = b2

    @staticmethod
    def __query__(b1s, b2s):
        return [(b1, b2) for b1 in b1s for b2 in b2s if JoinAll.__predicate__(b1, b2)]

    @staticmethod
    def __predicate__(b1, b2):
        if b1.ID == b2.ID:
            return True

@parameter(BaseSet)
@subset(BaseSet)
class ParameterHalf(BaseSet):
    @staticmethod
    def __query__(b1s, b2s):
        return [b1 for b1 in b1s if ParameterHalf.__predicate__(b1, b2s)]

    @staticmethod
    def __predicate__(b1, b2s):
        return b1.Number % 2 == 0

@parameter(BaseSet)
@subset(BaseSet)
class ParameterAll(BaseSet):
    @staticmethod
    def __query__(b1s, b2s):
        return [b1 for b1 in b1s if ParameterAll.__predicate__(b1, b2s)]

    @staticmethod
    def __predicate__(b1, b2s):
        return True
