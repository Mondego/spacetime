#!/usr/bin/env python
"""
Copyright (c) 2014, Intel Corporation

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

* Neither the name of Intel Corporation nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

@file    LocationKeyMap.py
@author  Mic Bowman
@date    2014-04-02

This module defines the SocialConnector class. This class implements
the social (people) aspects of the mobdat simulation.

"""

import os, sys
import logging

import random
from . import Trip

from applications.mobdat.common import TravelTimeEstimator, ValueTypes
from applications.mobdat.common.timedevent import TimedEvent, TimedEventList, IntervalVariable
from applications.mobdat.common.graph import SocialDecoration

logger = logging.getLogger(__name__)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class LocationKeyMap :
    __BusinessCache = {}

    # -----------------------------------------------------------------
    def __init__(self, world, person) :

        self.World = world
        self.Person = person

        self.LocationKeyMap = {}
        self.LocationKeyMap['home'] = ValueTypes.WeightedChoice({ self.Person : 1.0 })
        self.LocationKeyMap['work'] = ValueTypes.WeightedChoice({ self.Person.EmployedBy : 1.0 })

        self.AddLocationKey('coffee', SocialDecoration.BusinessType.Food, 'coffee')
        self.AddLocationKey('lunch', SocialDecoration.BusinessType.Food, 'fastfood')
        self.AddLocationKey('dinner', SocialDecoration.BusinessType.Food, 'small-restaurant')
        self.AddLocationKey('shopping', SocialDecoration.BusinessType.Service, None)

    # -----------------------------------------------------------------
    def _CreatePreferenceList(self, bizlist) :
        """
        Args:
            bizlist -- non-empty list of SocialNodes.Business objects
        """

        if not bizlist :
            raise ValueError("no businesses specified for preference list for %s" % self.Person.Name)

        plist = ValueTypes.WeightedChoice()
        for biz in bizlist :
            weight = self.Person.Preference.GetWeight(biz.Name)
            if weight : plist.AddChoice(biz, weight)

        # make sure there is at least one on the list
        if not plist.Choices() :
            plist.AddChoice(random.choice(bizlist), 1.0)

        return plist

    # -----------------------------------------------------------------
    def _FindBizByType(self, biztype, bizclass) :
        """
        Args:
            biztype -- SocialDecoration.BusinessType
            bizclass -- string name of the subtype of business
        """

        if (biztype, bizclass) not in self.__BusinessCache :
            self.__BusinessCache[(biztype, bizclass)] = SocialDecoration.BusinessProfileDecoration.FindByType(self.World, biztype, bizclass)

        return self.__BusinessCache[(biztype, bizclass)]

    # -----------------------------------------------------------------
    def AddLocationKey(self, name, biztype, bizclass) :
        """
        Args:
            name -- the location key used to retrieve the business choice
            biztype -- SocialDecoration.BusinessType
            bizclass -- string name of the subtype of business
        """
        self.LocationKeyMap[name] = self._CreatePreferenceList(self._FindBizByType(biztype, bizclass))

    # -----------------------------------------------------------------
    def ResolveLocationKey(self, name) :
        """
        Args:
            name -- the location key used to retrieve the business choice
        """
        location = self.LocationKeyMap[name].Choose()
        return location.ResidesAt

