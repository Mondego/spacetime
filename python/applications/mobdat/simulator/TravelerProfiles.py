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

@file    TravelerProfiles.py
@author  Mic Bowman
@date    2014-04-02

This module defines the SocialConnector class. This class implements
the social (people) aspects of the mobdat simulation.

"""

import os, sys
import logging

import random
from . import TravelRules

from applications.mobdat.common import TravelTimeEstimator
from applications.mobdat.common.timedevent import TimedEvent, TimedEventList

logger = logging.getLogger(__name__)


# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class TravelerProfile :

    # -----------------------------------------------------------------
    def __init__(self, person, world, estimator) :
        """
        Args:
            person -- Graph.Node (NodeType == Person) or SocialNodes.Person
            connector -- SocialConnector
        """
        self.World = world
        self.Person = person
        self.TravelEstimator = estimator

        self.RuleMap = TravelRules.BuildRuleMap(self.World, self.Person)

    # -----------------------------------------------------------------
    def BuildDailyEvents(self, worldday, addextras = True) :
        return None

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class DailyWorkerProfile(TravelerProfile) :

    # -----------------------------------------------------------------
    def BuildDailyEvents(self, worldday, addextras = True) :
        """
        BuildDailyEvents -- build a days worth of events for the person
        based on a static set of rules. This is still too hard-coded for me...
        Everyone shares the same set of rules though personalities would have
        a lot to do with the rules. That is, this should be abstracted
        and implemented in the person profiles object.

        Args:
            worldday -- integer, day to create
            addextras -- boolean, flag to shortcut optional rules

        """

        logger.debug('Compute day %d schedule for %s', worldday, self.Person.Name)

        homeev = TimedEvent.BackgroundEvent.Create('home', 0.0, (0.0, 0.0), (24.0 * 1000.0, 24.0 * 1000.0))
        evlist = TimedEventList.TimedEventList(homeev, estimator = self.TravelEstimator)

        if self.RuleMap['Work'].Apply(worldday, evlist) :

            if addextras :
                self.RuleMap['CoffeeBeforeWork'].Apply(worldday, evlist)
                self.RuleMap['LunchDuringWork'].Apply(worldday, evlist)
                self.RuleMap['Dinner'].Apply(worldday, evlist)
                self.RuleMap['ShoppingTrip'].Apply(worldday, evlist)

        # if its not a work day, then see if we should go shopping
        else :
            self.RuleMap['Dinner'].Apply(worldday, evlist)
            self.RuleMap['ShoppingTrip'].Apply(worldday, evlist)

        return evlist


# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
ProfileMap = {
    'worker' : DailyWorkerProfile,
    'student' : TravelerProfile,
    'homemaker' : TravelerProfile
    }

