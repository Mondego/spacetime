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

@file    Traveler.py
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
class TravelRule :

    def __init__(self, world, person) :
        self.World = world
        self.Person = person

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class WorkRule(TravelRule) :

    # -----------------------------------------------------------------
    @staticmethod
    def _AddEvent(evlist, event, schedule, deviation = 2.0) :
        duration = schedule.WorldEndTime - schedule.WorldStartTime
        sinterval = (schedule.WorldStartTime - deviation, schedule.WorldStartTime + deviation)
        einterval = (schedule.WorldEndTime - deviation, schedule.WorldEndTime + deviation)

        workEV = TimedEvent.AggregateDurationEvent.Create('work', 0.0, sinterval, einterval, duration)
        workID = evlist.AddPlaceEvent(workEV)
        evlist.InsertWithinPlaceEvent(event, workID)

        return workID

    # -----------------------------------------------------------------
    def __init__(self, world, person) :
        TravelRule.__init__(self, world, person)

        self.Job = self.Person.JobDescription

    # -----------------------------------------------------------------
    def Apply(self, worldday, evlist) :
        worldtime = worldday * 24.0
        schedule = self.Job.Schedule.NextScheduledEvent(worldtime)

        if schedule.Day == worldday :
            jobdeviation = 2.0 if self.Job.FlexibleHours else 0.2
            return self._AddEvent(evlist, evlist.LastEvent.EventID, schedule, deviation = jobdeviation)

        return None

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class CoffeeBeforeWorkRule(TravelRule) :

    # -----------------------------------------------------------------
    @staticmethod
    def _AddEvent(evlist, workevent, worldtime, interval = (0.0, 24.0), duration = 0.2) :
        """Add a PlaceEvent for coffee before a work event. This moves the
        coffee event as close as possible to the work event.
        """

        event = TimedEvent.PreEventEvent.Create('coffee', worldtime, interval, interval, duration)
        idc = evlist.AddPlaceEvent(event)

        evlist.InsertAfterPlaceEvent(evlist.PrevPlaceID(workevent.EventID), idc)

        return idc

    # -----------------------------------------------------------------
    def __init__(self, world, person) :
        TravelRule.__init__(self, world, person)

        try :
            self.ServiceProfile = self.World.FindNodeByName('coffee').ServiceProfile
        except :
            logger.error('unable to locate service profile for coffee')
            sys.exit(-1)

    # -----------------------------------------------------------------
    def Apply(self, worldday, evlist) :
        if random.uniform(0.0, 1.0) < self.Person.Preference.GetWeight('rule_CoffeeBeforeWork', 0.6) :
            return None

        worldtime = worldday * 24.0
        worklist = evlist.FindEvents(lambda ev : ev.Details == 'work')

        for workev in worklist :
            if worklist[0].EventStart.Overlaps(worldtime + 3.0, worldtime + 10.0) :
                duration = self.ServiceProfile.ServiceTime
                interval = self.ServiceProfile.Schedule.ScheduleForDay(worldday)

                return self._AddEvent(evlist, workev, worldtime, interval[0], duration)

        return None

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class LunchDuringWorkRule(TravelRule) :

    # -----------------------------------------------------------------
    @staticmethod
    def _AddEvent(evlist, workevent, worldtime) :
        """Add a PlaceEvent for lunch during a work event.
        """

        event = TimedEvent.VariableMiddleEvent.Create('lunch', worldtime, (11.5, 13.0), (12.5, 14.0), 0.75)
        idl = evlist.AddPlaceEvent(event)

        evlist.InsertWithinPlaceEvent(workevent.EventID, idl)

        return idl

    # -----------------------------------------------------------------
    def __init__(self, world, person) :
        TravelRule.__init__(self, world, person)

    # -----------------------------------------------------------------
    def Apply(self, worldday, evlist) :
        if random.uniform(0.0, 1.0) < self.Person.Preference.GetWeight('rule_LunchDuringWork', 0.75) :
            return None

        worldtime = worldday * 24.0
        worklist = evlist.FindEvents(lambda ev : ev.Details == 'work')

        for workev in worklist :
            if workev.EventStart.LT(worldtime + 10.0) and workev.EventEnd.GT(worldtime + 15.0) :
                return self._AddEvent(evlist, workev, worldtime)

        return None

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class DinnerRule(TravelRule) :

    # -----------------------------------------------------------------
    @staticmethod
    def _AddEvent(evlist, worldtime, workevent = None, interval = (0.0, 24.0)) :
        """Add a PlaceEvent for dinner after a work event.
        """

        if workevent :
            event = TimedEvent.PostEventEvent.Create('dinner', worldtime, interval, interval, 1.5)
            idr = evlist.AddPlaceEvent(event)

            evlist.InsertAfterPlaceEvent(workevent.EventID, idr)
        else :
            event = TimedEvent.VariableMiddleEvent.Create('dinner', worldtime, interval, interval, 1.5)
            idr = evlist.AddPlaceEvent(event)

            evlist.InsertWithinPlaceEvent(evlist.LastEvent.EventID, idr)

        return idr

    # -----------------------------------------------------------------
    def __init__(self, world, person) :
        TravelRule.__init__(self, world, person)

    # -----------------------------------------------------------------
    def Apply(self, worldday, evlist) :
        if random.uniform(0.0, 1.0) < self.Person.Preference.GetWeight('rule_RestaurantAfterWork', 0.8) :
            return None

        worldtime = worldday * 24.0
        worklist = evlist.FindEvents(lambda ev : ev.Details == 'work')

        for workev in worklist :
            if workev.EventEnd.GT(worldtime + 15.0) and workev.EventEnd.LT(worldtime + 20.0) :
                return self._AddEvent(evlist, worldtime, workevent = workev)

        return self._AddEvent(evlist, worldtime, interval = (16.0, 22.0))

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class ShoppingTripRule(TravelRule) :

    # -----------------------------------------------------------------
    @staticmethod
    def _AddEvent(evlist, worldtime, interval = (7.0, 22.0), maxcount = 4, prevevent = None) :
        # happens between 7am and 10pm

        if prevevent :
            ids = prevevent.EventID
        else :
            event = TimedEvent.VariableMiddleEvent.Create('shopping', worldtime, interval, interval, 0.5)
            ids = evlist.AddPlaceEvent(event)

            evlist.InsertWithinPlaceEvent(evlist.LastEvent.EventID, ids)

        stops = int(random.triangular(0, maxcount, 1))
        while stops > 0 :
            stops = stops - 1

            postev = TimedEvent.PostEventEvent.Create('shopping', worldtime, interval, interval, 0.5)
            idnew = evlist.AddPlaceEvent(postev)
            evlist.InsertAfterPlaceEvent(ids, idnew)
            ids = idnew

    # -----------------------------------------------------------------
    def __init__(self, world, person) :
        TravelRule.__init__(self, world, person)

    # -----------------------------------------------------------------
    def Apply(self, worldday, evlist) :
        if random.uniform(0.0, 1.0) < self.Person.Preference.GetWeight('rule_ShoppingTrip', 0.8) :
            return None

        worldtime = worldday * 24.0
        dinnerlist = evlist.FindEvents(lambda ev : ev.Details == 'dinner')
        if dinnerlist :
            dinnerev = dinnerlist[-1]
            return self._AddEvent(evlist, worldtime, maxcount = 2, prevevent = dinnerev)

        return self._AddEvent(evlist, worldtime)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
RuleMap = {
    'Work' : WorkRule,
    'CoffeeBeforeWork' : CoffeeBeforeWorkRule,
    'LunchDuringWork' : LunchDuringWorkRule,
    'Dinner' : DinnerRule,
    'ShoppingTrip' : ShoppingTripRule
    }

def BuildRuleMap(world, person) :
    rulemap = {}
    for name, rule in RuleMap.iteritems() :
        rulemap[name] = rule(world, person)

    return rulemap


