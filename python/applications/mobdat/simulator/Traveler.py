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
from . import Trip, LocationKeyMap, TravelerProfiles

from applications.mobdat.common import TravelTimeEstimator
from applications.mobdat.common.timedevent import TimedEvent, TimedEventList
from applications.mobdat.common.graph import SocialDecoration
from applications.mobdat.common.graph.SocialDecoration import BusinessType

logger = logging.getLogger(__name__)


# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class Traveler :

    # -----------------------------------------------------------------
    def __init__(self, person, connector) :
        """
        Args:
            person -- Graph.Node (NodeType == Person) or SocialNodes.Person
            connector -- SocialConnector
        """
        self.Connector = connector
        self.World = self.Connector.World

        self.Person = person

        self.LocationKeyMap = LocationKeyMap.LocationKeyMap(self.World, self.Person)
        self.TravelEstimator = TravelTimeEstimator.TravelTimeEstimator()
        self.TravelerProfile = TravelerProfiles.ProfileMap[self.Person.PersonProfile.Name](self.Person, self.World, self.TravelEstimator)

        self.EventList = None
        if self.BuildDailyEvents(self.Connector.WorldDay) :
            self._ScheduleFirstTrip()

    # -----------------------------------------------------------------
    def FindBusinessByType(self, biztype, bizclass) :
        if (biztype, bizclass) not in self.BusinessCache :
            predicate = SocialDecoration.BusinessProfileDecoration.BusinessTypePred(biztype, bizclass)
            self.BusinessCache[(biztype, bizclass)] = self.World.FindNodes(nodetype = 'Business', predicate = predicate)

        return self.BusinessCache[(biztype, bizclass)]

    # -----------------------------------------------------------------
    def InitializeLocationNameMap(self) :
        self.LocationNameMap = {}
        self.LocationNameMap['home'] = [ self.Person ]
        self.LocationNameMap['work'] = [ self.Employer ]

        self.LocationNameMap['coffee'] = self.FindBusinessByType(BusinessType.Food, 'coffee')  # @UndefinedVariable
        self.LocationNameMap['lunch'] = self.FindBusinessByType(BusinessType.Food, 'fastfood')  # @UndefinedVariable
        self.LocationNameMap['dinner'] = self.FindBusinessByType(BusinessType.Food, 'small-restaurant')  # @UndefinedVariable
        self.LocationNameMap['shopping'] = self.FindBusinessByType(BusinessType.Service, None)  # @UndefinedVariable

    # -----------------------------------------------------------------
    def ResolveLocationName(self, name) :
        location = random.choice(self.LocationNameMap[name])
        return location.ResidesAt

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

        evlist = self.TravelerProfile.BuildDailyEvents(worldday, addextras)

        # attempt to solve the constraints, if it doesn't work, then try
        # again with just work, really need to add an "optional" or "maximal"
        # notion to the constraint solver
        if not evlist.SolveConstraints() :
            if addextras :
                logger.info('Trip constraints failed for traveler %s', self.Person.Name)
                return self.BuildDailyEvents(worldday, False)
            else :
                logger.warn('Failed to resolve schedule constraints for traveler %s', self.Person.Name)
                self.EventList = None
                return False

        self.EventList = evlist
        return True

    # -----------------------------------------------------------------
    def _ScheduleFirstTrip(self) :
        """
        _ScheduleFirstTrip -- schedule the first trip, this is a unique situation because the world time
        might be in the middle of the current day, start the trip where the traveler would be at that time
        """

        source = self.LocationKeyMap.ResolveLocationKey('home')
        while self.EventList.MoreTripEvents() :
            tripev = self.EventList.PopTripEvent()
            starttime = float(tripev.StartTime)

            if starttime > self.Connector.WorldTime :
                destination = self.LocationKeyMap.ResolveLocationKey(tripev.DstName)
                self.Connector.AddTripToEventQueue(Trip.Trip(self, starttime, source, destination))

                logger.debug('Scheduled trip of to %s for %s from %s to %s', tripev.DstName, self.Person.Name, source.Name, destination.Name)
                return

            # this just allows us to start in the middle of the day, traveler at work
            # will start at work rather than starting at home, we start wherever we
            # supposedly ended the last trip
            source = self.LocationKeyMap.ResolveLocationKey(tripev.DstName)

        logger.info('No trip events for %s', self.Person.Name)

    # -----------------------------------------------------------------
    def _ScheduleNextTrip(self, source) :
        """
        _ScheduleNextTrip -- schedule the next trip for this traveler
        """
        if not self.EventList :
            return

        # this builds out daily events up to one week in the future with
        # the idea that we should be able to go through at least a full
        # work week looking for something interesting to happen
        for day in range(0, 7) :
            if self.EventList.MoreTripEvents() : break
            self.BuildDailyEvents(self.Connector.WorldDay + day + 1)

        tripev = self.EventList.PopTripEvent()
        starttime = float(tripev.StartTime)

        # there is a reasonable chance that this is a moot trip (for example, a person
        # works at a coffee shop and gets their coffee there, if that happens then
        # just skip this trip & schedule another one, an alternative would be to
        # drop the work place from all choices of service destinations
        destination = self.LocationKeyMap.ResolveLocationKey(tripev.DstName)
        if source.Name == destination.Name :
            return self._ScheduleNextTrip(source)

        self.Connector.AddTripToEventQueue(Trip.Trip(self, starttime, source, destination))

        logger.info('Scheduled trip of to %s for %s from %s to %s', tripev.DstName, self.Person.Name, source.Name, destination.Name)

    # -----------------------------------------------------------------
    def TripCompleted(self, trip) :
        """
        TripCompleted -- event handler called by the connector when the trip is completed

        Args:
            trip -- initialized Trip object
        """
        self.TravelEstimator.SaveTravelTime(trip.Source, trip.Destination, self.Connector.WorldTime - trip.ActualStartTime)
        self._ScheduleNextTrip(trip.Destination)

    # -----------------------------------------------------------------
    def TripStarted(self, trip) :
        pass

