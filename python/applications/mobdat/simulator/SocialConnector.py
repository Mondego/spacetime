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

@file    SocialConnector.py
@author  Mic Bowman
@date    2013-12-03

This module defines the SocialConnector class. This class implements
the social (people) aspects of the mobdat simulation.

"""

import os, sys
import logging
import json
from datamodel.mobdat.datamodel import BusinessNode, MobdatVehicle, Person,\
    ResidentialNode, Road, SimulationNode
from spacetime_local.declarations import Producer, Tracker
from spacetime_local.IApplication import IApplication
from common.converter import create_obj

import heapq
import BaseConnector, EventHandler, EventTypes, Traveler

DEBUG = False

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
@Producer(MobdatVehicle, Person, BusinessNode, Road, ResidentialNode, SimulationNode)
@Tracker(MobdatVehicle)
class SocialConnector(BaseConnector.BaseConnector, IApplication):

    # -----------------------------------------------------------------
    def __init__(self, settings, world, netsettings, cname, frame) :
        #EventHandler.EventHandler.__init__(self, evrouter)
        BaseConnector.BaseConnector.__init__(self, settings, world, netsettings)
        self.frame = frame
        self.__Logger = logging.getLogger(__name__)

        self.MaximumTravelers = int(settings["General"].get("MaximumTravelers", 0))
        self.TripCallbackMap = {}
        self.TripTimerEventQ = []
        self.DataFolder = settings["General"]["Data"]
        if DEBUG:
            self.MaximumTravelers = 100
            self.ExperimentMode = False
        else:
            if "ExperimentMode" in settings["SocialConnector"]:
                self.ExperimentMode = settings["SocialConnector"]["ExperimentMode"]
                if self.ExperimentMode:
                    try:
                        fname = settings["Experiment"]["TravelerFilePath"]
                        data_path = settings["General"]["Data"]

                        with open(os.path.join(data_path,fname)) as data_file:
                            self.TravelerList = json.load(data_file)
                    except:
                        self.__Logger.error("could not read traveler information for experiment. Aborting experiment mode.")
                        self.ExperimentMode = False
            else:
                self.ExperimentMode = False

        self.Travelers = {}
        self.CreateTravelers()

        self.AddBuildings()
        #self.__Logger.warn('SocialConnector initialization complete')

    # -----------------------------------------------------------------
    def AddTripToEventQueue(self, trip) :
        heapq.heappush(self.TripTimerEventQ, trip)

    # -----------------------------------------------------------------
    def AddBuildings(self) :
        pass

    # -----------------------------------------------------------------
    def CreateTravelers(self) :
        #for person in self.PerInfo.PersonList.itervalues() :
        count = 0
        if self.ExperimentMode:
            self.__Logger.warn('Running SocialConnector in experiment mode.')
            for name in self.TravelerList:
                person = self.World.FindNodeByName(name)
                if count % 100 == 0 :
                    self.__Logger.warn('%d travelers created', count)
                traveler = Traveler.Traveler(person, self)
                self.Travelers[name] = traveler
                count += 1
        else:
            for name, person in self.World.IterNodes(nodetype = 'Person') :
                if count % 100 == 0 :
                    self.__Logger.warn('%d travelers created', count)

                traveler = Traveler.Traveler(person, self)
                self.Travelers[name] = traveler

                count += 1
                if self.MaximumTravelers > 0 and self.MaximumTravelers < count :
                    break


    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # EVENT GENERATORS
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    # -----------------------------------------------------------------
    def GenerateTripBegEvent(self, trip) :
        """
        GenerateTripBegEvent -- create and publish a 'tripstart' event
        at the beginning of a trip

        trip -- object of type Trip
        """
        pname = trip.Traveler.Person.Name
        tripid = trip.TripID
        sname = trip.Source.Name
        dname = trip.Destination.Name

        #event = EventTypes.TripBegStatsEvent(self.CurrentStep, pname, tripid, sname, dname)
        #self.PublishEvent(event)


    # -----------------------------------------------------------------
    def GenerateTripEndEvent(self, trip) :
        """
        GenerateTripEndEvent -- create and publish an event to capture
        statistics about a completed trip

        trip -- a Trip object for a recently completed trip
        """
        pname = trip.Traveler.Person.Name
        tripid = trip.TripID
        sname = trip.Source.Name
        dname = trip.Destination.Name

        #event = EventTypes.TripEndStatsEvent(self.CurrentStep, pname, tripid, sname, dname)
        #self.PublishEvent(event)

    # -----------------------------------------------------------------
    def GenerateAddVehicleEvent(self, trip) :
        """
        GenerateAddVehicleEvent -- generate an AddVehicle event to start
        a new trip

        trip -- Trip object initialized with traveler, vehicle and destination information
        """

        #vname = str(trip.VehicleName)
        #vtype = str(trip.VehicleType)
        #rname = str(trip.Source.Capsule.DestinationName)
        #tname = str(trip.Destination.Capsule.SourceName)
        v = MobdatVehicle()
        v.Name = trip.VehicleName
        v.Type = trip.VehicleType
        v.Route = trip.Source.Capsule.DestinationName
        v.Target = trip.Destination.Capsule.SourceName
        self.frame.add(v)

        self.__Logger.debug('add vehicle %s of type %s from %s to %s',v.Name, v.Type, v.Route, v.Target)

        # save the trip so that when the vehicle arrives we can get the trip
        # that caused the car to be created
        self.TripCallbackMap[v.Name] = trip

        #event = EventTypes.EventAddVehicle(vname, vtype, rname, tname)
        #self.PublishEvent(event)

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # EVENT HANDLERS
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    # -----------------------------------------------------------------
    def HandleDeleteVehicles(self) :
        """
        HandleDeleteObjectEvent -- delete object means that a car has completed its
        trip so record the stats and add the next trip for the person

        event -- a DeleteObject event object
        """
        deleted = self.frame.get_deleted(MobdatVehicle)
        # self.__Logger.debug("Tick SUMO: New vehicles %s", added)
        for car in deleted:
            self.__Logger.warn("### Car %s has arrived, completing his trip.", car.Name)
            trip = self.TripCallbackMap.pop(car.Name)
            trip.TripCompleted(self)

    # -----------------------------------------------------------------
    # @instrument TODO
    def update(self) :
        """
        HandleTimerEvent -- timer event happened, process pending events from
        the eventq

        event -- Timer event object
        """
        self.CurrentStep = self.frame.get_curstep()
        self.HandleDeleteVehicles()
        if self.CurrentStep % 100 == 0 :
            wtime = self.WorldTime
            qlen = len(self.TripTimerEventQ)
            stime = self.TripTimerEventQ[0].ScheduledStartTime if self.TripTimerEventQ else 0.0
            self.__Logger.info('at time %0.3f, timer queue contains %s elements, next event scheduled for %0.3f', wtime, qlen, stime)


        while self.TripTimerEventQ :
            if self.TripTimerEventQ[0].ScheduledStartTime > self.WorldTime :
                break

            trip = heapq.heappop(self.TripTimerEventQ)
            trip.TripStarted(self)

    # -----------------------------------------------------------------
    def HandleShutdownEvent(self, event) :
        pass

    # -----------------------------------------------------------------
    def initialize(self, limit = None) :
        self.mybusiness = None
        # Limit the number of objects for testing purposes
        ilimit = 0
        plimit = None
        rlimit = None
        blimit = None
        rolimit = 0
        if DEBUG:
            plimit = 10
            rlimit = 10
            blimit = 10
        if self.DataFolder:
            try:
                f = open(os.path.join(self.DataFolder,"people.js"), "r")
                jsonlist = json.loads(f.read())
                f.close()
                for objjson in jsonlist[:plimit:]:
                    person = create_obj(Person, objjson)
                    self.frame.add(person)
            except:
                self.__Logger.exception("could not read data from people.js")

            try:
                f = open(os.path.join(self.DataFolder,"business.js"), "r")
                jsonlist = json.loads(f.read())
                f.close()
                for objjson in jsonlist[:blimit:]:
                    business = create_obj(BusinessNode, objjson)
                    self.frame.add(business)
            except:
                self.__Logger.exception("could not read data from business.js")

            try:
                f = open(os.path.join(self.DataFolder,"residential.js"), "r")
                jsonlist = json.loads(f.read())
                f.close()
                for objjson in jsonlist[:rlimit:]:
                    residence = create_obj(ResidentialNode, objjson)
                    self.frame.add(residence)
            except:
                self.__Logger.exception("could not read data from residential.js")

            try:
                f = open(os.path.join(self.DataFolder,"roads.js"), "r")
                jsonlist = json.loads(f.read())
                f.close()
                for objjson in jsonlist[:blimit:]:
                    road = create_obj(Road, objjson)
                    self.frame.add(road)
            except:
                self.__Logger.exception("could not read data from roads.js")

            try:
                f = open(os.path.join(self.DataFolder,"intersections.js"), "r")
                jsonlist = json.loads(f.read())
                f.close()
                for objjson in jsonlist[:ilimit:]:
                    intersection = create_obj(SimulationNode, objjson)
                    self.frame.add(intersection)
            except:
                self.__Logger.exception("could not read data from roads.js")
        #self.SubscribeEvent(EventTypes.EventDeleteObject, self.HandleDeleteObjectEvent)
        #self.SubscribeEvent(EventTypes.TimerEvent, self.HandleTimerEvent)
        #self.SubscribeEvent(EventTypes.ShutdownEvent, self.HandleShutdownEvent)
        #self.__Logger.info("Simulation started!")
        # all set... time to get to work!
        #self.HandleEvents()

    def shutdown(self):
        pass
