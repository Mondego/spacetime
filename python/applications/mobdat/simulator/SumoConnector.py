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

@file    SumoConnector.py
@author  Mic Bowman
@date    2013-12-03

This file defines the SumoConnector class that translates mobdat events
and operations into and out of the sumo traffic simulator.

"""

import logging
import math
import os, sys
from datamodel.mobdat.datamodel import MobdatVehicle
from spacetime_local.declarations import Producer, GetterSetter, Deleter
from spacetime_local.IApplication import IApplication
from datamodel.common.datamodel import Vector3, Quaternion

import platform
import subprocess
from sumolib import checkBinary
import traci

import BaseConnector, EventHandler, EventTypes
import traci.constants as tc
from common.instrument import timethis

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
@Producer(MobdatVehicle)
@GetterSetter(MobdatVehicle)
@Deleter(MobdatVehicle)
class SumoConnector(BaseConnector.BaseConnector, IApplication) :

    # -----------------------------------------------------------------
    def __init__(self, settings, world, netsettings, cname, frame) :
        BaseConnector.BaseConnector.__init__(self, settings, world, netsettings)
        self.cars = {}
        self.__Logger = logging.getLogger(__name__)
        self.frame = frame

        # the sumo time scale is 1sec per iteration so we need to scale
        # to the 100ms target for our iteration time, this probably
        # should be computed based on the target step size
        self.TimeScale = 1.0 / self.Interval

        self.ConfigFile = settings["SumoConnector"]["ConfigFile"]
        self.Port = settings["SumoConnector"]["SumoPort"]
        self.TrafficLights = {}

        self.DumpCount = 50
        self.EdgesPerIteration = 25

        self.VelocityFudgeFactor = settings["SumoConnector"].get("VelocityFudgeFactor",0.90)

        self.AverageClockSkew = 0.0
        # self.LastStepTime = 0.0

        # for cf in settings["SumoConnector"].get("ExtensionFiles",[]) :
        #     execfile(cf,{"EventHandler" : self})


    # -----------------------------------------------------------------
    # -----------------------------------------------------------------
    def __NormalizeCoordinate(self,pos) :
        return Vector3((pos[0] - self.XBase) / self.XSize, (pos[1] - self.YBase) / self.YSize, 0.0)

    # -----------------------------------------------------------------
    # see http://www.euclideanspace.com/maths/geometry/rotations/conversions/eulerToQuaternion/
    # where heading is interesting and bank and attitude are 0
    # -----------------------------------------------------------------
    def __NormalizeAngle(self,heading) :
        # convert to radians
        heading = (2.0 * heading * math.pi) / 360.0
        return Quaternion.FromHeading(heading)

    # -----------------------------------------------------------------
    def __NormalizeVelocity(self, speed, heading) :
        # i'm not at all sure why the coordinates for speed are off
        # by 270 degrees... but this works
        heading = (2.0 * (heading + 270.0) * math.pi) / 360.0

        # the 0.9 multiplier just makes sure we dont overestimate
        # the velocity because of the time shifting, experience
        # is better if the car falls behind a bit rather than
        # having to be moved back because it got ahead
        x = self.VelocityFudgeFactor * self.TimeScale * speed * math.cos(heading)
        y = self.VelocityFudgeFactor * self.TimeScale * speed * math.sin(heading)

        return Vector3(x / self.XSize, y / self.YSize, 0.0)

    # -----------------------------------------------------------------
    def _RecomputeRoutes(self) :
        if len(self.CurrentEdgeList) == 0 :
            self.CurrentEdgeList = list(self.EdgeList)

        count = 0
        while self.CurrentEdgeList and count < self.EdgesPerIteration :
            edge = self.CurrentEdgeList.pop()
            traci.edge.adaptTraveltime(edge, traci.edge.getTraveltime(edge))
            count += 1

    # # -----------------------------------------------------------------
    # def AddVehicle(self, vehid, routeid, typeid) :
    #     traci.vehicle.add(vehid, routeid, typeID=typeid)

    # # -----------------------------------------------------------------
    # def GetTrafficLightState(self, identity) :
    #     return traci.trafficlights.getReadYellowGreenState(identity)

    # # -----------------------------------------------------------------
    # def SetTrafficLightState(self, identity, state) :
    #     return traci.trafficlights.setRedYellowGreenState(identity, state)

    # -----------------------------------------------------------------
    def HandleTrafficLights(self) :
        pass
        #changelist = traci.trafficlights.getSubscriptionResults()
        #for tl, info in changelist.iteritems() :
        #    state = info[tc.TL_RED_YELLOW_GREEN_STATE]
        #    if state != self.TrafficLights[tl] :
        #        self.TrafficLights[tl] = state
        #        event = EventTypes.EventTrafficLightStateChange(tl,state)
        #        self.PublishEvent(event)

    # -----------------------------------------------------------------
    def HandleInductionLoops(self) :
        pass
        #changelist = traci.inductionloop.getSubscriptionResults()
        #for il, info in changelist.iteritems() :
        #    count = info[tc.LAST_STEP_VEHICLE_NUMBER]
        #    if count > 0 :
        #        event = EventTypes.EventInductionLoop(il,count)
        #        self.PublishEvent(event)

    # -----------------------------------------------------------------
    def HandleDepartedVehicles(self) :
        dlist = traci.simulation.getDepartedIDList()
        for v in dlist :
            traci.vehicle.subscribe(v,[tc.VAR_POSITION, tc.VAR_SPEED, tc.VAR_ANGLE])

            vtype = traci.vehicle.getTypeID(v)
            pos = self.__NormalizeCoordinate(traci.vehicle.getPosition(v))

            car = self.cars[v]
            car.Position = pos
            car.Type = vtype

            #event = EventTypes.EventCreateObject(v, vtype, pos)
            #self.PublishEvent(event)

    # -----------------------------------------------------------------
    def HandleArrivedVehicles(self) :
        alist = traci.simulation.getArrivedIDList()
        for v in alist :
            #event = EventTypes.EventDeleteObject(v)
            #self.PublishEvent(event)
            self.__Logger.info("Vehicle %s arrived at %s", self.cars[v].Name, self.cars[v].Position)
            self.frame.delete(MobdatVehicle, self.cars[v])
            del self.cars[v]

    # -----------------------------------------------------------------
    @timethis
    def HandleVehicleUpdates(self) :
        changelist = traci.vehicle.getSubscriptionResults()
        for v, info in changelist.iteritems() :
            car = self.frame.get(MobdatVehicle, self.cars[v].ID)
            car.Position = self.__NormalizeCoordinate(info[tc.VAR_POSITION])
            car.Rotation = self.__NormalizeAngle(info[tc.VAR_ANGLE])
            car.Velocity = self.__NormalizeVelocity(info[tc.VAR_SPEED], info[tc.VAR_ANGLE])
            #self.__Logger.info("Vehicle %s at %s", car.Name, car.Position)
            #event = EventTypes.EventObjectDynamics(v, pos, ang, vel)
            #self.PublishEvent(event)

    # -----------------------------------------------------------------
    # def HandleRerouteVehicle(self, event) :
    #     traci.vehicle.rerouteTraveltime(str(event.ObjectIdentity))

    # -----------------------------------------------------------------
    def NewVehicles(self):
        added = self.frame.get_new(MobdatVehicle)
        #self.__Logger.debug("Tick SUMO: New vehicles %s", added)
        for car in added:
            self.__Logger.debug('add vehicle %s going from %s to %s', car.Name, car.Route, car.Target)
            traci.vehicle.add(car.Name, car.Route, typeID=car.Type)
            traci.vehicle.changeTarget(car.Name, car.Target)
            self.cars[car.Name] = car

    # -----------------------------------------------------------------
    # Returns True if the simulation can continue
    # @instrument TODO:
    def update(self):
        self.CurrentStep += 1
        self.CurrentTime = self.Clock()

        # Compute the clock skew
        self.AverageClockSkew = (9.0 * self.AverageClockSkew + (self.Clock() - self.CurrentTime)) / 10.0

        try :

            self.NewVehicles()
            traci.simulationStep()

            self.HandleInductionLoops()
            self.HandleTrafficLights()
            self.HandleDepartedVehicles()
            self.HandleVehicleUpdates()
            self.HandleArrivedVehicles()

            if (self.CurrentStep % int(5.0 / self.Interval)) == 0:
                self.__Logger.info('[%s] number of vehicles in simulation: %s', self.CurrentStep, len(self.cars.keys()))

        except TypeError as detail:
            self.__Logger.exception("[sumoconector] simulation step failed with type error %s" % (str(detail)))
            sys.exit(-1)
        except ValueError as detail:
            self.__Logger.exception("[sumoconector] simulation step failed with value error %s" % (str(detail)))
            sys.exit(-1)
        except NameError as detail:
            self.__Logger.exception("[sumoconector] simulation step failed with name error %s" % (str(detail)))
            sys.exit(-1)
        except AttributeError as detail:
            self.__Logger.exception("[sumoconnector] simulation step failed with attribute error %s" % (str(detail)))
            sys.exit(-1)
        except :
            self.__Logger.exception("[sumoconnector] error occured in simulation step; %s" % (sys.exc_info()[0]))
            sys.exit(-1)

        self._RecomputeRoutes()

        #TODO: Reimplement stats connector
        #if (self.CurrentStep % self.DumpCount) == 0 :
            #count = traci.vehicle.getIDCount()
            #event = EventTypes.SumoConnectorStatsEvent(self.CurrentStep, self.AverageClockSkew, count)
            #self.PublishEvent(event)

        return True

    # -----------------------------------------------------------------
    def HandleShutdownEvent(self) :
        try :
            idlist = traci.vehicle.getIDList()
            for v in idlist :
                traci.vehicle.remove(v)

            traci.close()
            sys.stdout.flush()

            self.SumoProcess.wait()
            self.__Logger.info('shut down')
        except :
            exctype, value =  sys.exc_info()[:2]
            self.__Logger.warn('shutdown failed with exception type %s; %s' %  (exctype, str(value)))

    # -----------------------------------------------------------------
    def initialize(self) :
        if platform.system() == 'Windows' or platform.system().startswith("CYGWIN"):
            sumoBinary = checkBinary('sumo.exe')
        else:
            sumoBinary = checkBinary('sumo')

        sumoCommandLine = [sumoBinary, "-c", self.ConfigFile, "-l", "sumo.log"]
        self.SumoProcess = subprocess.Popen(sumoCommandLine, stdout=sys.stdout, stderr=sys.stderr)
        traci.init(self.Port)

        self.SimulationBoundary = traci.simulation.getNetBoundary()
        self.XBase = self.SimulationBoundary[0][0]
        self.XSize = self.SimulationBoundary[1][0] - self.XBase
        self.YBase = self.SimulationBoundary[0][1]
        self.YSize = self.SimulationBoundary[1][1] - self.YBase
        #self.__Logger.warn("starting sumo connector")

        # initialize the edge list, drop all the internal edges
        self.EdgeList = []
        for edge in traci.edge.getIDList() :
            # this is just to ensure that everything is initialized first time
            traci.edge.adaptTraveltime(edge, traci.edge.getTraveltime(edge))

            # only keep the "real" edges for computation for now
            if not edge.startswith(':') :
                self.EdgeList.append(edge)
        self.CurrentEdgeList = list(self.EdgeList)

        # initialize the traffic light state
        tllist = traci.trafficlights.getIDList()
        for tl in tllist :
            self.TrafficLights[tl] = traci.trafficlights.getRedYellowGreenState(tl)
            traci.trafficlights.subscribe(tl,[tc.TL_RED_YELLOW_GREEN_STATE])

        # initialize the induction loops
        illist = traci.inductionloop.getIDList()
        for il in illist :
            traci.inductionloop.subscribe(il, [tc.LAST_STEP_VEHICLE_NUMBER])

        # For testing purposes
        #v1 = Vehicle()
        #v1.Route = "rCity00:mall222E245N"
        #v1.Target = "City10:mall230E245N=O=City10:mall222E245N"
        #v1.Name = "Model A BLUE"
        #v1.Type = "Model A BLUE"
        #self.frame.add(v1)
    def shutdown(self):
        pass
