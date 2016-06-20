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

@file    EventTypes.py
@author  Mic Bowman
@date    2013-12-03

This module defines a variety of event classes.

"""

import os, sys

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class SubscribeEvent :
    # -----------------------------------------------------------------
    def __init__(self, handler, evtype) :
        self.Handler = handler
        self.EventType = evtype

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class UnsubscribeEvent :
    # -----------------------------------------------------------------
    def __init__(self, handler, evtype) :
        self.Handler = handler
        self.EventType = evtype

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class ShutdownEvent :
    # -----------------------------------------------------------------
    def __init__(self, router) :
        self.RouterShutdown = router

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class StatsEvent :
    # -----------------------------------------------------------------
    def __init__(self, timestep, skey = None) :
        self.StatKey = self.__class__.__name__ if not skey else skey
        self.CurrentStep = timestep

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class TripStatsEvent(StatsEvent) :
    # -----------------------------------------------------------------
    def __init__(self, timestep, statkey, person, tripid, snode, dnode) :
        StatsEvent.__init__(self, timestep, statkey)

        self.Person = person
        self.TripID = tripid
        self.SourceNode = snode
        self.DestinationNode = dnode

    # -----------------------------------------------------------------
    def __str__(self) :
        fstring = "{0},{1},{2},{3},{4},{5}"
        return fstring.format(self.StatKey, self.CurrentStep, self.Person, self.TripID, self.SourceNode, self.DestinationNode)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class TripBegStatsEvent(TripStatsEvent) :
    # -----------------------------------------------------------------
    def __init__(self, timestep, person, tripid, snode, dnode) :
        TripStatsEvent.__init__(self, timestep, 'tripbeg', person, tripid, snode, dnode)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class TripEndStatsEvent(TripStatsEvent) :
    # -----------------------------------------------------------------
    def __init__(self, timestep, person, tripid, snode, dnode) :
        TripStatsEvent.__init__(self, timestep, 'tripend', person, tripid, snode, dnode)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class SumoConnectorStatsEvent(StatsEvent) :
    # -----------------------------------------------------------------
    def __init__(self, timestep, clockskew = 0.0, vehiclecount = 0) :
        StatsEvent.__init__(self, timestep, 'sumoconnector')

        self.ClockSkew = clockskew
        self.VehicleCount = vehiclecount

    # -----------------------------------------------------------------
    def __str__(self) :
        fstring = "{0},{1},{2:.3f},{3}"
        return fstring.format(self.StatKey, self.CurrentStep, self.ClockSkew, self.VehicleCount)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class OpenSimConnectorStatsEvent(StatsEvent) :
    # -----------------------------------------------------------------
    def __init__(self, timestep, clockskew = 0.0) :
        StatsEvent.__init__(self, timestep, 'osconnector')

        self.ClockSkew = clockskew

    # -----------------------------------------------------------------
    def __str__(self) :
        fstring = "{0},{1},{2:.3f}"
        return fstring.format(self.StatKey, self.CurrentStep, self.ClockSkew)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class TimerEvent :
    # -----------------------------------------------------------------
    def __init__(self, currentStep, currentTime) :
        self.CurrentStep = currentStep
        self.CurrentTime = currentTime

    # -----------------------------------------------------------------
    def __str__(self) :
        fstring = "CurrentStep:{0}"
        return fstring.format(self.CurrentStep)

    # -----------------------------------------------------------------
    def Dump(self) :
        fstring = "<{0},{1}>"
        return fstring.format(self.__class__.__name__,str(self))

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class ObjectEvent :
    # -----------------------------------------------------------------
    def __init__(self, identity) :
        self.ObjectIdentity = identity

    # -----------------------------------------------------------------
    def __str__(self) :
        fstring = "Identity:{0}"
        return fstring.format(self.ObjectIdentity)
    # -----------------------------------------------------------------
    def Dump(self) :
        fstring = "<{0},{1}>"
        return fstring.format(self.__class__.__name__,str(self))

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EventCreateObject(ObjectEvent) :
    # -----------------------------------------------------------------
    def __init__(self, identity, objtype, pos) :
        ObjectEvent.__init__(self, identity)
        self.ObjectType = objtype
        # Need position to determine on which simulator should the object be created
        self.ObjectPosition = pos

    # -----------------------------------------------------------------
    def __str__(self) :
        pstring = super(EventCreateObject,self).__str__()
        fstring = "{0},ObjectType:{1}"
        return fstring.format(pstring,self.ObjectType)


## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EventAddVehicle(ObjectEvent) :
    # -----------------------------------------------------------------
    def __init__(self, identity, objtype, route, target) :
        ObjectEvent.__init__(self, identity)
        self.ObjectType = objtype
        self.Route = route
        self.Target = target

    # -----------------------------------------------------------------
    def __str__(self) :
        #pstring = super(EventCreateVehicle,self).__str__()
        #fstring = "{0},Route:{1}"
        #return fstring.format(pstring,self.Route)

        pstring = "Identity:%s,Type:%s,Route:%s" % (self.ObjectIdentity, self.ObjectType, self.Route)
        return pstring


## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EventDeleteObject(ObjectEvent) :
    # -----------------------------------------------------------------
    def __init__(self, identity) :
        ObjectEvent.__init__(self, identity)

    # -----------------------------------------------------------------
    def __str__(self) :
        pstring = super(EventDeleteObject,self).__str__()
        return pstring

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EventPropertyChange(ObjectEvent) :
    # -----------------------------------------------------------------
    def __init__(self, identity, propkey, propval) :
        ObjectEvent.__init__(self, identity)
        self.ObjectProperty = propkey
        self.ObjectValue = propval

    # -----------------------------------------------------------------
    def __str__(self) :
        pstring = super(EventPropertyChange,self).__str__()
        fstring = "{0},{1}:{2}"
        return fstring.format(pstring,self.ObjectProperty,self.ObjectValue)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EventObjectDynamics(ObjectEvent) :

    # -----------------------------------------------------------------
    def __init__(self, identity, position, rotation, velocity) :
        ObjectEvent.__init__(self, identity)
        self.ObjectPosition = position
        self.ObjectRotation = rotation
        self.ObjectVelocity = velocity

    # -----------------------------------------------------------------
    def __str__(self) :
        pstring = super(EventObjectDynamics,self).__str__()
        fstring = "{0},x:{1},y:{2},z:{3}"
        return fstring.format(pstring,self.ObjectPosition.x,self.ObjectPosition.y,self.ObjectPosition.z)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EventInductionLoop(ObjectEvent) :
    # -----------------------------------------------------------------
    def __init__(self, identity, count) :
        ObjectEvent.__init__(self, identity)
        self.VehicleCount = count

    # -----------------------------------------------------------------
    def __str__(self) :
        pstring = super(EventInductionLoop,self).__str__()
        fstring = "{0},VehicleCount:{1}"
        return fstring.format(pstring,self.VehicleCount)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EventTrafficLightStateChange(ObjectEvent) :
    # -----------------------------------------------------------------
    def __init__(self, identity, state) :
        ObjectEvent.__init__(self, identity)
        self.StopLightState = state

    # -----------------------------------------------------------------
    def __str__(self) :
        pstring = super(EventTrafficLightStateChange,self).__str__()
        fstring = "{0},state:{2}"
        return fstring.format(pstring,self.StopLightState)
