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

@file    StatsConnector.py
@author  Mic Bowman
@date    2013-12-03

Picks up stats events and canonicalizes processing

"""

import os, sys
import logging

import re
import BaseConnector, EventHandler, EventTypes

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class StatsConnector(EventHandler.EventHandler, BaseConnector.BaseConnector) :

    # -----------------------------------------------------------------
    def __init__(self, evrouter, settings, world, netsettings, cname) :
        """Initialize the StatsConnector

        Keyword arguments:
        evhandler -- the initialized event handler, EventRegistry type
        settings -- dictionary of settings from the configuration file
        """

        EventHandler.EventHandler.__init__(self, evrouter)
        BaseConnector.BaseConnector.__init__(self, settings, world, netsettings)

        self.Logger = logging.getLogger(__name__)

        self.CollectObjectDynamics = settings["StatsConnector"].get("CollectObjectDynamics", False)
        self.CollectObjectPattern =  settings["StatsConnector"].get("CollectObjectPattern", ".*")
        self.CollectObjectRE = re.compile(self.CollectObjectPattern, re.IGNORECASE)

        self.CurrentStep = 0
        self.CurrentTime = 0

    # -----------------------------------------------------------------
    def HandleVehicle(self,event) :
        self.Logger.info(str(event))

    # -----------------------------------------------------------------
    def HandleStatsEvent(self, event) :
        if event.__class__ == EventTypes.SumoConnectorStatsEvent :
            vcount = event.VehicleCount
            scount = event.CurrentStep
            timeofday = self.GetWorldTimeOfDay(scount)
            self.Logger.warn("{0} vehicles in the simulation, steps={1}, time={2:.2f}".format(vcount, scount, timeofday))

        self.Logger.info(str(event))

    # -----------------------------------------------------------------
    def HandleTimerEvent(self, event) :
        self.CurrentStep = event.CurrentStep
        self.CurrentTime = event.CurrentTime

    # -----------------------------------------------------------------
    def HandleShutdownEvent(self, event) :
        pass

    # -----------------------------------------------------------------
    def HandleObjectDynamicsEvent(self, event) :
        if self.CollectObjectRE.match(event.ObjectIdentity) :
            name = event.ObjectIdentity
            x = event.ObjectPosition.x
            y = event.ObjectPosition.y
            self.Logger.info("dynamics, {0}, {1}, {2:.4f}, {3:.4f}".format(self.CurrentStep, name, x, y))

    # -----------------------------------------------------------------
    def SimulationStart(self) :
        # print "StatsConnector initialized"

        # Connect to the event registry
        # self.SubscribeEvent(EventTypes.StatsEvent, self.HandleStatsEvent)
        self.SubscribeEvent(EventTypes.SumoConnectorStatsEvent, self.HandleStatsEvent)
        self.SubscribeEvent(EventTypes.OpenSimConnectorStatsEvent, self.HandleStatsEvent)
        self.SubscribeEvent(EventTypes.TripBegStatsEvent, self.HandleStatsEvent)
        self.SubscribeEvent(EventTypes.TripEndStatsEvent, self.HandleStatsEvent)

        self.SubscribeEvent(EventTypes.TimerEvent, self.HandleTimerEvent)
        self.SubscribeEvent(EventTypes.ShutdownEvent, self.HandleShutdownEvent)

        if self.CollectObjectDynamics :
            self.SubscribeEvent(EventTypes.EventObjectDynamics, self.HandleObjectDynamicsEvent)

        # all set... time to get to work!
        self.HandleEvents()
