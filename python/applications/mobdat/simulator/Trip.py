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

@file    Trip.py
@author  Mic Bowman
@date    2014-04-02

This module defines the SocialConnector class. This class implements
the social (people) aspects of the mobdat simulation.

"""

import os, sys
import logging

from applications.mobdat.common.Utilities import GenName

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class Trip :
    """
    Trip -- A class to capture the state and necessary callbacks for
    a trip.
    """

    # -----------------------------------------------------------------
    def __init__(self, traveler, stime, source, destination) :
        """
        Args:
            traveler -- object of type Traveler
            stime -- float, world time at the start of the trip
            source -- LayoutNodes.LocationCapsule
            destination -- LayoutNodes.LocationCapsule
        """
        self.Traveler = traveler

        self.ScheduledStartTime = stime
        self.ActualStartTime = 0

        self.Source = source
        self.Destination = destination

        # the vehicle name should come from the person, however Sumo
        # does not generate create events when a vehicle name is reused
        self.TripID = GenName(self.Traveler.Person.Name + '_trip')
        self.VehicleName = self.TripID
        self.VehicleType = self.Traveler.Person.Vehicle.VehicleType

    # -----------------------------------------------------------------
    def TripCompleted(self, connector) :
        """
        TripCompleted -- event handler called when the connector completes the trip

        Args:
            connector -- object of type SocialConnector
        """

        connector.GenerateTripEndEvent(self)

        self.Traveler.TripCompleted(self)

    # -----------------------------------------------------------------
    def TripStarted(self, connector) :
        """
        TripStarted -- event handler called when the connector is ready to schedule
        the start of the trip, calls into the traveler handler and also generates
        the appropriate global events.

        Args:
            connector -- object of type SocialConnector
        """
        self.ActualStartTime = connector.WorldTime
        self.Traveler.TripStarted(self)

        connector.GenerateTripBegEvent(self)
        connector.GenerateAddVehicleEvent(self)

    # -----------------------------------------------------------------
    def __cmp__(self, other) :
        return cmp(self.ScheduledStartTime, other.ScheduledStartTime)

