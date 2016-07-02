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

@file    Controller.py
@author  Mic Bowman
@date    2013-12-03

This module defines routines for controling the mobdat simulator. The controller
sets up the connectors and then drives the simulation through the periodic
clock ticks.

"""
from __future__ import absolute_import

import logging
from multiprocessing import Process, Manager
import os, sys
import platform, time, threading, cmd

from applications.mobdat.simulator import EventRouter, EventTypes
from applications.mobdat.simulator import SumoConnector, OpenSimConnector, SocialConnector, StatsConnector
from spacetime_local.frame import frame

from applications.mobdat.common import LayoutSettings, WorldInfo
from applications.mobdat.common.Utilities import AuthByUserName
from applications.mobdat.prime import PrimeSimulator
from common.instrument import SpacetimeInstruments as si
import datetime
from threading import Thread


# -----------------------------------------------------------------
# -----------------------------------------------------------------

_SimulationControllers = {
    'sumo' : SumoConnector.SumoConnector,
    'social' : SocialConnector.SocialConnector,
    # NOT PORTED TO SPACETIME 'stats' : StatsConnector.StatsConnector,
    'opensim' : OpenSimConnector.OpenSimConnector,
    'prime' : PrimeSimulator.PrimeSimulator
    }

logger = logging.getLogger(__name__)

def Controller(settings) :
    """
    Controller is the main entry point for driving the simulation.

    Arguments:
    settings -- nested dictionary with variables for configuring the connectors
    """

    laysettings = LayoutSettings.LayoutSettings(settings)
    # laysettings = None
    # load the world
    infofile = settings["General"].get("WorldInfoFile", "info.js")
    logger.info('loading world data from %s', infofile)
    world = WorldInfo.WorldInfo.LoadFromFile(infofile)
    # world = None

    cnames = settings["General"].get("Connectors", ['sumo', 'opensim', 'social', 'stats'])
    instrument = settings["General"].get("Instrument", False)
    profiling = settings["General"].get("Profiling", False)
    store_type = settings["General"].get("Store", "SimpleStore")
    process = settings["General"].get("MultiProcessing", False)
    timer = settings["General"].get("Timer", None)
    autostart = settings["General"].get("AutoStart", False)
    if timer:
        seconds = 0
        minutes = 0
        hours = 0
        if "Seconds" in timer:
            seconds = timer["Seconds"]
        if "Minutes" in timer:
            minutes = timer["Minutes"]
        if "Hours" in timer:
            hours = timer["Hours"]
        timer = datetime.timedelta(seconds=seconds, minutes=minutes, hours=hours)

    connectors = []
    for cname in cnames :
        if cname not in _SimulationControllers :
            logger.warn('skipping unknown simulation connector; %s' % (cname))
            continue

        cframe = frame(time_step=200, instrument=instrument, profiling=profiling)
        connector = _SimulationControllers[cname](settings, world, laysettings, cname, cframe)
        cframe.attach_app(connector)
        connectors.append(cframe)

    if instrument:
        si.setup_instruments(connectors)

    for f in connectors:
        f.run_async()

    frame.loop()
    print "closing down controller"
    sys.exit(0)
