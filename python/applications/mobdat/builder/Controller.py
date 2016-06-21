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

import os, sys, traceback
import logging
import cProfile

import json
from applications.mobdat.common import LayoutSettings
from applications.mobdat.builder import WorldBuilder, OpenSimBuilder, SumoBuilder,\
    DataBuilder, PersonPicker

global world
global laysettings

logger = logging.getLogger(__name__)
world = {}
laysettings = {}

# -----------------------------------------------------------------
# -----------------------------------------------------------------
def Controller(settings, pushlist) :
    global world
    global laysettings
    """
    Controller is the main entry point for driving the network building process.

    Arguments:
    settings -- nested dictionary with variables for configuring the connectors
    """
    rewrite_worldinfo = False
    laysettings = LayoutSettings.LayoutSettings(settings)

    """
    Phase 1: Load or create the world.
    """
    loadfile = settings["General"].get("WorldInfoFile","info.js")
    partial_save = settings["Builder"].get("PartialSave","partial.js")

    if os.path.isfile(loadfile):
        logger.warn('Loading world info from js file. This may take a few minutes...')
        try:
            world = WorldBuilder.WorldBuilder.LoadFromFile(loadfile)
        except:
            logger.error("Could not load world info from file " + loadfile)
    else:
        try:
            logger.warn('Loading partial world info from js file. This may take a few minutes...')
            world = WorldBuilder.WorldBuilder.LoadFromFile(partial_save)
        except (ValueError, IOError):
            logger.warn('could not find partial save file, starting new world.')
            world = WorldBuilder.WorldBuilder()
            world.step = []
        except:
            raise


    """
    Phase 2: World is created. Run the extension files
    """
    dbbindings = {"laysettings" : laysettings, "world" : world}

    for cf in settings["Builder"].get("ExtensionFiles",[]) :
        try :
            #cProfile.runctx('execfile(cf,dbbindings)',{'cf':cf,'dbbindings':dbbindings},{})
            if partial_save:
                if cf not in world.step:
                    execfile(cf,globals())
                    world.step.append(cf)
                    with open(partial_save, "w") as fp:
                        json.dump(world.Dump(),fp,ensure_ascii=True)
                    logger.info("saved partial world for {0}".format(cf))
            else:
                execfile(cf, dbbindings)
            rewrite_worldinfo = True
            logger.info('loaded extension file %s', cf)
        except :
            logger.warn('unhandled error processing extension file %s\n%s', cf, traceback.format_exc(10))
            sys.exit(-1)

    if rewrite_worldinfo:
        # if worldinfo already exists, make another one
        if os.path.isfile(loadfile):
            i = 0
            tmpfile = '{0}_{1}'.format(loadfile,i)
            while os.path.isfile(tmpfile):
                tmpfile = '{0}_{1}'.format(loadfile,i)
                i+=1
            loadfile = tmpfile

        logger.info('saving world data to %s',loadfile)

        with open(loadfile, "w") as fp :
            # json.dump(world.Dump(), fp, indent=2, ensure_ascii=True)
            json.dump(world.Dump(), fp, ensure_ascii=True)

        if partial_save and os.path.isfile(partial_save):
            os.remove(partial_save)

    for push in pushlist :
        if push == 'opensim' :
            logger.info("building opensim")
            osb = OpenSimBuilder.OpenSimBuilder(settings, world, laysettings)
            osb.PushNetworkToOpenSim()
        elif push == 'sumo' :
            logger.info("building sumo")
            scb = SumoBuilder.SumoBuilder(settings, world, laysettings)
            scb.PushNetworkToSumo()
        elif push == 'data' :
            logger.info('building data for CADIS')
            db = DataBuilder.DataBuilder(settings,world,laysettings)
            db.PushNetworkToFile()
        elif push == "personpicker":
            logger.info('building traveler data for experiments')
            PersonPicker.PersonPicker(settings,world,laysettings)
