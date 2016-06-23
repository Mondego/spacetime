#!/usr/bin/python
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

@file    netbuilder
@author  Mic Bowman
@date    2013-12-03

This is the main script for the netbuilder tool. It reads and executes a
network configuration program building the output configuration files
for the specified connectors (opensim, sumo or social)

"""

import os, sys
import logging, logging.handlers, warnings
import platform

sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))
if platform.system() == 'Windows' or platform.system().startswith("CYGWIN"):
    if os.environ.get("SUMO_WINDOWS"):
        sys.path.append(os.path.join(os.environ.get("SUMO_WINDOWS"), "tools"))
    else:
        print "set environment variable SUMO_WINDOWS to the SUMO directory"
        sys.exit(1)
else:
    if os.environ.get("SUMO_LINUX"):
        sys.path.append(os.path.join(os.environ.get("SUMO_LINUX"), "tools"))
    else:
        print "set environment variable SUMO_LINUX to the SUMO directory"
        sys.exit(1)
from applications.mobdat.builder import Controller

import time, argparse, json


logger = logging.getLogger()

# -----------------------------------------------------------------
# -----------------------------------------------------------------
def ParseConfigurationFiles(cfiles) :
    config = {}
    for cfile in cfiles :
        try :
            config.update(json.load(open(cfile)))
        except IOError as detail :
            warnings.warn("Error parsing configuration file %s; IO error %s" % (cfile, str(detail)))
            sys.exit(-1)
        except ValueError as detail :
            warnings.warn("Error parsing configuration file %s; value error %s" % (cfile, str(detail)))
            sys.exit(-1)
        except NameError as detail :
            warnings.warn("Error parsing configuration file %s; name error %s" % (cfile, str(detail)))
            sys.exit(-1)
        except :
            warnings.warn('Error parsing configuration file %s; %s' % (cfile, sys.exc_info()[0]))
            sys.exit(-1)

    return config

# -----------------------------------------------------------------
# -----------------------------------------------------------------
def ParseEnvironment(config) :
    capenv = os.environ.get('OS_REMOTECONTROL_CAP')
    if capenv :
        [capability, expires] = capenv.split(':',1)
        if expires and int(expires) > int(time.time()) :
            config["OpenSimConnector"]["Capability"] = capability

    scene = os.environ.get('OS_REMOTECONTROL_SCENE')
    if scene :
        config["OpenSimConnector"]["Scene"] = scene

    endpoint = os.environ.get('OS_REMOTECONTROL_URL')
    if endpoint :
        config["OpenSimConnector"]["EndPoint"] = endpoint

    asendpoint = os.environ.get('OS_REMOTECONTROL_UDP')
    if asendpoint :
        config["OpenSimConnector"]["AsyncEndPoint"] = asendpoint

# -----------------------------------------------------------------
# -----------------------------------------------------------------
def ParseCommandLine(config, args) :
    parser = argparse.ArgumentParser()

    parser.add_argument('--load', help='data file to load on startup')
    parser.add_argument('--extension', help='list of extension files to process', nargs="*")
    parser.add_argument('--noextension', help='do not process extension files', action='store_true')

    parser.add_argument('--capability', help='capability uuid')
    parser.add_argument('--scene', help='name of the scene in which the operation is performed')
    parser.add_argument('--endpoint', help='URL of the simulator dispatcher')
    parser.add_argument('--asendpoint', help='UDP endpoint of the dispatcher')

    parser.add_argument('pushlist', help='Connectors to apply to the network', nargs="*")

    options = parser.parse_args(args)

    if options.load :
        config["Builder"]["LoadFile"] = options.load

    if options.noextension :
        config["Builder"]["ExtensionFiles"] = []

    if options.extension :
        config["Builder"]["ExtensionFiles"] = options.extension

    if options.capability :
        config["OpenSimConnector"]["Capability"] = options.capability

    if options.scene :
        config["OpenSimConnector"]["Scene"] = options.scene

    if options.endpoint :
        config["OpenSimConnector"]["EndPoint"] = options.endpoint

    if options.asendpoint :
        config["OpenSimConnector"]["AsyncEndPoint"] = options.asendpoint

    return options.pushlist

# -----------------------------------------------------------------
# -----------------------------------------------------------------
def SetupLoggers() :
    global logger
    logger.setLevel(logging.DEBUG)

    #logfile = filename=os.path.join(os.path.dirname(__file__), "../logs/builder.log")
    logfile = filename=os.path.join(os.path.dirname(__file__), "logs/builder.log")

    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except:
            print "could not create directory for logs, quitting"
            raise
    flog = logging.FileHandler(logfile, mode='w')
    flog.setFormatter(logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
    logger.addHandler(flog)

    clog = logging.StreamHandler()
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    clog.setLevel(logging.INFO)
    logger.addHandler(clog)

# -----------------------------------------------------------------
# -----------------------------------------------------------------
def Main() :
    SetupLoggers()

    # parse out the configuration file first
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='configuration file for simulation settings', default=['settings.js'], nargs = '+')
    (options, remainder) = parser.parse_known_args()

    settings = ParseConfigurationFiles(options.config)
    #ParseEnvironment(settings)
    pushlist = ParseCommandLine(settings,remainder)

    Controller.Controller(settings, pushlist)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

# -----------------------------------------------------------------
# -----------------------------------------------------------------
if __name__ == '__main__':
    Main()
