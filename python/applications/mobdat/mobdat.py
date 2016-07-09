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

@file    mobdat
@author  Mic Bowman
@date    2013-12-03

This is the main script for running the mobdat mobile data simulator.

"""
from __future__ import absolute_import

import sys, os
import logging, logging.handlers, warnings

sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))

from common.util import get_os

if get_os().startswith('Windows'):
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


import time, json, argparse
from applications.mobdat.simulator import Controller

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
def ParseCommandLine(config, args) :
    parser = argparse.ArgumentParser()

    parser.add_argument('--capability', help='capability uuid')
    parser.add_argument('--scene', help='name of the scene in which the operation is performed')
    parser.add_argument('--endpoint', help='URL of the simulator dispatcher')
    parser.add_argument('--asendpoint', help='UDP endpoint of the dispatcher')

    parser.add_argument("--interval", help="sumo interval time", type=float)
    parser.add_argument("--connectors", help="list of connectors to use in the simulation", nargs='+')

    parser.add_argument("--starttime", help="hour of the day when simulation starts", type=float)
    parser.add_argument("--secondsperstep", help="number of seconds per simulation step", type=float)

    parser.add_argument("--steps", help="number of steps to execute", type=int)

    parser.add_argument("--travelers", help="maximum number of travelers to generate", type=int)

    options = parser.parse_args(args)

    if options.starttime :
        config["General"]["StartTimeOfDay"] = options.starttime

    if options.secondsperstep :
        config["General"]["SecondsPerStep"] = options.secondsperstep

    if options.capability :
        config["OpenSimConnector"]["Capability"] = options.capability

    if options.scene :
        config["OpenSimConnector"]["Scene"] = options.scene

    if options.endpoint :
        config["OpenSimConnector"]["EndPoint"] = options.endpoint

    if options.asendpoint :
        config["OpenSimConnector"]["AsyncEndPoint"] = options.asendpoint

    if options.steps :
        config['General']['TimeSteps'] = options.steps

    if options.travelers :
        config['General']['MaximumTravelers'] = options.travelers

    if options.interval :
        config["General"]["Interval"] = options.interval

    if options.connectors :
        config["General"]["Connectors"] = options.connectors

# -----------------------------------------------------------------
# -----------------------------------------------------------------
# def HandleWarnings(message, category, filename, lineno, file=None) :
#     mformat = 'WARNING: {0}\n'
#     sys.stderr.write(mformat.format(message))

# -----------------------------------------------------------------
# -----------------------------------------------------------------
def SetupLoggers() :
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logfile = filename=os.path.join(os.path.dirname(__file__), "logs/mobdat.log")

    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except:
            print "could not create directory for logs, quitting"
            raise

    flog = logging.handlers.RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=50, mode='w')
    flog.setLevel(logging.WARN)
    flog.setFormatter(logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
    logger.addHandler(flog)

    clog = logging.StreamHandler()
    #clog.addFilter(logging.Filter(name='mobdat'))
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    clog.setLevel(logging.DEBUG)
    logger.addHandler(clog)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


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

    ParseCommandLine(settings,remainder)

    # retrieving required capabilities from scenes
    """
    for sname,sim in settings["OpenSimConnector"]["Scenes"].items():
        endpoint = sim["EndPoint"] if "EndPoint" in sim else \
            exit('No endpoint defined for Scene {0}'.format(sname))
        avname = sim["AvatarName"] if "AvatarName" in sim else \
            exit('No avatar name specified to login in Scene {0}'.format(sname))
        domain = sim["Domain"] if "Domain" in sim else ['Dispatcher', 'RemoteControl']
        lifespan = sim["LifeSpan"] if "LifeSpan" in sim else 3600
        passwd = sim["Password"] if "Password" in sim else None

        rc = OpenSimRemoteControl.OpenSimRemoteControl(endpoint)

        if not passwd :
            print "Please type the password for User {0} in Scene {1}".format(avname,sname)
            passwd = getpass.getpass()
        settings["OpenSimConnector"]["Scenes"][sname] = {}
        AuthByUserName(rc, avname, passwd, domain, lifespan, \
                       settings["OpenSimConnector"]["Scenes"][sname])
    """
    Controller.Controller(settings)

if __name__ == '__main__':
    Main()
