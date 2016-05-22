#!/usr/bin/python
'''
Created on Dec 17, 2015

@author: Arthur Valadares
'''

import logging
import logging.handlers
import os
import sys
import argparse

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))
from applications.ospuller.ospuller import OpenSimPuller
from applications.ospuller.testsim import TestSimulation

from applications.nodesim.nodesim import NodeSimulation
from applications.nodesim.testsim import NodeTestSimulation
from spacetime_local.frame import frame

class Simulation(object):
    '''
    classdocs
    '''

    def __init__(self, args):
        '''
        Constructor
        '''
        frameos = frame(time_step=200)
        frameos.attach_app(OpenSimPuller(frameos, args))

        frameos.run_async()

        frame.loop()

def setupLoggers():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    clog = logging.StreamHandler()
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    clog.setLevel(logging.DEBUG)
    logger.addHandler(clog)

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

if __name__ == "__main__":
    setupLoggers()
    parser = argparse.ArgumentParser()
    parser.add_argument('-url', '--url', type=str, default='http://127.0.0.1:9000', help='URL of OpenSim server')
    parser.add_argument('-u', '--user', type=str, default='Test User', help='Account for OpenSim server')
    parser.add_argument('-p', '--password', type=str, default='123', help='Password for OpenSim server')
    parser.add_argument('-s', '--scene', type=str, default='Test 1', help='Scene name for OpenSim server')
    parser.add_argument('-f', '--fetch', action='store_true', help='Fetch asset IDs from DB')
    parser.add_argument('-dbh', '--dbhost', type=str, default='127.0.0.1', help='Host of MySQL server')
    parser.add_argument('-dbu', '--dbuser', type=str, default='opensim', help='Account for MySQL server')
    parser.add_argument('-dbp', '--dbpassword', type=str, default='opensim', help='Password for MySQL server')
    parser.add_argument('-dbs', '--dbschema', type=str, default='opensim', help='Database schema in MySQL server')
    args = parser.parse_args()

    sim = Simulation(args)
