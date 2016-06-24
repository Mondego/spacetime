#!/usr/bin/python
'''
Created on Dec 17, 2015

@author: Arthur Valadares
'''

import logging
import logging.handlers
import os
import sys


sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))

from common.instrument import SpacetimeInstruments as si
from applications.nodesim.nodesim import NodeSimulation
from applications.nodesim.testsim import NodeTestSimulation
from spacetime_local.frame import frame

class Simulation(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        framenode = frame(time_step=200, instrument=True)
        framenode.attach_app(NodeSimulation(framenode))

        frametest = frame(time_step=200, instrument=True)
        frametest.attach_app(NodeTestSimulation(frametest))

        si.setup_instruments(frame.framelist)

        framenode.run_async()
        frametest.run_async()

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
    sim = Simulation()
