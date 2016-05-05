#!/usr/bin/python
'''
Created on Dec 17, 2015

@author: Arthur Valadares
'''

import logging
import logging.handlers
import os
import sys
from spacetime_local.frame import frame
from nodesim import NodeSimulation
from testsim import NodeTestSimulation

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))

logger = None


class Simulation(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        framenode = frame(time_step=200)
        framenode.attach_app(NodeSimulation(framenode))

        frametest = frame(time_step=200)
        frametest.attach_app(NodeTestSimulation(frametest))

        framenode.run_async()
        frametest.run()

def setupLoggers():
    global logger
    logger = logging.getLogger()
    logging.info("testing before")
    logger.setLevel(logging.DEBUG)

    # logfile = os.path.join(os.path.dirname(__file__), "../../logs/CADIS.log")
    # flog = logging.handlers.RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=50, mode='w')
    # flog.setFormatter(logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
    # logger.addHandler(flog)
    logging.getLogger("requests").setLevel(logging.WARNING)
    clog = logging.StreamHandler()
    clog.addFilter(logging.Filter(name='CADIS'))
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    clog.setLevel(logging.DEBUG)
    logger.addHandler(clog)


if __name__ == "__main__":
    setupLoggers()
    sim = Simulation()
