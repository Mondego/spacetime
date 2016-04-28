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
from trafficsim import TrafficSimulation
from pedestriansim_join import PedestrianSimulation
from cool_console import ConsoleSimulation

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
        frame_car = frame(time_step=1000)
        frame_car.attach_app(TrafficSimulation(frame_car))

        frame_ped = frame(time_step=1000)
        frame_ped.attach_app(PedestrianSimulation(frame_ped))

        con_frame = frame(time_step=500)
        con_frame.attach_app(ConsoleSimulation(con_frame))

        frame_car.run_async()
        frame_ped.run_async()
        con_frame.run()


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
