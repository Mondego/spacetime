'''
Created on Dec 17, 2015

@author: Arthur Valadares
'''

import logging
from datamodel.akshatp.datamodel import Car_akshatp, InactiveCar_akshatp, ActiveCar_akshatp
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter

logger = logging.getLogger(__name__)
LOG_HEADER = "[TRAFFIC]"


@Producer(Car_akshatp)
@GetterSetter(InactiveCar_akshatp, ActiveCar_akshatp)
class TrafficSimulation(IApplication):
    '''
    classdocs
    '''

    frame = None
    ticks = 0
    TICKS_BETWEEN_CARS = 10
    cars = []

    def __init__(self, frame):
        '''
        Constructor
        '''
        self.frame = frame

    def initialize(self):
        # logger.debug("%s Initializing", LOG_HEADER)
        for i in xrange(3):
            self.frame.add(Car_akshatp())
        # self.frame.add(Car("1d4883f3-b8f7-11e5-a78c-acbc327e1743")) # Valid uuid Example
        # self.frame.add(Car(10)) # Valid int Example
        # self.frame.add(Car("1d4883f3")) Invalid  Example - Crashes

        self.cars = self.frame.get(Car_akshatp)

    def update(self):
        # logger.info("%s Tick", LOG_HEADER)
        if self.ticks % self.TICKS_BETWEEN_CARS == 0:
            try:
                inactives = self.frame.get(InactiveCar_akshatp)
                # logger.debug("%s ************** InactiveCars: %s", LOG_HEADER, len(inactives))
                if inactives != None and len(inactives) > 0:
                    # logger.debug("%s ************** Starting car %s", LOG_HEADER, inactives[0].ID)
                    inactives[0].start();

            except Exception:
                logger.exception("Error: ")

        for car in self.frame.get(ActiveCar_akshatp):
            car.move()
        self.ticks += 1

    def shutdown(self):
        pass
