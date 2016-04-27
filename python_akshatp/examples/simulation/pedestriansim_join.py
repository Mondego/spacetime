'''
Created on Jan 11, 2015

@author: Ian Carvalho
'''

import logging
from datamodel.carpedestrian.datamodel import Pedestrian, StoppedPedestrian, Walker, Car, CarAndPedestrianNearby, \
    PedestrianHasAvodiedCollision
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Setter

logger = logging.getLogger(__name__)
LOG_HEADER = "[PEDESTRIANS]"


@Producer(Pedestrian)
@Setter(Car, Pedestrian)
@GetterSetter(StoppedPedestrian, Walker, CarAndPedestrianNearby, PedestrianHasAvodiedCollision)
class PedestrianSimulation(IApplication):
    '''
    classdocs
    '''

    frame = None
    ticks = 0
    TICKS_BETWEEN_PEDESTRIANS = 10
    pedestrians = []

    def __init__(self, frame):
        '''
        Constructor
        '''
        self.frame = frame

    def initialize(self):
        # logger.debug("%s Initializing", LOG_HEADER)
        for i in xrange(5):
            self.frame.add(Pedestrian())
        self.pedestrians = self.frame.get(Pedestrian)

    def update(self):
        # logger.info("%s Tick", LOG_HEADER)
        if self.ticks % self.TICKS_BETWEEN_PEDESTRIANS == 0:
            try:
                inactives = self.frame.get(StoppedPedestrian)
                # logger.debug("%s ************** StoppedPedestrian: %s", LOG_HEADER, len(inactives))
                if inactives != None and len(inactives) > 0:
                    # logger.debug("%s ************** Moving Pedestrian %s", LOG_HEADER, inactives[0].ID)
                    inactives[0].move();

            except Exception:
                logger.exception("Error: ")

        for car_ped in self.frame.get(CarAndPedestrianNearby):
            car_ped.move()
        for pedestrian in self.frame.get(PedestrianHasAvodiedCollision):
            pedestrian.move()
        for pedestrian in self.frame.get(Walker):
            pedestrian.move()
        self.ticks += 1

    def shutdown(self):
        pass
