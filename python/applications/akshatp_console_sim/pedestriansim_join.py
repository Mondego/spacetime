'''
Created on Jan 11, 2015

@author: Ian Carvalho
'''

import logging
from datamodel.akshatp.datamodel import Pedestrian_akshatp, StoppedPedestrian_akshatp, Walker_akshatp, Car_akshatp, CarAndPedestrianNearby_akshatp, \
    PedestrianHasAvodiedCollision_akshatp
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Setter

logger = logging.getLogger(__name__)
LOG_HEADER = "[PEDESTRIANS]"


@Producer(Pedestrian_akshatp)
@Setter(Car_akshatp, Pedestrian_akshatp)
@GetterSetter(StoppedPedestrian_akshatp, Walker_akshatp, CarAndPedestrianNearby_akshatp, PedestrianHasAvodiedCollision_akshatp)
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
            self.frame.add(Pedestrian_akshatp())
        self.pedestrians = self.frame.get(Pedestrian_akshatp)

    def update(self):
        # logger.info("%s Tick", LOG_HEADER)
        if self.ticks % self.TICKS_BETWEEN_PEDESTRIANS == 0:
            try:
                inactives = self.frame.get(StoppedPedestrian_akshatp)
                # logger.debug("%s ************** StoppedPedestrian: %s", LOG_HEADER, len(inactives))
                if inactives != None and len(inactives) > 0:
                    # logger.debug("%s ************** Moving Pedestrian %s", LOG_HEADER, inactives[0].ID)
                    inactives[0].move();

            except Exception:
                logger.exception("Error: ")

        for car_ped in self.frame.get(CarAndPedestrianNearby_akshatp):
            car_ped.move()
        for pedestrian in self.frame.get(PedestrianHasAvodiedCollision_akshatp):
            pedestrian.move()
        for pedestrian in self.frame.get(Walker_akshatp):
            pedestrian.move()
        self.ticks += 1

    def shutdown(self):
        pass
