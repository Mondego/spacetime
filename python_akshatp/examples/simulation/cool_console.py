#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on Apr 23, 2016

@author: Akshat Patel
'''

import logging
import os
from datamodel.carpedestrian.datamodel import Walker, ActiveCar, PedestrianHasAvodiedCollision,\
    CarAndPedestrianNearby, InactiveCar, StoppedPedestrian
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Getter

logger = logging.getLogger(__name__)
LOG_HEADER = "[CON]"


@Getter(ActiveCar, Walker, PedestrianHasAvodiedCollision, CarAndPedestrianNearby, InactiveCar, StoppedPedestrian)
class ConsoleSimulation(IApplication):
    '''
    classdocs
    '''

    frame = None
    ticks = 0

    def is_done(self):
        return self.start_shutdown

    def __init__(self, frame):
        '''
        Constructor
        '''
        self.frame = frame
        self.start_shutdown = False

        self.width = 30
        self.height = 5



    def initialize(self):
        logger.debug("%s Initializing", LOG_HEADER)

    def update(self):
        os.system('clear')
        self.simgrid = [['_' for _ in range(self.width)] for _ in range(self.height)]

        for car in self.frame.get(InactiveCar):
            logger.debug("[InactiveCar] {0} Y:{1} X:{2} ".format(car.ID, car.Position.Y, car.Position.X))

        for car in self.frame.get(ActiveCar):
            logger.debug("[ActiveCar] {0} Y:{1} X:{2} ".format(car.ID, car.Position.Y, car.Position.X))
            if 0 <= car.Position.X < self.width:
                if car.Color == 0:
                    self.simgrid[car.Position.Y][car.Position.X] = '🚗'
                elif car.Color == 1:
                    self.simgrid[car.Position.Y][car.Position.X] = '🚕'
                elif car.Color == 2:
                    self.simgrid[car.Position.Y][car.Position.X] = '🚙'
                elif car.Color == 3:
                    self.simgrid[car.Position.Y][car.Position.X] = '🚌'
                elif car.Color == 4:
                    self.simgrid[car.Position.Y][car.Position.X] = '🚚'
                else:
                    self.simgrid[car.Position.Y][car.Position.X] = '🚛'

        for ped in self.frame.get(Walker):
            logger.debug("[Walker] {0} Y:{1} X:{2} ".format(ped.ID, ped.Y, ped.X))
            self.simgrid[ped.Y][ped.X] = '🙂'

        for ped in self.frame.get(PedestrianHasAvodiedCollision):
            logger.debug("[PedestrianHasAvoidedCollision] {0} Y:{1} X:{2} ".format(ped.ID, ped.Y, ped.X))
            self.simgrid[ped.Y][ped.X] = '😡'

        for cptuple in self.frame.get(CarAndPedestrianNearby):
            # logger.debug("[CarAndPedestrianNearby] {0} Y:{1} X:{2} ".format(cptuple.pedestrian.ID, cptuple.pedestrian.Y,
            #                                                                 cptuple.pedestrian.X))
            self.simgrid[cptuple.pedestrian.Y][cptuple.pedestrian.X] = '😱'

        for ped in self.frame.get(StoppedPedestrian):
            logger.debug("[StoppedPedestrian] {0} Y:{1} X:{2} ".format(ped.ID, ped.Y, ped.X))


        for x in range(self.height):
            line = ''
            for y in range(self.width):
                line += self.simgrid[x][y]
            line += '\n'
            print(line)

    def shutdown(self):
        pass
