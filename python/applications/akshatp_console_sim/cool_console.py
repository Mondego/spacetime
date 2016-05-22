#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on Apr 23, 2016

@author: Akshat Patel
'''

import logging
import os
from datamodel.akshatp.datamodel import Walker_akshatp, ActiveCar_akshatp, PedestrianHasAvodiedCollision_akshatp,\
    CarAndPedestrianNearby_akshatp, InactiveCar_akshatp, StoppedPedestrian_akshatp
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Getter

logger = logging.getLogger(__name__)
LOG_HEADER = "[CON]"


@Getter(ActiveCar_akshatp, Walker_akshatp, PedestrianHasAvodiedCollision_akshatp, CarAndPedestrianNearby_akshatp, InactiveCar_akshatp, StoppedPedestrian_akshatp)
class ConsoleSimulation(IApplication):
    '''
    classdocs
    '''

    frame = None
    ticks = 0

    @property
    def done(self):
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

        for car in self.frame.get(InactiveCar_akshatp):
            logger.debug("[InactiveCar_akshatp] {0} Y:{1} X:{2} ".format(car.ID, car.Position.Y, car.Position.X))

        for car in self.frame.get(ActiveCar_akshatp):
            logger.debug("[ActiveCar_akshatp] {0} Y:{1} X:{2} ".format(car.ID, car.Position.Y, car.Position.X))
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

        for ped in self.frame.get(Walker_akshatp):
            logger.debug("[Walker_akshatp] {0} Y:{1} X:{2} ".format(ped.ID, ped.Y, ped.X))
            self.simgrid[ped.Y][ped.X] = '🙂'

        for ped in self.frame.get(PedestrianHasAvodiedCollision_akshatp):
            logger.debug("[PedestrianHasAvodiedCollision_akshatp] {0} Y:{1} X:{2} ".format(ped.ID, ped.Y, ped.X))
            self.simgrid[ped.Y][ped.X] = '😡'

        for cptuple in self.frame.get(CarAndPedestrianNearby_akshatp):
            # logger.debug("[CarAndPedestrianNearby_akshatp] {0} Y:{1} X:{2} ".format(cptuple.pedestrian.ID, cptuple.pedestrian.Y,
            #                                                                 cptuple.pedestrian.X))
            self.simgrid[cptuple.pedestrian.Y][cptuple.pedestrian.X] = '😱'

        for ped in self.frame.get(StoppedPedestrian_akshatp):
            logger.debug("[StoppedPedestrian_akshatp] {0} Y:{1} X:{2} ".format(ped.ID, ped.Y, ped.X))


        for x in range(self.height):
            line = ''
            for y in range(self.width):
                line += self.simgrid[x][y]
            line += '\n'
            print(line)

    def shutdown(self):
        pass
