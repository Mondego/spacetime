'''
Created on May 17, 2016

@author: Arthur Valadares
'''
import logging

from datamodel.common.datamodel import Vehicle, Vector3
from spacetime_local import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Tracker, \
    Deleter
from random import choice
import math
import numpy as np


@Producer(Vehicle)
@GetterSetter(Vehicle)
@Deleter(Vehicle)
class OpenSimPullerTestSimulation(IApplication.IApplication):
    def __init__(self, frame):
        '''
        Constructor
        '''
        self.frame = frame
        self.step = 0
        self.logger = logging.getLogger(__name__)
        self.timestep = self.frame.get_timestep()
        self.rotating_degree = 2
        self.rotation = math.radians(self.rotating_degree)
        self.todelete = []

    def initialize(self):
        pass

    def update(self):
        cars = self.frame.get(Vehicle)
        for c in cars:
            if self.step % 5:
                px = (c.Velocity.X * math.cos(self.rotation) -
                    c.Velocity.Y * math.sin(self.rotation))
                py = (c.Velocity.X  * math.sin(self.rotation) +
                    c.Velocity.Y  * math.cos(self.rotation))
                c.Velocity = Vector3(px, py, c.Velocity.Z)
            c.Position += c.Velocity * self.timestep

        if self.step % 40 == 0:
            # First route: from a random place to another random place
            v = Vehicle()
            v.Position = Vector3(500,500,25.5)
            v.Velocity = Vector3(10,10,0)
            v.Name = "[testcar] Car %s" % self.step
            v.Lenght = 5
            v.Width = 2
            self.frame.add(v)
            self.todelete.append(v.ID)

        if len(self.todelete) > 360 / self.rotating_degree:
            car = self.frame.get_one(Vehicle, self.todelete[0])
            self.frame.delete(Vehicle, car)
            del self.todelete[0]

        self.step += 1


    def shutdown(self):
        self.logger.info("Shutting down OpenSimPullerTestSimulation")
