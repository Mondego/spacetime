'''
Created on Aug 24, 2016

@author: arthurvaladares
'''
import logging

from datamodel.benchmark.datamodel import *
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Tracker, \
    Deleter, Setter, Getter
from common.instrument import timethis
global BM_PRODUCER
global BM_SETTER
global BM_GETTER
global BM_GETTERSETTER
global BM_TRACKER
global BM_DELETER

@Producer(*BM_PRODUCER)
@Getter(*BM_GETTER)
@Setter(*BM_SETTER)
@GetterSetter(*BM_GETTERSETTER)
@Tracker(*BM_TRACKER)
@Deleter(*BM_DELETER)
class BenchmarkSimulation(IApplication):
    def __init__(self, frame, instances, steps, init_hook, update_hook=None):
        self.frame = frame
        self.done = False
        self.curstep = 0
        self.instances = instances
        self.simsteps = steps
        self.update_hook = update_hook
        self.init_hook = init_hook
        self.logger = logging.getLogger(__name__)

    def initialize(self):
        self.init_hook(self)

    @timethis
    def update(self):
        if self.update_hook:
            self.update_hook(self)
        self.curstep += 1
        if self.curstep == self.simsteps:
            self.done = True


    def shutdown(self):
        self.logger.info("Shutting down benchmark")

    @property
    def done(self):
        return self._done

    @done.setter
    def done(self, value):
        self._done = value

