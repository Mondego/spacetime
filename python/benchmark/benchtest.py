'''
Created on Aug 24, 2016

@author: arthurvaladares
'''
import logging

from datamodel.benchmark.datamodel import *
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Tracker, \
    Deleter, Setter, Getter
from common.instrument import SpacetimeInstruments as si, timethis
global BT_PRODUCER
global BT_GETTER
global BT_SETTER
global BT_GETTERSETTER
global BT_TRACKER
global BT_DELETER

@Producer(*BT_PRODUCER)
@Getter(*BT_GETTER)
@Setter(*BT_SETTER)
@GetterSetter(*BT_GETTERSETTER)
@Tracker(*BT_TRACKER)
@Deleter(*BT_DELETER)
class BenchmarkTestSimulation(IApplication):
    def __init__(self, frame, event, init_hook=None, update_hook=None):
        self.event = event
        self.frame = frame
        self.curstep = 0
        self.init_hook = init_hook
        self.update_hook = update_hook
        self.running = True
        self.done = False
        self.logger = logging.getLogger(__name__)

    def initialize(self):
        if self.init_hook:
            self.init_hook(self)

    @timethis
    def update(self):
        if self.update_hook:
            self.update_hook(self)
        if self.event.is_set() == True:
            self.event.clear()
            self.done = True
        self.curstep += 1

    def shutdown(self):
        self.logger.info("Shutting down benchmark")

    @property
    def done(self):
        return self._done

    @done.setter
    def done(self, value):
        self._done = value