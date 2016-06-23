'''
Created on Jan 27, 2016

@author: Arthur Valadares
'''
import logging
import random
from random import randint
from datamodel.mobdat.datamodel import BusinessNode, MobdatVehicle, PrimeNode,\
    EmptyBusiness, ResidentialNode, SimulationNode
from spacetime_local.declarations import Producer, GetterSetter
from applications.mobdat.common.graph.SocialNodes import Person
from spacetime_local.IApplication import IApplication

@Producer(MobdatVehicle, BusinessNode, SimulationNode, PrimeNode)
@GetterSetter(MobdatVehicle, Person, BusinessNode, ResidentialNode, SimulationNode, PrimeNode, EmptyBusiness)
class PrimeSimulator(IApplication):
    def __init__(self, settings, world, netsettings, cname, frame) :
        self.frame = frame
        self.world = world
        self.__Logger = logging.getLogger(__name__)
        self.schedule_deliveries = {}
        pass

    def initialize(self):
        self.mybusiness = None
        self.__Logger.warn('PrimeSimulator initialization complete')

    def add_deliveries(self):
        ppl = self.frame.get(Person)
        if len(ppl) > 40:
            customers = random.sample(ppl, 10)
            # Synchronized attribute
            if not self.mybusiness.Customers:
                self.mybusiness.Customers = []
            self.mybusiness.Customers.append([p.Name for p in customers])
            if not hasattr(self, "CustomerObjects"):
                self.mybusiness.CustomerObjects = {}

            # Non-synchronized attribute
            for c in customers:
                self.mybusiness.CustomerObjects[c.Name] = c
                starttime = random.randint(self.CurrentStep, self.CurrentStep + 400)
                if starttime not in self.schedule_deliveries:
                    self.schedule_deliveries[starttime] = []
                self.schedule_deliveries[starttime].append(c)
            self.__Logger.info("Delivery schedule: %s", self.schedule_deliveries.keys())

    # @instrument TODO:
    def update(self):
        self.CurrentStep = self.frame.step
        if not self.mybusiness:
            a = self.frame.get(EmptyBusiness)
            if len(a) > 0:
                pn = PrimeNode()
                pn.ID = a[0].ID
                pn.Name = "Amazon"
                pn.PeakCustomerCount = 0
                pn.Rezcap = a[0].Rezcap
                self.frame.add(pn)
                self.mybusiness = pn
                self.frame.disable_subset(EmptyBusiness)

        if self.CurrentStep in self.schedule_deliveries:
            for c in self.schedule_deliveries[self.CurrentStep]:
                if hasattr(c.LivesAt, "Rezcap"):
                    v = MobdatVehicle()
                    v.Name = "AmazonTo%s_%s" % (c.Name, randint(0, 100))
                    v.Type = c.Vehicle.VehicleType
                    v.Route = self.mybusiness.Rezcap.DestinationName
                    v.Target = c.LivesAt.Rezcap.SourceName
                    self.frame.add(v)
                    self.__Logger.info("starting amazon delivery to %s", c.Name)
                else:
                    self.__Logger.debug("User %s is homeless (%s)", c.Name, c.LivesAt)
            del self.schedule_deliveries[self.CurrentStep]

        if self.mybusiness and len(self.schedule_deliveries) == 0:
            self.add_deliveries()

    def shutdown(self):
        pass
