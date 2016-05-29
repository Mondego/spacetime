'''
Created on Apr 28, 2016

@author: Arthur Valadares
'''
import logging

from datamodel.nodesim.datamodel import RouteRequest, Route, Waypoint,\
    ResidentialNode, BusinessNode
from spacetime_local import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Tracker, \
    Deleter
from random import choice


@Producer(RouteRequest)
@GetterSetter(RouteRequest, Route)
@Tracker(Route, Waypoint, ResidentialNode, BusinessNode)
@Deleter(Route)
class NodeTestSimulation(IApplication.IApplication):
    def __init__(self, frame):
        '''
        Constructor
        '''
        self.frame = frame
        self.step = 0
        self.logger = logging.getLogger(__name__)
        self._done = False

    def initialize(self):
        pass

    def update(self):
        if self.step % 5 == 0:
            # First route: from a random place to another random place
            req = RouteRequest()
            req.Owner = "NodeTestSimulation"
            req.Source = None # Picks a random source waypoint
            req.Destination = None # Picks a random destination waypoint
            req.Name = "Random"
            self.frame.add(req)

            # Second route: from a residential place to a business place
            resnodes = self.frame.get(ResidentialNode)
            bnodes = self.frame.get(BusinessNode)


            if len(resnodes) > 0 and len(bnodes) > 0:
                res_node = choice(resnodes)
                bus_node = choice(bnodes)
                if res_node.WP and bus_node.WP:
                    req2 = RouteRequest()
                    req2.Owner = "NodeTestSimulation"
                    req2.Source = res_node.WP
                    req2.Destination = bus_node.WP
                    req2.Name = "ResidentialToBusiness"
                    self.frame.add(req2)

        routes = self.frame.get_new(Route)
        if len(routes) > 0:
            for rt in routes:
                if rt.Owner == "NodeTestSimulation":
                    self.logger.info("[%s]: %s", rt.Name, rt.Waypoints)
                    self.frame.delete(Route, rt)
        self.step += 1

    def shutdown(self):
        self.logger.info("Shutting down NodeTestSimulation")

    @property
    def done(self):
        return self._done

    @done.setter
    def done(self, value):
        self._done = value

