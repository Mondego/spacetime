'''
Created on Apr 28, 2016

@author: Arthur Valadares
'''
from datamodel.nodesim.datamodel import RouteRequest, Route
from spacetime_local.declarations import Producer, GetterSetter, Tracker
from spacetime_local import IApplication

@Producer(RouteRequest)
@GetterSetter(RouteRequest, Route)
@Tracker(Route)
class NodeTestSimulation(IApplication.IApplication):
    def __init__(self, frame):
        '''
        Constructor
        '''
        self.frame = frame
        self.step = 0

    def initialize(self):
        pass

    def update(self):
        if self.step % 10 == 0:
            req = RouteRequest()
            req.Owner = "NodeTestSimulation"
            req.Source = None
            req.Destination = None
            self.frame.add(req)
        self.step += 1
        routes = self.frame.get_new(Route)
        if len(routes) > 0:
            for rt in routes:
                print rt.Waypoints
                self.frame.delete(rt)

    def shutdown(self):
        pass
