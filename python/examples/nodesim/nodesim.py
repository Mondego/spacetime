'''
Created on Apr 28, 2016

@author: Arthur Valadares
'''
from datamodel.nodesim.datamodel import BusinessNode, ResidentialNode, Node, Waypoint,\
    RouteRequest, Route
from spacetime_local.declarations import Producer, GetterSetter, Tracker,\
    Deleter
from spacetime_local import IApplication
import os
from pyroute.loadOsm import LoadOsm
from pyroute.route import Router
from random import choice

@Producer(Waypoint, Node, BusinessNode, ResidentialNode, Route)
@GetterSetter(BusinessNode, ResidentialNode, Route, Waypoint, Node)
@Deleter(RouteRequest)
@Tracker(RouteRequest)
class NodeSimulation(IApplication.IApplication):
    def __init__(self, frame):
        '''
        Constructor
        '''
        self.frame = frame

    def initialize(self):
        path_data = os.path.dirname(os.path.realpath(__file__))
        self.data = LoadOsm(os.path.join(path_data,'data/RoadsMeters.osm.xml'))
        for node, loc in self.data.nodes.items():
            wpt = Waypoint()
            wpt.Location = loc
            wpt.Wpid = str(node)
            self.frame.add(wpt)

            if loc[1] > 1924.0:
                bnode = BusinessNode()
                bnode.BusinessType = "None"
                bnode.Name = str(node)
                bnode.Wpid = wpt
                self.frame.add(bnode)
            elif loc[1] < 473.0 or loc[0] < 283.0:
                rnode = ResidentialNode()
                rnode.Name = str(node)
                rnode.Wpid = wpt
                self.frame.add(rnode)
            else:
                node = Node()
                node.Name = str(node)
                node.Wpid = wpt
                self.frame.add(node)
        self.router = Router(self.data)

    def update(self):
        rt_reqs = self.frame.get_new(RouteRequest)
        from threading import currentThread
        for req in rt_reqs:
            if not req.Source:
                req.Source = choice(self.frame.get(Waypoint))
            if not req.Destination:
                req.Destination = choice(self.frame.get(Waypoint))
            result, route = self.router.doRouteAsLL(int(req.Source.Wpid),
                int(req.Destination.Wpid), "car", "nodes")
            if result:
                res = Route()
                res.Source = req.Source
                res.Destination = req.Destination
                res.Owner = req.Owner
                res.Waypoints = []
                for node in route:
                    wpt = self.frame.get(Waypoint, str(node))
                    res.Waypoints.append(wpt)
                print "Route from %s to %s" % (res.Source.Wpid,
                                               res.Destination.Wpid)
                print ",".join([w.Wpid for w in res.Waypoints])
            else:
                print "Cannot travel from %s to %s" % (req.Source.Wpid,
                                                       req.Destination.Wpid)
            self.frame.delete(RouteRequest, req)

    def shutdown(self):
        pass

