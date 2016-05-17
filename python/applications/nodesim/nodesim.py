'''
Created on Apr 28, 2016

@author: Arthur Valadares
'''
import copy
import logging
import os
from random import choice

from datamodel.nodesim.datamodel import Vector3
from datamodel.nodesim.datamodel import BusinessNode, ResidentialNode, Node, Waypoint, \
    RouteRequest, Route, Road, Edge
from spacetime_local import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Tracker, \
    Deleter

from .pyroute.loadOsm import LoadOsm
from .pyroute.route import Router


@Producer(Waypoint, Node, BusinessNode, ResidentialNode, Route, Road)
@GetterSetter(Waypoint)
@Deleter(RouteRequest)
@Tracker(RouteRequest)
class NodeSimulation(IApplication.IApplication):
    def __init__(self, frame):
        '''
        Constructor
        '''
        self.frame = frame
        self.logger = logging.getLogger(__name__)

    def initialize(self):
        path_data = os.path.dirname(os.path.realpath(__file__))
        self.data = LoadOsm(os.path.join(path_data,'data/RoadsMeters.osm.xml'), storeMap = 1)
        self.logger.info(self.data.report())
        for node, loc in self.data.nodes.items():
            # Y
            lat = loc[0]
            # X
            lon = loc[1]
            wpt = Waypoint()
            wpt.Location = Vector3(lon, lat, 0)
            wpt.Wpid = str(node)
            self.frame.add(wpt)

            if lat > 1924.0:
                bnode = BusinessNode()
                bnode.BusinessType = ""
                bnode.Name = str(node)
                bnode.WP = wpt
                self.frame.add(bnode)
            # node 310 (lon: 283, lat: 473)
            # node 30 (lon: 633)
            # node 2400 (lat: 982)
            elif (lat < 982.0 and lon < 283.0) or (lat < 473.0 and lon < 620.0):
                rnode = ResidentialNode()
                rnode.Name = str(node)
                rnode.WP = wpt
                self.frame.add(rnode)
            else:
                normal_node = Node()
                normal_node.Name = str(node)
                normal_node.WP = wpt
                self.frame.add(normal_node)

        for way in self.data.ways:
            road = Road()
            for k,v in way.items():
                if k == "name":
                    road.Name = v
                elif k == "lanes":
                    road.Lanes = v
                elif k == "oneway":
                    if v == "yes":
                        road.Oneway = True
                    else:
                        road.Oneway = False
                elif k == "n":
                    for wpid in v:
                        try:
                            road.Waypoints.append(self.frame.get(Waypoint,str(wpid)))
                        except:
                            self.logger.error("could not find wpid %s" % wpid)
            self.frame.add(road)

        self.edges = Edge.EdgesFromRoads(self.frame.get(Road))
        self.router = Router(self.data)

    def update(self):
        rt_reqs = self.frame.get_new(RouteRequest)
        for req in rt_reqs:
            if not req.Source:
                req.Source = choice(self.frame.get(Waypoint))
            if not req.Destination:
                req.Destination = choice(self.frame.get(Waypoint))
            try:
                result, route = self.router.doRouteAsLL(int(req.Source.Wpid),
                    int(req.Destination.Wpid), "car", "nodes")
            except:
                self.logger.error("Wpid invalid. List: %s", [wp.Wpid for wp in self.frame.get(Waypoint)])
            res = Route()
            res.Owner = req.Owner
            res.Name = req.Name
            res.Waypoints = []
            if result == "success":
                res.Source = req.Source
                res.Destination = req.Destination

                edge = self.edges[str(route[0]) + "=o=" + str(route[1])]
                previous = edge.get_coordinates()[1]
                res.Waypoints.append(previous)
                for i in xrange(1,len(route)-1):
                    edge = self.edges[str(route[i]) + "=o=" + str(route[i+1])]
                    curpoint = copy.copy(edge.get_coordinates()[0])
                    curpoint.X = (curpoint.X + previous.X)/2
                    curpoint.Y = (curpoint.Y + previous.Y)/2
                    res.Waypoints.append(curpoint)
                    previous = copy.copy(edge.get_coordinates()[1])
                res.Waypoints.append(edge.get_coordinates()[1])

                self.logger.debug("Route from %s to %s",res.Source.Wpid,
                                               res.Destination.Wpid)
                self.logger.debug(",".join([str(w) for w in res.Waypoints]))

                # Draws maps of adjusted and original route on matplotlib
                # originals = [self.frame.get(Waypoint,str(nodeid)) for nodeid in route]
                # self.draw_maps(res.Waypoints,originals)

            else:
                self.logger.warn("Cannot travel from %s to %s",
                                                       req.Source.Wpid,
                                                       req.Destination.Wpid)
                res.Source = None
                res.Destination = None
            self.frame.add(res)
            self.frame.delete(RouteRequest, req)

    def draw_maps(self, adjusted, originals, block=True):
        import matplotlib.pyplot as plt
        plt.plot([vec.Location.X for vec in originals],[vec.Location.Y for vec in originals], marker='^')
        plt.plot([vec.X for vec in adjusted],[vec.Y for vec in adjusted], marker='o')
        plt.axis('equal')
        plt.show(block=block)

    def shutdown(self):
        self.logger.info("Shutting down NodeSimulation")
