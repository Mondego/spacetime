'''
Created on Apr 28, 2016

@author: Arthur Valadares
'''
from __future__ import absolute_import

from pcc.set import pcc_set
from pcc.attributes import primarykey, dimension
from pcc.projection import projection
import numpy as np
from datamodel.common.datamodel import Vector3

@pcc_set
class Waypoint(object):
    '''
    Description

    Waypoint represents nodes in OSM XML file.

    Properties

    Location: Vector3(X, Y, Z) coordinates in meters
    Wpid: Matches unique identifier node id in OSM XML file.
    '''
    @dimension(Vector3)
    def Location(self):
        return self._Location

    @Location.setter
    def Location(self, value):
        self._Location = value

    @primarykey(str)
    def Wpid(self):
        return self._Wpid

    @Wpid.setter
    def Wpid(self, value):
        self._Wpid = value

@pcc_set
class Edge(object):
    '''
    TODO
    '''
    @staticmethod
    def EdgesFromRoads(roads):
        edges = {}
        for rd in roads:
            wps = rd.Waypoints
            for i in xrange(len(wps)-1):
                ed = Edge()
                ed.Source = wps[i]
                ed.Destination = wps[i+1]
                ed.Name = wps[i].Wpid + "=o=" + wps[i+1].Wpid
                ed.Oneway = rd.Oneway
                edges[ed.Name] = ed
            if rd.Oneway == False:
                for i in xrange(len(wps)-1, -1, -1):
                    ed = Edge()
                    ed.Source = wps[i]
                    ed.Destination = wps[i-1]
                    ed.Name = wps[i].Wpid + "=o=" + wps[i-1].Wpid
                    ed.Oneway = rd.Oneway
                    edges[ed.Name] = ed
        return edges

    # 3 meter offsets for two-way roads
    _offset = 3
    @dimension(Waypoint)
    def Source(self):
        return self._Source

    @Source.setter
    def Source(self, value):
        self._Source = value

    @dimension(Waypoint)
    def Destination(self):
        return self._Destination

    @Destination.setter
    def Destination(self, value):
        self._Destination = value

    @primarykey(str)
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    @dimension(bool)
    def Oneway(self):
        return self._Oneway

    @Oneway.setter
    def Oneway(self, value):
        self._Oneway = value

    def get_coordinates(self):
        if self.Oneway == True:
            return (self.Source.Location, self.Destination.Location)
        elif hasattr(self, "_coordinates"):
            return self._coordinates
        else:
            (x0,y0) = self.Source.Location.X,  self.Source.Location.Y
            (x1,y1) = self.Destination.Location.X,  self.Destination.Location.Y

            # normalize and multiply by offset
            vec = np.array((x1-x0,y1-y0))
            norm_vec = np.linalg.norm(vec)
            normal_vec = vec/norm_vec

            # offset normal ortogonal of vector
            ovec = np.array((normal_vec[1], -normal_vec[0])) * self._offset

            source_coord = ovec + (x0,y0)
            parallel_coord = (normal_vec * norm_vec) + source_coord

            self._coordinates = (
                    Vector3(source_coord[0], source_coord[1],0) ,
                    Vector3(parallel_coord[0], parallel_coord[1],0))
            #print self._coordinates
            return self._coordinates

@pcc_set
class Road(object):
    '''
    Description

    A Road contains a list of Waypoints matching the roads in the original map.

    Properties

    ID: Primary key, automatically generated
    Name: Name of the road
    Waypoints: List of waypoints defining the road
    Oneway: True if the road is one-way, false if not
    Lanes: Number of lanes on the road
    '''
    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(str)
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    @dimension(list)
    def Waypoints(self):
        return self._Waypoints

    @Waypoints.setter
    def Waypoints(self, value):
        self._Waypoints = value

    @dimension(bool)
    def Oneway(self):
        return self._Oneway

    @Oneway.setter
    def Oneway(self, value):
        self._Oneway = value

    @dimension(int)
    def Lanes(self):
        return self._Lanes

    @Lanes.setter
    def Lanes(self, value):
        self._Lanes = value

    def __init__(self):
        self.Lanes = 1
        self.Waypoints = []
        self.Name = ""
        self.Oneway = True


@pcc_set
class Node(object):
    '''
    Description

    Nodes are used to expand semantics of Waypoints. A Node could represent a
    business, a residence, a traffic light, or any other meaning attached to a
    certain Waypoint.

    Properties

    ID: Primary key, automatically generated
    Name: Name of node as string
    Waypoint: Reference to the Waypoint it represents.
    '''
    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(str)
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    @dimension(Waypoint)
    def WP(self):
        return self._Waypoint

    @WP.setter
    def WP(self, value):
        self._Waypoint = value

    def __init__(self):
        self.Name = ""
        self.WP = None

@pcc_set
class BusinessNode(Node.Class()):
    '''
    Description

    A BusinessNode is a Node of a business/industry location. BusinessNode
    is used to mark Waypoints that are business-related, like the location
    of a Business for vehicles to move to and from.

    Properties

    BusinessType: Description or name of business
    '''
    @dimension(str)
    def BusinessType(self):
        return self._BusinessType

    @BusinessType.setter
    def BusinessType(self, value):
        self._BusinessType = value

@pcc_set
class ResidentialNode(Node.Class()):
    '''
    Description

    A ResidentialNode is a Node of a residence. It is used to mark a Waypoint as
    a residential location, for vehicles to move to and from.
    '''

@pcc_set
class RouteRequest(object):
    '''
    Description

    RouteRequest is used to request a route between two waypoints. The request
    is fulfilled by the NodeSimulation application, that tracks RouteRequest and
    produces a Route containing a list of Waypoints.
    To create a RouteRequest, Source and Destination can be assigned Waypoints,
    or, if assigned None, a random Waypoints will be filled in.

    Properties

    ID: Primary key, automatically filled in
    Source: Source waypoint (set to None for random)
    Destination: Destination waypoint (set to None for random)
    Owner: A unique identifiable name so the application can track its result
    Name: An optional name for the route
    '''
    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(Waypoint)
    def Source(self):
        return self._Source

    @Source.setter
    def Source(self, value):
        self._Source = value

    @dimension(Waypoint)
    def Destination(self):
        return self._Destination

    @Destination.setter
    def Destination(self, value):
        self._Destination = value

    @dimension(str)
    def Owner(self):
        return self._Owner

    @Owner.setter
    def Owner(self, value):
        self._Owner = value

    @dimension(str)
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    def __init__(self):
        self.Source = None
        self.Destination = None
        self.Owner = ""
        self.Name = ""

@pcc_set
class Route(RouteRequest.Class()):
    '''
    Description

    A Route describes source, destination, and a list of Vector3 coordinates
    to get from one to another. Route is created as a response to RouteRequest
    (see RouteRequest).


    Properties

    Waypoints: A list of Vector3 objects describing the route from Source to
    Destination point by point. Path between two points should be considered a
    straight line.
    '''
    @dimension(list)
    def Waypoints(self):
        return self._Waypoints

    @Waypoints.setter
    def Waypoints(self, value):
        self._Waypoints = value
