'''
Created on Apr 28, 2016

@author: Arthur Valadares
'''
from pcc.set import pcc_set
from pcc.attributes import primarykey, dimension
from pcc.projection import projection

class Vector3(object):
    X = 0
    Y = 0
    Z = 0

    def __init__(self, X, Y, Z):
        self.X = X
        self.Y = Y
        self.Z = Z

    def __json__(self):
        return self.__dict__

    def __str__(self):
        return self.__dict__.__str__()

    def __eq__(self, other):
        return (isinstance(other, Vector3) and (other.X == self.X and other.Y == self.Y and other.Z == self.Z))

    def __ne__(self, other):
        return not self.__eq__(other)

    @staticmethod
    def __decode__(dic):
        return Vector3(dic['X'], dic['Y'], dic['Z'])

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

    _Waypoint = None
    @dimension(Waypoint)
    def Waypoint(self):
        return self._Waypoint

    @Waypoint.setter
    def Waypoint(self, value):
        self._Wapoint = value

    def __init__(self):
        self.ID = None

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
        self.BusinessType = value

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
        if type(value) == RouteRequest:
            pass
        self._Destination = value

    @dimension(str)
    def Owner(self):
        return self._Owner

    @Owner.setter
    def Owner(self, value):
        self._Owner = value

@pcc_set
class Route(RouteRequest.Class()):
    '''
    Description

    A Route describes source, destination, and a list of waypoints to get from
    one to another. Route is created as a response to RouteRequest (see
    RouteRequest).


    Properties

    Waypoints: A list of Waypoint objects, describing the route from Source to
    Destination point by point. Path between two points should be considered a
    straight line.
    '''
    @dimension(list)
    def Waypoints(self):
        return self._Waypoints

    @Waypoints.setter
    def Waypoints(self, value):
        self._Waypoints = value
