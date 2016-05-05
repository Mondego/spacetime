'''
Created on Apr 28, 2016

@author: Arthur Valadares
'''
from pcc.set import pcc_set
from pcc.attributes import primarykey, dimension
from pcc.projection import projection

@pcc_set
class Waypoint(object):
    '''
    classdocs
    '''
    #@primarykey(str)
    #def ID(self):
    #    return self._ID
    
    #@ID.setter
    #def ID(self, value):
    #    self._ID = value
        
    @dimension(tuple)
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
    classdocs
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
    classdocs
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
    classdocs
    '''
    
@pcc_set
class RouteRequest(object):
    '''
    classdocs
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
    @dimension(list)
    def Waypoints(self):
        return self._Waypoints
    
    @Waypoints.setter
    def Waypoints(self, value):
        self._Waypoints = value
    