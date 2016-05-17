from pcc.set import pcc_set
from pcc.attributes import primarykey, dimension
from pcc.projection import projection
import numpy as np
import uuid

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
class Vehicle(object):
    '''
    Description

    Describes a vehicle's basic properties. Should be used as a base class for
    more features.

    Properties

    Position: x,y,z (Vector3) position of vehicle
    Velocity: x,y,z (Vector3) velocity of vehicle
    Length: Length of vehicle from back to front in meters
    Width: Width of vehicle from side to side in meters
    '''
    _ID = None
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

    @dimension(Vector3)
    def Position(self):
        return self._Position

    @Position.setter
    def Position(self, value):
        self._Position = value

    @dimension(Vector3)
    def Velocity(self):
        return self._Velocity

    @Velocity.setter
    def Velocity(self, value):
        self._Velocity = value

    @dimension(int)
    def Length(self):
        return self._Length

    @Length.setter
    def Length(self, value):
        self._Length = value

    @dimension(int)
    def Width(self):
        return self._Width

    @Width.setter
    def Width(self, value):
        self._Width = value

    def __init__(self):
        self.ID = str(uuid.uuid4())
        self.Position = Vector3(0,0,0)
        self.Velocity = Vector3(0,0,0)
        self.Length = 0
        self.Width = 0
        self.Name = ""
