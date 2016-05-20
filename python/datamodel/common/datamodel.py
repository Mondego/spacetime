from pcc.set import pcc_set
from pcc.attributes import primarykey, dimension
from pcc.projection import projection
import uuid
import math

class Quaternion :

    # -----------------------------------------------------------------
    def __init__(self, x = 0.0, y = 0.0, z = 0.0, w = 1.0) :
        self.x = x
        self.y = y
        self.z = z
        self.w = w

    # -----------------------------------------------------------------
    # see http://www.euclideanspace.com/maths/geometry/rotations/conversions/eulerToQuaternion/
    # where heading is interesting and bank and attitude are 0
    # -----------------------------------------------------------------
    @classmethod
    def FromVector3(cls, vec):
        heading = math.atan(vec.Y/vec.X)
        if vec.X < 0:
            heading = heading + math.pi
        return Quaternion.FromHeading(heading)


    @classmethod
    def FromHeading(cls, heading) :
        c1 = math.cos(heading)
        s1 = math.sin(heading)
        w = math.sqrt(2.0 + 2.0 * c1) / 2.0
        z = (2.0 * s1) / (4.0 * w) if w != 0 else 1.0
        return Quaternion(0.0, 0.0, z, w)

    # -----------------------------------------------------------------
    def Equals(self, other) :
        return self.x == other.x and self.y == other.y and self.z == other.z and self.w == other.w

    # -----------------------------------------------------------------
    def ToList(self) :
        return [self.x, self.y, self.z, self.w]

    # -----------------------------------------------------------------
    def ToHeading(self) :
        return math.atan2(2.0 * self.y * self.w - 2.0 * self.x * self.z, 1.0 - 2.0 * self.y * self.y - 2.0 * self.z * self.z)

    # -----------------------------------------------------------------
    def __eq__(self, other) :
        return self.Equals(other)

    # -----------------------------------------------------------------
    def __str__(self) :
        fmt = "<{0}, {1}, {2}, {3}>"
        return fmt.format(self.x, self.y, self.z, self.w)

    @staticmethod
    def __decode__(dic):
        if 'x' in dic and 'y' in dic and 'z' in dic and 'w' in dic:
            return Quaternion(dic['x'], dic['y'], dic['z'], dic['w'])
        elif 'X' in dic and 'Y' in dic and 'Z' in dic and 'W' in dic:
            return Quaternion(dic['X'], dic['Y'], dic['Z'], dic['W'])
        else:
            raise Exception("Could not decode Vector3 with dic %s" % dic)

    def __json__(self):
        return self.__dict__

class Vector3(object):
    X = 0
    Y = 0
    Z = 0

    def __init__(self, X, Y, Z):
        self.X = X
        self.Y = Y
        self.Z = Z

    def AddVector(self, other) :
        return Vector3(self.X + other.X, self.Y + other.Y, self.Z + other.Z)

    # -----------------------------------------------------------------
    def SubVector(self, other) :
        return Vector3(self.X - other.X, self.Y - other.Y, self.Z - other.Z)

    # -----------------------------------------------------------------
    def ScaleConstant(self, factor) :
        return Vector3(self.X * factor, self.Y * factor, self.Z * factor)

    # -----------------------------------------------------------------
    def ScaleVector(self, scale) :
        return Vector3(self.X * scale.X, self.Y * scale.Y, self.Z * scale.Z)

    def ToList(self):
        return [self.X, self.Y, self.Z]

    def Rotate(self, rad):
        heading = math.atan(self.Y/self.X)
        return Vector3()

    def __json__(self):
        return self.__dict__

    def __str__(self):
        return self.__dict__.__str__()

    def __eq__(self, other):
        return (isinstance(other, Vector3) and (other.X == self.X and other.Y == self.Y and other.Z == self.Z))

    def __ne__(self, other):
        return not self.__eq__(other)

    # -----------------------------------------------------------------
    def __add__(self, other) :
        return self.AddVector(other)

    # -----------------------------------------------------------------
    def __sub__(self, other) :
        return self.SubVector(other)

    # -----------------------------------------------------------------
    def __mul__(self, factor) :
        return self.ScaleConstant(factor)

    # -----------------------------------------------------------------
    def __div__(self, factor) :
        return self.ScaleConstant(1.0 / factor)

    @staticmethod
    def __decode__(dic):
        return Vector3(dic['X'], dic['Y'], dic['Z'])

class Color:
    Red = 0
    Green = 1
    Blue = 2
    Yellow = 3
    Black = 4
    White = 5
    Grey = 6

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
