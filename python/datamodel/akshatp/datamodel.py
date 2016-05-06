'''
Created on Dec 15, 2015

@author: arthurvaladares
'''

import logging
from pcc.join import join
from pcc.subset import subset
from pcc.parameterize import parameterize
from pcc.projection import projection
from pcc.set import pcc_set
from pcc.attributes import dimension, primarykey
from random import randrange

from spacetime_local.frame import frame

import traceback


logger = logging.getLogger(__name__)
LOG_HEADER = "[DATAMODEL]"
class Color:
    Red = 0
    Green = 1
    Blue = 2
    Yellow = 3
    Black = 4
    White = 5
    Grey = 6

#Vector3 = namedtuple("Vector3", ['X', 'Y', 'Z'])
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
class Car_akshatp(object):
    '''
    classdocs
    '''
    INITIAL_POSITION = 30
    FINAL_POSITION = 0
    SPEED = 1
    MAX_Y = 5

    _ID = None

    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    _Position = Vector3(INITIAL_POSITION, randrange(MAX_Y), 0)

    @dimension(Vector3)
    def Position(self):
        return self._Position

    @Position.setter
    def Position(self, value):
        self._Position = value

    _Velocity = Vector3(0, 0, 0)

    @dimension(Vector3)
    def Velocity(self):
        return self._Velocity

    @Velocity.setter
    def Velocity(self, value):
        self._Velocity = value

    _Color = randrange(7)

    @dimension(Color)
    def Color(self):
        return self._Color

    @Color.setter
    def Color(self, value):
        self._Color = value

    _Length = 0

    @dimension(int)
    def Length(self):
        return self._Length

    @Length.setter
    def Length(self, value):
        self._Length = value

    _Width = 0

    @dimension(int)
    def Width(self):
        return self._Width

    @Width.setter
    def Width(self, value):
        self._Width = value

    def __init__(self, uid=None):
        self.ID = uid
        self.Length = 1
        self.Color = randrange(7)
        self.Position = Vector3(self.INITIAL_POSITION, randrange(self.MAX_Y), 0)


@subset(Car_akshatp)
class InactiveCar_akshatp(Car_akshatp):
    @staticmethod
    def __query__(cars):
        return [c for c in cars if InactiveCar_akshatp.__predicate__(c)]

    @staticmethod
    def __predicate__(c):
        return c._Velocity == Vector3(0,0,0)

    def start(self):
        # logger.debug("[InactiveCar]: {0} starting".format(self.ID))
        self.Velocity = Vector3(self.SPEED, 0, 0)
        self.Position = Vector3(self.Position.X - self.Velocity.X, self.Position.Y + self.Velocity.Y,
                                self.Position.Z + self.Velocity.Z)


@subset(Car_akshatp)
class ActiveCar_akshatp(Car_akshatp):
    @staticmethod
    def __query__(cars):  # @DontTrace
        return [c for c in cars if ActiveCar_akshatp.__predicate__(c)]

    @staticmethod
    def __predicate__(c):
        return c.Velocity.X != 0

    def move(self):
        self.Position = Vector3(self.Position.X - self.Velocity.X, self.Position.Y + self.Velocity.Y,
                                self.Position.Z + self.Velocity.Z)
        # logger.debug("[ActiveCar] {2}: Current velocity: {0}, New position {1}".format(self.Velocity, self.Position, self.ID))

        # End of ride
        if (self.Position.X <= self.FINAL_POSITION):
            self.stop()

    def stop(self):
        # logger.debug("[ActiveCar]: {0} stopping".format(self.ID))

        self.Color = randrange(7)

        self.Position.X = self.INITIAL_POSITION
        self.Position.Y = randrange(self.MAX_Y)
        self.Position.Z = 0

        self.Velocity.X = 0
        self.Velocity.Y = 0
        self.Velocity.Z = 0


@pcc_set
class Pedestrian_akshatp(object):
    INITIAL_POSITION = -1
    FINAL_POSITION = 30
    SPEED = 1
    MAX_Y = 5

    _ID = None

    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    _X = 0

    @dimension(int)
    def X(self):
        return self._X

    @X.setter
    def X(self, value):
        self._X = value

    _Y = 0

    @dimension(int)
    def Y(self):
        return self._Y

    @Y.setter
    def Y(self, value):
        self._Y = value

    _hasAvoidedCollision = False

    @dimension(bool)
    def hasAvoidedCollision(self):
        return self._hasAvoidedCollision

    @hasAvoidedCollision.setter
    def hasAvoidedCollision(self, value):
        self._hasAvoidedCollision = value

    def __init__(self, i=None):
        self.ID = i
        self.X = self.INITIAL_POSITION
        self.Y = randrange(self.MAX_Y)
        self.hasAvoidedCollision = False

    def move(self):
        self.X += self.SPEED

        # logger.debug("[Pedestrian]: {0} New position <{1}, {2}>".format(self.ID, self.X, self.Y))

        # End of ride
        if self.X >= self.FINAL_POSITION:
            self.stop()

    def stop(self):
        # logger.debug("[Pedestrian]: {0} stopping".format(self.ID))
        self.X = self.INITIAL_POSITION

    def setposition(self, x):
        self.X = x


@subset(Pedestrian_akshatp)
class StoppedPedestrian_akshatp(Pedestrian_akshatp):
    @staticmethod
    def __query__(peds):
        return [p for p in peds if StoppedPedestrian_akshatp.__predicate__(p)]

    @staticmethod
    def __predicate__(p):
        return p.X == Pedestrian_akshatp.INITIAL_POSITION


@subset(Pedestrian_akshatp)
class Walker_akshatp(Pedestrian_akshatp):
    @staticmethod
    def __query__(peds):
        return [p for p in peds if Walker_akshatp.__predicate__(p)]

    @staticmethod
    def __predicate__(p):
        return p.X != Pedestrian_akshatp.INITIAL_POSITION and not p.hasAvoidedCollision


@subset(Pedestrian_akshatp)
class PedestrianHasAvodiedCollision_akshatp(Pedestrian_akshatp):
    @staticmethod
    def __query__(peds):
        return [p for p in peds if PedestrianHasAvodiedCollision_akshatp.__predicate__(p)]

    @staticmethod
    def __predicate__(p):
        return p.hasAvoidedCollision

    def move(self):
        # logger.debug("[Pedestrian]: {0} avoided collision!".format(self.ID));
        self.hasAvoidedCollision = False
        self.X += self.SPEED;
        if self.X >= self.FINAL_POSITION:
            self.stop();


@join(Pedestrian_akshatp, Car_akshatp)
class CarAndPedestrianNearby_akshatp(object):
    @primarykey(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(Car_akshatp)
    def car(self):
        return self._car

    @car.setter
    def car(self, value):
        self._car = value

    @dimension(Pedestrian_akshatp)
    def pedestrian(self):
        return self._ped

    @pedestrian.setter
    def pedestrian(self, value):
        self._ped = value

    def __init__(self, p, c):
        self.car = c
        self.pedestrian = p

    @staticmethod
    def __query__(peds, cars):
        return [CarAndPedestrianNearby_akshatp.Create(p, c) for p in peds for c in cars if
                CarAndPedestrianNearby_akshatp.__predicate__(p, c)]

    @staticmethod
    def __predicate__(p, c):
        if c.Position.Y == p.Y and c.Position.X - p.X < 6:
            return True
        return False

    def move(self):
        # logger.debug("[Pedestrian]: {0} avoiding collision!".format(self.ID));

        self.pedestrian.hasAvoidedCollision = True

        if self.pedestrian.Y == 0:
            self.pedestrian.Y += 1
        elif self.pedestrian.Y == self.pedestrian.MAX_Y - 1:
            self.pedestrian.Y -= 1
        else:
            self.pedestrian.Y += choice([-1, 1])
