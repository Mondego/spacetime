from multiprocessing import Process, Event as MPEvent, Queue
import unittest
import time

from rtypes import pcc_set, primarykey, dimension, subset, predicate
from rtypes.utils.enums import Datatype

from spacetime import Dataframe
from spacetime.utils.enums import Event

@pcc_set
class Car(object):
    oid = primarykey(int)
    xvel = dimension(int)
    yvel = dimension(int)
    xpos = dimension(int)
    ypos = dimension(int)
    color = dimension(str)

    def start(self, vel):
        self.xvel, self.yvel = vel

    def details(self):
        return self.oid, self.xvel, self.yvel, self.xpos, self.ypos, self.color

    def __init__(self, oid,  color):
        self.oid = oid
        self.xvel = 0
        self.yvel = 0
        self.xpos = 0
        self.ypos = 0
        self.color = color

@subset(Car)
class ActiveCar(object):
    @predicate(Car.xvel, Car.yvel)
    def pred_func(xvel, yvel):
        return xvel != 0 or yvel != 0

    def move(self):
        self.xpos += self.xvel
        self.ypos += self.yvel

    def details(self):
        return self.oid, self.xvel, self.yvel, self.xpos, self.ypos, self.color

    def stop(self):
        self.xvel, self.yvel = 0, 0

@subset(Car)
class RedCar(object):
    @predicate(Car.color)
    def pred_func(color):
        return color == "RED"

    def details(self):
        return self.oid, self.xvel, self.yvel, self.xpos, self.ypos, self.color

@subset(ActiveCar)
class RedActiveCar(object):
    @predicate(Car.color)
    def pred_func(color):
        return color == "RED"

    def details(self):
        return self.oid, self.xvel, self.yvel, self.xpos, self.ypos, self.color


class TestSubset(unittest.TestCase):
    def test_one_df_subset(self):
        df = Dataframe("test", [Car, ActiveCar])
        car = Car(0, "BLUE")
        df.add_one(Car, car)
        df.commit()
        self.assertEqual(1, len(df.read_all(Car)))
        self.assertEqual(0, len(df.read_all(ActiveCar)))
        car.start((10, 10))
        self.assertEqual(1, len(df.read_all(Car)))
        self.assertEqual(1, len(df.read_all(ActiveCar)))
        acar = df.read_one(ActiveCar, 0)
        self.assertEqual(acar.details, (0, 10, 10, 0, 0, "BLUE"))
        self.assertIsInstance(acar, ActiveCar)
        df.commit()
        self.assertEqual(1, len(df.read_all(Car)))
        self.assertEqual(1, len(df.read_all(ActiveCar)))
        self.assertEqual(acar.details, (0, 10, 10, 0, 0, "BLUE"))
        self.assertIsInstance(acar, ActiveCar)
        
        
