#!/usr/bin/env python
"""
Copyright (c) 2014, Intel Corporation

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

* Neither the name of Intel Corporation nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

@file    ValueTypes.py
@author  Mic Bowman
@date    2013-12-03

This module defines some useful value types for the simulation including
Vector3 for positions and velocity and Quaternion for rotations.

"""

import os, sys
import random, math

if os.environ.get("SUMO_HOME"):
    sys.path.append(os.path.join(os.environ.get("SUMO_HOME"), "tools"))
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
def MakeEnum(*sequential, **named):
    """Create an enum type from a list of strings
    """

    enums = dict(zip(sequential, range(len(sequential))), **named)
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums['KeyName'] = reverse
    return type('Enum', (), enums)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
DaysOfTheWeek = MakeEnum('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
Cardinals = MakeEnum('WEST', 'NORTH', 'EAST', 'SOUTH')

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class WeightedChoice :

    # -----------------------------------------------------------------
    def __init__(self, choices = {}) :
        self.Modified = False
        self.TotalWeight = 0
        self.Weights = {}

        for choice, weight in choices.iteritems() :
            self.AddChoice(choice, weight)

    # -----------------------------------------------------------------
    def AddChoice(self, choice, weight) :
        self.Modified = True
        self.Weights[choice] = weight

    # -----------------------------------------------------------------
    def DropChoice(self, choice) :
        self.Modified = True
        del self.Weights[choice]

    # -----------------------------------------------------------------
    def Choices(self) :
        return self.Weights.keys()

    # -----------------------------------------------------------------
    def Choose(self) :
        if self.Modified :
            self.TotalWeight = 0
            for weight in self.Weights.itervalues() :
                self.TotalWeight += weight
            self.Modified = False

        pick = random.uniform(0, self.TotalWeight)

        for choice, weight in self.Weights.iteritems() :
            if pick - weight <= 0 : return choice
            pick = pick - weight

        # this should only happen on floating point oddness
        return random.choice(self.Weights.keys())

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class Vector3 :

    # -----------------------------------------------------------------
    def __init__(self, x = 0.0, y = 0.0, z = 0.0) :
        # self._value = (x, y, z)
        self.x = x
        self.y = y
        self.z = z

    # -----------------------------------------------------------------
    def ToList(self) :
        return [self.x, self.y, self.z]

    # -----------------------------------------------------------------
    def VectorDistanceSquared(self, other) :
        dx = self.x - other.x
        dy = self.y - other.y
        dz = self.z - other.z
        return dx * dx + dy * dy + dz * dz

    # -----------------------------------------------------------------
    def VectorDistance(self, other) :
        return math.sqrt(self.VectorDistanceSquared(other))

    # -----------------------------------------------------------------
    def Length(self) :
        return math.sqrt(self.VectorDistanceSquared(ZeroVector))

    # -----------------------------------------------------------------
    def LengthSquared(self) :
        return self.VectorDistanceSquared(ZeroVector)

    # -----------------------------------------------------------------
    def AddVector(self, other) :
        return Vector3(self.x + other.x, self.y + other.y, self.z + other.z)

    # -----------------------------------------------------------------
    def SubVector(self, other) :
        return Vector3(self.x - other.x, self.y - other.y, self.z - other.z)

    # -----------------------------------------------------------------
    def ScaleConstant(self, factor) :
        return Vector3(self.x * factor, self.y * factor, self.z * factor)

    # -----------------------------------------------------------------
    def ScaleVector(self, scale) :
        return Vector3(self.x * scale.x, self.y * scale.y, self.z * scale.z)

    # -----------------------------------------------------------------
    def Equals(self, other) :
        if isinstance(other, Vector3):
            return self.x == other.x and self.y == other.y and self.z == other.z
        elif isinstance(other, tuple) or isinstance(other, list):
            return (other[0] == self.x and other[1] == self.y and other[2] == self.z)

    # -----------------------------------------------------------------
    def ApproxEquals(self, other, tolerance) :
        return self.VectorDistanceSquared(other) < (tolerance * tolerance)

    # -----------------------------------------------------------------
    def __eq__(self, other) :
        return self.Equals(other)

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

    # -----------------------------------------------------------------
    def __str__(self) :
        fmt = "<{0:f}, {1:f}, {2:f}>"
        return fmt.format(self.x, self.y, self.z)

    def __json__(self):
        return self.__dict__

    @staticmethod
    def __decode__(dic):
        if 'x' in dic and 'y' in dic and 'z' in dic:
            return Vector3(dic['x'], dic['y'], dic['z'])
        elif 'X' in dic and 'Y' in dic and 'Z' in dic:
            return Vector3(dic['X'], dic['Y'], dic['Z'])
        else:
            raise Exception("Could not decode Vector3 with dic %s" % dic)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
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
    def FromHeading(self, heading) :
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

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
ZeroVector = Vector3(0.0, 0.0, 0.0)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
if __name__ == '__main__' :
    xx = WeightedChoice({ 'foo' :  0.0000001})
    print xx.Choose()

    weight = 1.0 / 3.0
    yy = WeightedChoice({ 'a' :  weight, 'b' : weight, 'c' : weight})
    for i in range(15) :
        print yy.Choose()
