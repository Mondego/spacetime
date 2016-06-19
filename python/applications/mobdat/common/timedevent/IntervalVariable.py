#!/usr/bin/python
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

@file    IntervalVariable.py
@author  Mic Bowman
@date    2014-03-31

This package defines modules for the mobdat simulation environment

"""

import os, sys
import logging

# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

import random, math
from applications.mobdat.common.Utilities import GenName

logger = logging.getLogger(__name__)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class IntervalVariable :
    Priority = 5
    MinimalVariance = 0.00001

    # -----------------------------------------------------------------
    def __init__(self, stime, etime = None, iv_id = None) :
        """ Create a variable to use for time constraints

        Args:
            stime -- float, the interval start time
            etime -- float, the interval end time
            ade_id -- unique identifier for the time variable
        """

        self.IntervalStart = float(min(stime, etime or stime))
        self.IntervalEnd = float(max(stime, etime or stime))
        self.ID = iv_id or GenName('TV')

    # -----------------------------------------------------------------
    def __str__(self) :
        if self.IsFixed() :
            return "[{0:5.3f}]".format(self.GetValue())

        return "<{0:5.3f}:{1:5.3f}>".format(self.IntervalStart, self.IntervalEnd)

    # -----------------------------------------------------------------
    def __float__(self) :
        if self.IsFixed() :
            return self.IntervalStart

        raise ValueError("Attempt to convert indeterminant time variable to float {0}".format(self.ID))

    # -----------------------------------------------------------------
    def Copy(self, time_id = None) :
        """ Create a copy of the time variable """
        return self.__class__(self.IntervalStart, self.IntervalEnd, time_id)

    # -----------------------------------------------------------------
    def IsFixed(self) :
        """ Predicate to determine if the interval has set a single value """
        return math.fabs(self.IntervalEnd - self.IntervalStart) < IntervalVariable.MinimalVariance

    # -----------------------------------------------------------------
    def IsValid(self) :
        """ Predicate to determine if the interval has any valid values """
        if self.IsFixed() :
            return True

        # if not self.IntervalStart <= self.IntervalEnd :
        #     logger.warn("Variable %s is invalid: %s", self.ID, str(self))

        return self.IntervalStart <= self.IntervalEnd

    # -----------------------------------------------------------------
    def PickValue(self) :
        return self.SetValue(random.uniform(self.IntervalStart, self.IntervalEnd))

    # -----------------------------------------------------------------
    def SetValue(self, value) :
        """ Fix the time interval to a single value

        Args:
            time -- a value in the interval
        Returns:
            the fixed value of the variable
        """

        # value < self.IntervalStart
        if self.IntervalStart - value > IntervalVariable.MinimalVariance :
            logger.warn("Invalid value: %s, too small, expecting range %s", value, str(self))
            raise ValueError("Invalid value")

        # self.IntervalEnd < value :
        if value - self.IntervalEnd > IntervalVariable.MinimalVariance :
            logger.warn("Invalid value: %s, too large, expecting range %s", value, str(self))
            raise ValueError("Invalid value")

        self.IntervalStart = value
        self.IntervalEnd = value
        return value

    # -----------------------------------------------------------------
    def GetValue(self) :
        return self.IntervalStart if self.IsFixed() else None

    # -----------------------------------------------------------------
    def Overlaps(self, s, e) :
        # IntervalStart is between s & e
        if s <= self.IntervalStart and self.IntervalStart <= e : return True

        # IntervalEnd is between s & e
        if s <= self.IntervalEnd and self.IntervalEnd <= e : return True

        # The interval contains [s, e]
        if self.IntervalStart <= s and e <= self.IntervalEnd : return True

        return False

    # -----------------------------------------------------------------
    def LT(self, value, maybe = True) :
        return self.IntervalStart < value if maybe else self.IntervalEnd < value

    # -----------------------------------------------------------------
    def GT(self, value, maybe = True) :
        return self.IntervalEnd > value if maybe else self.IntervalStart > value

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class GaussianIntervalVariable(IntervalVariable) :
    Priority = 5

    # -----------------------------------------------------------------
    def PickValue(self) :
        mean = (self.IntervalStart + self.IntervalEnd) / 2.0
        stdev = (self.IntervalEnd - self.IntervalStart) / 4.0
        value = max(self.IntervalStart, min(self.IntervalEnd, random.gauss(mean, stdev)))

        return self.SetValue(value)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class MinimumIntervalVariable(IntervalVariable) :
    Priority = 0

    # -----------------------------------------------------------------
    def PickValue(self) :
        return self.SetValue(self.IntervalStart)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class MaximumIntervalVariable(IntervalVariable) :
    Priority = 0

    # -----------------------------------------------------------------
    def PickValue(self) :
        return self.SetValue(self.IntervalEnd)


