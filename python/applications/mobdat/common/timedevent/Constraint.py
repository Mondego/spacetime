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

@file    Constraint.py
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

import math
from applications.mobdat.common.Utilities import GenName

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class Constraint :
    Priority = 0
    MinimalVariance = 0.00001

    # -----------------------------------------------------------------
    def __init__(self) :
        self.ConstraintID = GenName('CONSTRAINT')

    # -----------------------------------------------------------------
    @staticmethod
    def fpcompare(v1, v2) :
        if math.fabs(v1 - v2) < Constraint.MinimalVariance :
            return 0
        if v1 < v2 :
            return -1
        return 1

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class OrderConstraint(Constraint) :
    Priority = 1

    # -----------------------------------------------------------------
    def __init__(self, id1, id2, delta = 0) :
        """Create an instance of the OrderConstraint.

        Args:
            id1 -- symbolic identifier for the first time variable
            id2 -- symbolic identifier for the second time variable
            delta -- the minimum separation between the time variables
        """
        Constraint.__init__(self)

        self.ID1 = id1
        self.ID2 = id2
        self.Delta = delta

    # -----------------------------------------------------------------
    def Apply(self, varstore) :
        ev1 = varstore[self.ID1]
        ev2 = varstore[self.ID2]

        changed = False

        if self.fpcompare(ev2.IntervalStart, ev1.IntervalStart + self.Delta) < 0 :
            ev2.IntervalStart = ev1.IntervalStart + self.Delta
            changed = True

        if self.fpcompare(ev2.IntervalEnd, ev1.IntervalEnd + self.Delta) < 0 :
            ev1.IntervalEnd = ev2.IntervalEnd - self.Delta
            changed = True

        return changed

    # -----------------------------------------------------------------
    def Dump(self, varstore) :
        print "{0}: ID1={1}, ID2={2}, Delta={3}".format(self.ConstraintID, self.ID1, self.ID2, self.Delta)
        print "{0:5s} == {1}".format(self.ID1, str(varstore[self.ID1]))
        print "{0:5s} == {1}".format(self.ID2, str(varstore[self.ID2]))

        ev1 = varstore[self.ID1]
        ev2 = varstore[self.ID2]

        if ev2.IntervalStart < ev1.IntervalStart + self.Delta :
            print "ev2.IntervalStart < ev1.IntervalStart + self.Delta"
            print "{2} < {0} + {1}".format(ev1.IntervalEnd, self.Delta, ev2.IntervalStart)

        if ev1.IntervalEnd + self.Delta > ev2.IntervalEnd :
            print "ev1.IntervalEnd + self.Delta > ev2.IntervalEnd"
            print "{0} + {1} > {2}".format(ev1.IntervalEnd, self.Delta, ev2.IntervalStart)

        print
