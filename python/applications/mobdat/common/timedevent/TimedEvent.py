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

@file    TimedEvent.py
@author  Mic Bowman
@date    2014-03-31

This package defines modules for the mobdat simulation environment

"""

import os, sys
import logging
from applications.mobdat.common.timedevent.IntervalVariable import GaussianIntervalVariable,\
    MaximumIntervalVariable, MinimumIntervalVariable
from applications.mobdat.common.Utilities import GenName
from applications.mobdat.common.timedevent.Constraint import OrderConstraint

# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

logger = logging.getLogger(__name__)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class TripEvent :
    def __init__(self, stime, splace, dplace) :
        self.StartTime = stime
        self.SrcName = splace.Details
        self.DstName = dplace.Details

    def __str__(self) :
        return 'travel from {0} to {1} starting at {2}'.format(self.SrcName, self.DstName, self.StartTime)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class PlaceEvent :

    # -----------------------------------------------------------------
    def __init__(self, details, stimevar, etimevar, duration = 0.01, pe_id = None) :
        self.Details = details
        self.EventID = pe_id or GenName('PLACE')

        # EventStart and EventEnd properties contain the original interval definitions, all
        # constraint resolution will happen on copies of these variables stored
        # EventStart and EventEnd properties.
        self.BaseEventStart = stimevar
        self.BaseEventEnd = etimevar
        self.Reset()

        # Duration is the minimum duration of the event
        self.Duration = max(duration, 0.01)

        # Arrival and Departure properties connect this event into an
        # ordered chain of events
        self.Arrival = None
        self.Departure = None

    # -----------------------------------------------------------------
    def Split(self) :
        raise AttributeError("Event {0} of type {1} is not splittable".format(self.EventID, self.__class__.__name__))

    # -----------------------------------------------------------------
    def NextPlace(self) :
        return self.Departure.DstPlace if self.Departure else None

    # -----------------------------------------------------------------
    def PrevPlace(self) :
        return self.Arrival.SrcPlace if self.Arrival else None

    # -----------------------------------------------------------------
    def Reset(self) :
        """
        Copy the initial start and end interval variables into the EventStart
        and EventEnd properties
        """
        self.EventStart = self.BaseEventStart.Copy(self.BaseEventStart.ID)
        self.EventEnd = self.BaseEventEnd.Copy(self.BaseEventEnd.ID)

    # -----------------------------------------------------------------
    def AddVariables(self, vstore) :
        """
        Place interval variables into a variable store object so that the event structure
        itself remains "clean" and a reset can take place if resolution fails.

        Args:
            vstore -- TimedEventList.VariableStore object
        """
        self.Reset()

        vstore[self.EventStart.ID] = self.EventStart
        vstore[self.EventEnd.ID] = self.EventEnd

        if self.Departure :
            self.Departure.AddVariables(vstore)

    # -----------------------------------------------------------------
    def AddConstraints(self, cstore) :
        """
        Add constraints for this event to the constraint store.

        Args:
            cstore -- TimedEventList.ConstraintStore object
        """
        constraint = OrderConstraint(self.EventStart.ID, self.EventEnd.ID, self.Duration)
        cstore.append(constraint)

        if self.Departure :
            self.Departure.AddConstraints(cstore)

    # -----------------------------------------------------------------
    def DumpToLog(self) :
        logger.warn("[{0:10s}]: {1:8s} from {2} to {3}".format(self.EventID, self.Details, str(self.EventStart), str(self.EventEnd)))
        if self.Departure :
            self.Departure.DumpToLog()

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class BackgroundEvent(PlaceEvent) :

    # -----------------------------------------------------------------
    @staticmethod
    def Create(details, base, sinterval, einterval, minduration = 0.01) :
        svar = MinimumIntervalVariable(base + sinterval[0], base + sinterval[1])
        evar = MaximumIntervalVariable(base + einterval[0], base + einterval[1])

        return BackgroundEvent(details, svar, evar, minduration)

    # -----------------------------------------------------------------
    def Split(self) :
        """
        For a background event Split() is really just make a copy
        of the current event
        """
        svar = self.BaseEventStart.Copy()
        evar = self.BaseEventEnd.Copy()

        return self.__class__(self.Details, svar, evar, self.Duration)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class PreEventEvent(PlaceEvent) :
    """
    PreEventEvent -- a PlaceEvent that occurs immediately before another
    event so start and stop intervals happen as late as possible.
    """

    # -----------------------------------------------------------------
    @staticmethod
    def Create(details, base, sinterval, einterval, minduration = 0.01) :
        svar = MaximumIntervalVariable(base + sinterval[0], base + sinterval[1])
        evar = MaximumIntervalVariable(base + einterval[0], base + einterval[1])

        return PreEventEvent(details, svar, evar, minduration)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class PostEventEvent(PlaceEvent) :
    """
    PostEventEvent -- a PlaceEvent that occurs immediately after another
    event so start and stop intervals happen as early as possible.
    """

    # -----------------------------------------------------------------
    @staticmethod
    def Create(details, base, sinterval, einterval, minduration = 0.01) :
        svar = MinimumIntervalVariable(base + sinterval[0], base + sinterval[1])
        evar = MinimumIntervalVariable(base + einterval[0], base + einterval[1])

        return PostEventEvent(details, svar, evar, minduration)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class VariableMiddleEvent(PlaceEvent) :
    """
    VariableEvent -- a PlaceEvent with a variable start time that
    generally occurs around the middle of its duration
    """

    # -----------------------------------------------------------------
    @staticmethod
    def Create(details, base, sinterval, einterval, minduration = 0.01) :
        svar = GaussianIntervalVariable(base + sinterval[0], base + sinterval[1])
        evar = GaussianIntervalVariable(base + einterval[0], base + einterval[1])

        return VariableMiddleEvent(details, svar, evar, minduration)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class AggregateDurationEvent(PlaceEvent) :

    # -----------------------------------------------------------------
    @staticmethod
    def Create(details, base, sinterval, einterval, minduration = 8.0, minsplit = 1.0) :
        svar = GaussianIntervalVariable(base + sinterval[0], base + sinterval[1])
        evar = GaussianIntervalVariable(base + einterval[0], base + einterval[1])

        work = AggregateDurationEvent(details, svar, evar, minduration)
        work.MinimumSplitDuration = minsplit

        return work

    # -----------------------------------------------------------------
    def __init__(self, details, stimevar, etimevar, duration = 0.01, ade_id = None) :
        PlaceEvent.__init__(self, details, stimevar, etimevar, duration, ade_id)

        self.MinimumSplitDuration = duration
        self.AggregateID = GenName('AGGREGATE')
        self.AggregateHead = True

    # -----------------------------------------------------------------
    def Split(self) :
        svar = self.BaseEventStart.Copy()
        evar = self.BaseEventEnd.Copy()

        event = self.__class__(self.Details, svar, evar, self.Duration)

        # propogate aggregate information
        event.MinimumSplitDuration = self.MinimumSplitDuration
        event.AggregateID = self.AggregateID
        event.AggregateHead = False

        return event

    # -----------------------------------------------------------------
    def _FindAggregateDuration(self) :
        evar = self.EventEnd
        total = self.Duration

        accum = 0
        travel = self.Departure
        while travel :
            accum += travel.Duration
            if travel.DstPlace.__class__ == self.__class__  and travel.DstPlace.AggregateID == self.AggregateID :
                evar = travel.DstPlace.EventEnd
                total += accum
                accum = 0
            else :
                accum += travel.DstPlace.Duration

            travel = travel.DstPlace.Departure

        return (evar, total)

    # -----------------------------------------------------------------
    def AddConstraints(self, cstore) :
        constraint = OrderConstraint(self.EventStart.ID, self.EventEnd.ID, self.MinimumSplitDuration)
        cstore.append(constraint)

        if self.AggregateHead :
            (evar, total) = self._FindAggregateDuration()
            constraint = OrderConstraint(self.EventStart.ID, evar.ID, total)
            cstore.append(constraint)

        if self.Departure :
            self.Departure.AddConstraints(cstore)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class TravelEvent :
    DefaultDuration = 0.5

    # -----------------------------------------------------------------
    def __init__(self, srcplace, dstplace, estimator = None, te_id = None) :
        self.SrcPlace = srcplace
        self.DstPlace = dstplace
        self.Duration = estimator.ComputeTravelTime(srcplace.Details, dstplace.Details) if estimator else self.DefaultDuration
        self.EventID = te_id or GenName('TRAVEL')

    # -----------------------------------------------------------------
    def AddVariables(self, vstore) :
        if self.DstPlace :
            self.DstPlace.AddVariables(vstore)

    # -----------------------------------------------------------------
    def AddConstraints(self, cstore) :
        if self.DstPlace :
            self.DstPlace.AddConstraints(cstore)

            constraint = OrderConstraint(self.SrcPlace.EventEnd.ID, self.DstPlace.EventStart.ID, self.Duration)
            cstore.append(constraint)

    # -----------------------------------------------------------------
    def DumpToLog(self) :
        logger.warn("[{0:10s}]: travel from {1} to {2}".format(self.EventID, self.SrcPlace.Details, self.DstPlace.Details))
        if self.DstPlace :
            self.DstPlace.DumpToLog()

