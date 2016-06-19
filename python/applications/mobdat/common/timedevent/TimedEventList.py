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

@file    TimedEventList.py
@author  Mic Bowman
@date    2014-03-31

This package defines modules for the mobdat simulation environment

"""

import os, sys, traceback
import logging

# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

import random
from applications.mobdat.common.TravelTimeEstimator import TravelTimeEstimator
from applications.mobdat.common.timedevent import TimedEvent, IntervalVariable

logger = logging.getLogger(__name__)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class IntervalVariableStore(dict) :

    # -----------------------------------------------------------------
    def __init__(self, *args, **kwargs) :
        dict.__init__(self, *args, **kwargs)

    # -----------------------------------------------------------------
    def Copy(self) :
        newlist = IntervalVariableStore()
        for tvar in self.itervalues() :
            newlist[tvar.ID] = tvar.Copy(tvar.ID)

        return newlist

    # -----------------------------------------------------------------
    def StoreIsValid(self) :
        """ Determine if the store is in a consistent state

        Returns:
            True if all variables are still valid
        """
        for var in self.itervalues() :
            if not var.IsValid() :
                logger.debug('variable {0} is inconsistent; {1}'.format(var.ID, str(var)))
                return False

        return True

    # -----------------------------------------------------------------
    def StoreIsFixed(self) :
        """ Determine if all variables in the store have fixed their values

        Returns:
            True if all variables are fixed
        """
        for var in self.itervalues() :
            if not var.IsFixed() : return False

        return True

    # -----------------------------------------------------------------
    def FindFreeVariables(self) :
        """ Find the time variables with values that have not been
        set. Ignore invalid variables.

        Returns:
            A possibly empty list of variable identifiers
        """
        variables = []
        for var in self.itervalues() :
            if not var.IsFixed() : variables.append(var)

        return sorted(variables, key= lambda var : var.Priority, reverse=True)

    # -----------------------------------------------------------------
    def DumpToLog(self) :
        for tvar in sorted(self.values(), key= lambda tvar : tvar.IntervalStart) :
            logger.warn("{0:5s} {1}".format(tvar.ID, str(tvar)))


# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class ConstraintStore(list) :

    # -----------------------------------------------------------------
    def __init__(self, *args) :
        list.__init__(self, args)

        self.LastPickedVariable = None

    # -----------------------------------------------------------------
    def DumpToLog(self, varstore) :
        for constraint in self :
            constraint.DumpToLog(varstore)

    # -----------------------------------------------------------------
    def ApplyConstraints(self, varstore) :
        """ Apply the list of constraints repeatedly until the variable
        space stabilizes. With float ranges there is some danger of this
        never stopping though that is unlikely.

        Returns:
            True if all constraints applied, False if there was a conflict
        """

        changed = True
        while changed :
            if not varstore.StoreIsValid() :
                return False

            changed = False
            for constraint in self :
                changed = constraint.Apply(varstore) or changed

        return varstore.StoreIsValid()

    # -----------------------------------------------------------------
    def SolveConstraints(self, varstore) :
        """ Apply constraints repeatedly until all variables have been given a value

        Args:
            varstore -- store of IntervalVariables over which constraints will be applied

        Returns:
            True if the variable store is valid after all variables have been given a value
        """
        if not self.ApplyConstraints(varstore) :
            logger.debug("resolution failed, no variable picked")
            return False

        variables = varstore.FindFreeVariables()
        for var in variables :
            var.PickValue()
            self.LastPickedVariable = var.ID

            # print "================================================================="
            # print "Pick variable {0} and set value to {1}".format(var.ID, var.IntervalStart)
            # print "================================================================="

            if not self.ApplyConstraints(varstore) :
                logger.debug("resolution failed, last picked variable is %s",self.LastPickedVariable)
                return False

        return True

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class TimedEventList :
    # -----------------------------------------------------------------
    def __init__(self, baseev, estimator = None) :
        """
        Args:
            baseev -- initialized PlaceEvent object that sets limits on time scope
        """
        self.Events = {}
        self.TravelTimeEstimator = estimator or TravelTimeEstimator()

        baseid = self.AddPlaceEvent(baseev)

        self.BaseEvent = self.Events[baseid]

    # -----------------------------------------------------------------
    @property
    def LastEvent(self) :
        event = self.BaseEvent
        while event.Departure :
            event = event.Departure.DstPlace

        return event

    # -----------------------------------------------------------------
    def FindEvents(self, pred) :
        result = []

        event = self.BaseEvent
        if pred(event) : result.append(event)

        while event.Departure :
            event = event.Departure.DstPlace
            if pred(event) : result.append(event)

        return result

    # -----------------------------------------------------------------
    def PrevPlaceID(self, eventid) :
        ev = self.Events[eventid].PrevPlace()
        return ev.EventID if ev else None

    # -----------------------------------------------------------------
    def NextPlaceID(self, eventid) :
        ev = self.Events[eventid].NextPlace()
        return ev.EventID if ev else None

    # -----------------------------------------------------------------
    def MoreTripEvents(self) :
        return self.BaseEvent.NextPlace()

    # -----------------------------------------------------------------
    def PopTripEvent(self) :
        stime = self.BaseEvent.EventEnd
        splace = self.BaseEvent
        dplace = self.BaseEvent.NextPlace()

        if not dplace :
            return None

        self.BaseEvent = dplace
        return TimedEvent.TripEvent(stime, splace, dplace)

    # -----------------------------------------------------------------
    def AddPlaceEvent(self, event) :
        """ Create a PlaceEvent object from the parameters and save it in the list of events

        Args:
            event -- initialized PlaceEvent object
        Returns:
            The identifier of the newly created event
        """

        self.Events[event.EventID] = event
        return event.EventID

    # -----------------------------------------------------------------
    def InsertAfterPlaceEvent(self, id1, id2) :
        """Insert PlaceEvent id2 after the event id1. Create a travel event to move
        from the current location to the new one.

        Args:
            id1 -- string event identifier
            id2 -- string event identifier
        """

        ev1 = self.Events[id1]
        ev2 = self.Events[id2]

        if ev1.Departure :
            ev2.Departure = TimedEvent.TravelEvent(ev2, ev1.Departure.DstPlace, estimator = self.TravelTimeEstimator)

        ev1.Departure = TimedEvent.TravelEvent(ev1, ev2, estimator = self.TravelTimeEstimator)
        ev2.Arrival = ev1.Departure

        t1 = max(ev1.BaseEventStart.IntervalStart, ev2.BaseEventStart.IntervalStart)
        t2 = max(ev1.BaseEventStart.IntervalEnd, ev2.BaseEventStart.IntervalEnd)

        ev1.EventEnd = IntervalVariable.MaximumIntervalVariable(t1, t2, ev1.BaseEventEnd.ID)

        return (id1, id2)

    # -----------------------------------------------------------------
    def InsertWithinPlaceEvent(self, idprev, idnew) :
        """Split event idprev and insert idnew into the middle. Create travel events to
        move from the current location to the new location and then back to the current location.
        The assumption is that self.BaseEventStart.IntervalStart < place.BaseEventStart.IntervalStart and
        place.EventEnd.IntervalEnd < self.EventEnd.IntervalEnd

        Args:
            idprev -- string event identifier
            idnew -- string event identifier
        """
        evprev = self.Events[idprev]
        evnew = self.Events[idnew]

        # this is really wrong, there should be a constraint across the two intervals
        # that ensures that the duration is consistent...
        # oldduration = evprev.Duration
        # if oldduration > 0.01 :
        #     evprev.Duration = oldduration / 2.0

        evnext = evprev.Split()
        idnext = self.AddPlaceEvent(evnext)

        # connect the next events destination to the previous events destination
        if evprev.Departure :
            evnext.Departure = TimedEvent.TravelEvent(evnext, evprev.Departure.DstPlace, estimator = self.TravelTimeEstimator)

        # connect the previous event to the new event
        evnew.Arrival = evprev.Departure = TimedEvent.TravelEvent(evprev, evnew, estimator = self.TravelTimeEstimator)

        # connect the new event to the next event
        evnext.Arrival = evnew.Departure = TimedEvent.TravelEvent(evnew, evnext, estimator = self.TravelTimeEstimator)

        # abut the start of the next event to the new event
        evnext.BaseEventStart = IntervalVariable.MinimumIntervalVariable(evnew.BaseEventEnd.IntervalStart, evprev.BaseEventEnd.IntervalEnd)

        # abut the end of the previous event to the new event
        evprev.BaseEventEnd = IntervalVariable.MaximumIntervalVariable(evprev.BaseEventStart.IntervalStart, evnew.BaseEventStart.IntervalEnd)

        return (idprev, idnew, idnext)

    # -----------------------------------------------------------------
    def SolveConstraints(self) :
        # create the variable store for the events
        vstore = IntervalVariableStore()
        self.BaseEvent.AddVariables(vstore)

        # create all the constraints for the events
        cstore = ConstraintStore()
        self.BaseEvent.AddConstraints(cstore)

        # and now solve the whole mess
        return cstore.SolveConstraints(vstore)

    # -----------------------------------------------------------------
    def DumpToLogIntervalVariables(self) :
        vstore = IntervalVariableStore()
        self.BaseEvent.AddVariables(vstore)
        vstore.DumpToLog()

    # -----------------------------------------------------------------
    def DumpToLog(self) :
        self.BaseEvent.DumpToLog()

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
if __name__ == '__main__' :

    from applications.mobdat.common.timedevent.TimedEvent import BackgroundEvent

    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    def AddWorkEvent(evlist, event, days) :
        workEV = TimedEvent.AggregateDurationEvent.Create('work', days * 24.0, (6.0, 9.0), (14.0, 17.0), 9.0)
        workID = evlist.AddPlaceEvent(workEV)
        evlist.InsertWithinPlaceEvent(event, workID)

        return workID

    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    def AddCoffeeBeforeWorkEvent(evlist, workevent, days) :
        """Add a PlaceEvent for coffee before a work event. This moves the
        coffee event as close as possible to the work event.
        """

        event = TimedEvent.PreEventEvent.Create('coffee', days * 24.0, (0.0, 24.0), (0.0, 24.0), 0.2)
        idc = evlist.AddPlaceEvent(event)

        evlist.InsertAfterPlaceEvent(evlist.PrevPlaceID(workevent), idc)

        return idc

    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    def AddLunchToWorkEvent(evlist, workevent, days) :
        event = TimedEvent.VariableMiddleEvent.Create('lunch', days * 24.0, (11.5, 13.0), (12.5, 14.0), 0.75)
        idl = evlist.AddPlaceEvent(event)

        evlist.InsertWithinPlaceEvent(workevent, idl)

        return idl

    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    def AddRestaurantAfterWorkEvent(evlist, workevent, day) :
        event = TimedEvent.PostEventEvent.Create('dinner', day * 24.0, (0.0, 24.0), (0.0, 24.0), 1.5)
        idr = evlist.AddPlaceEvent(event)

        evlist.InsertAfterPlaceEvent(workevent, idr)

        return idr

    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    def AddShoppingTrip(evlist, day, maxcount = 4, prevevent = None) :
        # happens between 7am and 10pm

        if prevevent :
            ids = prevevent
        else :
            event = TimedEvent.VariableMiddleEvent.Create('shopping', day * 24.0, (7.0, 22.0), (7.0, 22.0), 0.75)
            ids = evlist.AddPlaceEvent(event)
            evlist.InsertWithinPlaceEvent(evlist.LastEvent.EventID, ids)

        stops = int(random.triangular(0, 4, 1))
        while stops > 0 :
            stops = stops - 1

            postev = TimedEvent.PostEventEvent.Create('shopping', day * 24.0, (7.0, 22.0), (7.0, 22.0), 0.5)
            idnew = evlist.AddPlaceEvent(postev)
            evlist.InsertAfterPlaceEvent(ids, idnew)
            ids = idnew

    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    ## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    def BuildOneDay(evlist, day) :
        lastev = evlist.LastEvent.EventID
        workev = AddWorkEvent(evlist, lastev, day)

        if random.uniform(0.0, 1.0) > 0.6 :
            workev = evlist.FindEvents(lambda ev : ev.Details == 'work')[-1].EventID
            AddCoffeeBeforeWorkEvent(evlist, workev, day)

        if random.uniform(0.0, 1.0) > 0.8 :
            workev = evlist.FindEvents(lambda ev : ev.Details == 'work')[-1].EventID
            AddLunchToWorkEvent(evlist, workev, day)

        if random.uniform(0.0, 1.0) > 0.8 :
            workev = evlist.FindEvents(lambda ev : ev.Details == 'work')[-1].EventID
            dinnerev = AddRestaurantAfterWorkEvent(evlist, workev, day)
            if random.uniform(0.0, 1.0) > 0.7 :
                AddShoppingTrip(evlist, day, maxcount = 2, prevevent = dinnerev)
        else :
            if random.uniform(0.0, 1.0) > 0.9 :
                AddShoppingTrip(evlist, day)

    # -----------------------------------------------------------------
    clog = logging.StreamHandler()
    clog.setFormatter(logging.Formatter('>>> [%(name)s] %(message)s'))
    clog.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(clog)


    for day in range(0, 1000) :
        homeev = BackgroundEvent.Create('home', 0.0, (0.0, 0.0), (24.0 * 1000.0, 24.0 * 1000.0))
        evlist = TimedEventList(homeev)

        print '---------- day = {0:4} ----------'.format(day)
        BuildOneDay(evlist, 0.0)
        # BuildOneDay(evlist, day)

        resolved = False
        try :
            resolved = evlist.SolveConstraints()
        except :
            logger.error('internal inconsistency detected; %s', traceback.format_exc(10))
            evlist.DumpToLog()
            sys.exit(1)

        if not resolved :
            logger.error('resolution failed for day %s', day)
            evlist.DumpToLog()
            sys.exit(1)

        while evlist.MoreTripEvents() :
            trip = evlist.PopTripEvent()
            print str(trip)

    # evlist.DumpToLog()

