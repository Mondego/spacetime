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

@file    BusinessInfo.py
@author  Mic Bowman
@date    2013-12-03

This file defines routines used to build features of a mobdat traffic
network such as building a grid of roads.

"""

import os, sys
import logging

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))

from applications.mobdat.common.ValueTypes import DaysOfTheWeek

logger = logging.getLogger(__name__)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class ScheduledEvent :

    # -----------------------------------------------------------------
    def __init__(self, day, stime, etime) :
        self.Day = day
        self.StartTime = stime
        self.EndTime = etime

    # -----------------------------------------------------------------
    @property
    def WorldStartTime(self) : return self.Day * 24.0 + self.StartTime

    # -----------------------------------------------------------------
    @property
    def WorldEndTime(self) : return self.Day * 24.0 + self.EndTime

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class WeeklySchedule :

    # -----------------------------------------------------------------
    @staticmethod
    def WorkWeekSchedule(stime, etime) :
        sched = [[] for _ in range(DaysOfTheWeek.Mon, DaysOfTheWeek.Sun + 1)]

        # work week
        for d in range(DaysOfTheWeek.Mon, DaysOfTheWeek.Sat) :
            sched[d].append((stime,etime))

        return WeeklySchedule(sched)

    # -----------------------------------------------------------------
    @staticmethod
    def FullWeekSchedule(stime, etime) :
        sched = [[] for _ in range(DaysOfTheWeek.Mon, DaysOfTheWeek.Sun + 1)]

        # work week
        for d in range(DaysOfTheWeek.Mon, DaysOfTheWeek.Sun + 1) :
            sched[d].append((stime,etime))

        return WeeklySchedule(sched)

    # -----------------------------------------------------------------
    @staticmethod
    def SpecialSchedule(**keywords) :
        sched = [[] for _ in range(DaysOfTheWeek.Mon, DaysOfTheWeek.Sun + 1)]

        for key in keywords :
            if isinstance(keywords[key], tuple) :
                sched[DaysOfTheWeek.__dict__[key]].append(keywords[key])
            elif isinstance(keywords[key], list) :
                sched[DaysOfTheWeek.__dict__[key]].extend(keywords[key])

        return WeeklySchedule(sched)

    # -----------------------------------------------------------------
    def __init__(self, schedule, offset = 0.0) :
        for d in range(DaysOfTheWeek.Mon, DaysOfTheWeek.Sun + 1) :
            self.__dict__[DaysOfTheWeek.KeyName[d]] = map(lambda x : (x[0] + offset, x[1] + offset), schedule[d])

    # -----------------------------------------------------------------
    def ScheduleForDay(self, day) :
        day = day % (DaysOfTheWeek.Sun + 1)
        return sorted(self.__dict__[DaysOfTheWeek.KeyName[day]], key= lambda sched: sched[0])

    # -----------------------------------------------------------------
    def ScheduledAtTime(self, day, time) :
        time = time % 24
        for sched in self.ScheduleForDay(day) :
            if sched[0] <= time and time <= sched[1] :
                return True

        return False

    # -----------------------------------------------------------------
    def NextScheduledEvent(self, worldtime) :
        day = int(worldtime / 24.0)
        time = worldtime % 24

        for iday in range(DaysOfTheWeek.Mon, DaysOfTheWeek.Sun + 1) :
            for sched in self.ScheduleForDay(iday + int(day)) :
                if sched[0] >= time : return ScheduledEvent(iday + int(day), sched[0], sched[1])
            time = 0.0          # start at the beginning of the next day

        return None

    # -----------------------------------------------------------------
    def Dump(self) :
        result = []
        for d in range(DaysOfTheWeek.Mon, DaysOfTheWeek.Sun + 1) :
            result.append(self.ScheduleForDay(d))
        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
if __name__ == '__main__' :
    sched = WeeklySchedule.WorkWeekSchedule(8.0, 17.0)
    ev = sched.NextScheduledEvent(0.0, 0.0)
    print "{0}, {1}, {2}".format(ev.Day, ev.StartTime, ev.EndTime)

    ev = sched.NextScheduledEvent(0.0, 12.0)
    print "{0}, {1}, {2}".format(ev.Day, ev.StartTime, ev.EndTime)

    ev = sched.NextScheduledEvent(6.0, 0.0)
    print "{0}, {1}, {2}".format(ev.Day, ev.StartTime, ev.EndTime)

    ev = sched.NextScheduledEvent(6.0, 12.0)
    print "{0}, {1}, {2}".format(ev.Day, ev.StartTime, ev.EndTime)

    ev = sched.NextScheduledEvent(600.0, 2.0)
    print "{0}, {1}, {2}".format(ev.Day, ev.StartTime, ev.EndTime)

    ev = sched.NextScheduledEvent(322.75690, 2.2334)
    print "{0}, {1}, {2}".format(ev.Day, ev.StartTime, ev.EndTime)
