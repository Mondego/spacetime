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

@file    SocialDecoration.py
@author  Mic Bowman
@date    2013-12-03

This file defines routines used to build features of a mobdat traffic
network such as building a grid of roads.

"""

import os, sys

# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

from applications.mobdat.common.ValueTypes import MakeEnum, DaysOfTheWeek
from applications.mobdat.common.Schedule import WeeklySchedule
from Decoration import Decoration
import SocialNodes

import random

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
BusinessType = MakeEnum('Unknown', 'Factory', 'Service', 'Civic', 'Entertainment', 'School', 'Retail', 'Food')

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class JobDescription :

    # -------------------------------------------------------
    @staticmethod
    def Load(pinfo) :
        profilename = pinfo['Name']
        salary = pinfo['Salary']
        flexible = pinfo['FlexibleHours']
        schedule = WeeklySchedule(pinfo['Schedule'])

        return JobDescription(profilename, salary, flexible, schedule)

    # -------------------------------------------------------
    def __init__(self, name, salary, flexible, schedule) :
        self.Name = name
        self.Salary = salary
        self.FlexibleHours = flexible
        self.Schedule = schedule

    # -------------------------------------------------------
    def Copy(self, offset = 0.0) :
        return JobDescription(self.Name, self.Salary, self.FlexibleHours, WeeklySchedule(self.Schedule.Dump(), offset))

    # -------------------------------------------------------
    def Dump(self) :
        result = dict()
        result['Name'] = self.Name
        result['Salary'] = self.Salary
        result['FlexibleHours'] = self.FlexibleHours
        result['Schedule'] = self.Schedule.Dump()

        return result


## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EmploymentProfileDecoration(Decoration) :
    DecorationName = 'EmploymentProfile'

    # -------------------------------------------------------
    @staticmethod
    def Load(graph, pinfo) :
        joblist = dict()
        for jobinfo in pinfo['JobList'] :
            joblist[JobDescription.Load(jobinfo['Job'])] = jobinfo['Demand']

        return EmploymentProfileDecoration(joblist)

    # -------------------------------------------------------
    def __init__(self, joblist) :
        """
        Args:
            joblist -- dictionary mapping JobDescription --> Demand
        """

        Decoration.__init__(self)

        self.JobList = dict()
        for job, demand in joblist.iteritems() :
            self.JobList[job.Copy()] = demand

    # -------------------------------------------------------
    def ScaleProfile(self, scale = 1.0, offset = 0.0) :
        """
        """

        joblist = dict()
        for job, demand in self.JobList.iteritems() :
            d = int(demand * scale)
            joblist[job.Copy(offset)] = d if d > 0 else 1

        return joblist

    # -------------------------------------------------------
    def PeakEmployeeCount(self, day = DaysOfTheWeek.Mon) :
        """
        Compute the peak hourly employee count over the
        course of a day.

        day -- DaysOfTheWeek enum
        """

        # this is the *ugliest* worst performing version of this computation
        # i can imagine. just dont feel the need to do anything more clever
        # right now
        peak = 0
        for hour in range(0, 24) :
            count = 0
            for job, demand in self.JobList.iteritems() :
                count += demand if job.Schedule.ScheduledAtTime(day, hour) else 0

            peak = count if peak < count else peak

        return peak

    # -------------------------------------------------------
    def Dump(self) :
        result = Decoration.Dump(self)

        result['JobList'] = []
        for job, demand in self.JobList.iteritems() :
            result['JobList'].append({ 'Job' : job.Dump(), 'Demand' : demand})

        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class ServiceProfileDecoration(Decoration) :
    DecorationName = 'ServiceProfile'

    # -------------------------------------------------------
    @staticmethod
    def Load(graph, pinfo) :
        bizhours = WeeklySchedule(pinfo['Schedule'])
        capacity = pinfo['CustomerCapacity']
        servicetime = pinfo['ServiceTime']

        return ServiceProfileDecoration(bizhours, capacity, servicetime)

    # -------------------------------------------------------
    def __init__(self, bizhours, capacity, servicetime) :
        """
        Args:
            bizhours -- object of type WeeklySchedule
            capacity -- integer maximum customer capacity
            servicetime -- float mean time to service a customer
        """
        Decoration.__init__(self)

        self.Schedule = bizhours
        self.CustomerCapacity = capacity
        self.ServiceTime = servicetime

    # -------------------------------------------------------
    def PeakServiceCount(self, days = None) :
        """
        Compute the peak number of customers expected during the
        day. Given that the duration of visits impacts this, the
        number is really a conservative guess.

        days -- list of DaysOfTheWeek
        """

        if not days :
            days = range(DaysOfTheWeek.Mon, DaysOfTheWeek.Sun + 1)

        # this is the *ugliest* worst performing version of this computation
        # i can imagine. just dont feel the need to do anything more clever
        # right now, note that since capacity is a constant its even dumber
        # since we know it will either be capacity or 0
        peak = 0
        for day in days :
            for hour in range(0, 24) :
                if self.Schedule.ScheduledAtTime(day, hour) :
                    count = self.CustomerCapacity
                    peak = count if peak < count else peak

        return peak

    # -------------------------------------------------------
    def Dump(self) :
        result = Decoration.Dump(self)
        result['CustomerCapacity'] = self.CustomerCapacity
        result['ServiceTime'] = self.ServiceTime
        result['Schedule'] = self.Schedule.Dump()

        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class BusinessProfileDecoration(Decoration) :
    DecorationName = 'BusinessProfile'

    # -------------------------------------------------------
    @staticmethod
    def BusinessTypePred(biztype, bizclass) :
        return lambda node : node.BusinessProfile.BusinessType == biztype and node.BusinessProfile.TestAnnotation(bizclass)

    # -------------------------------------------------------
    @staticmethod
    def FindByType(graph, biztype, bizclass) :
        predicate = BusinessProfileDecoration.BusinessTypePred(biztype, bizclass)
        return graph.FindNodes(nodetype = SocialNodes.Business.__name__, predicate = predicate)

    # -------------------------------------------------------
    @staticmethod
    def Load(graph, pinfo) :
        return BusinessProfileDecoration(pinfo['BusinessType'], pinfo['Annotations'])

    # -------------------------------------------------------
    def __init__(self, biztype, annotations = None) :
        """
        The business profile class captures the structure of a
        generic business pattern. The profile can be used to
        create specific businesses that match the pattern.

        biztype -- BusinessType enum
        annotations -- List of string annotations for the profile
        """

        self.BusinessType = biztype
        self.Annotations = {}

        if annotations :
            for word in annotations :
                self.Annotations[word] = True

    # -------------------------------------------------------
    def AddAnnotation(self, word) :
        self.Annotations[word] = True

    # -------------------------------------------------------
    def RemAnnotation(self, word) :
        if word in self.Annotations :
            del self.Annotations[word]

    # -------------------------------------------------------
    def TestAnnotation(self, word) :
        return not word or word in self.Annotations

    # -------------------------------------------------------
    def Dump(self) :
        result = Decoration.Dump(self)
        result['Annotations'] = self.Annotations.keys()
        result['BusinessType'] = self.BusinessType

        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class JobDescriptionDecoration(Decoration) :
    DecorationName = 'JobDescription'

    # -----------------------------------------------------------------
    @staticmethod
    def Load(graph, info) :
        job = JobDescription.Load(info['Job'])
        return JobDescriptionDecoration(job)

    # -----------------------------------------------------------------
    def __init__(self, job) :
        """
        Args:
            job -- object of type JobDescription
        """
        Decoration.__init__(self)
        self.JobDescription = job.Copy()

    # -----------------------------------------------------------------
    def __getattr__(self, name) :
        return getattr(self.JobDescription, name)

    # -----------------------------------------------------------------
    def Dump(self) :
        result = Decoration.Dump(self)
        result['Job'] = self.JobDescription.Dump()

        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class VehicleTypeDecoration(Decoration) :
    DecorationName = 'VehicleType'

    # -----------------------------------------------------------------
    @staticmethod
    def Load(graph, info) :
        decr = VehicleTypeDecoration()
        for name, rate in info['VehicleTypeMap'].iteritems() :
            decr.AddVehicleType(name, rate)

        return decr

    # -----------------------------------------------------------------
    def __init__(self) :
        """
        Args:
        """
        Decoration.__init__(self)

        self.VehicleTypeMap = {}
        self.VehicleTypeList = None

    # -----------------------------------------------------------------
    def AddVehicleType(self, name, rate) :
        """
        Args:
            name -- string, name of a vehicle type defined in layoutsettings
            rate -- relative frequency of occurance
        """
        self.VehicleTypeMap[name] = rate
        self.VehicleTypeList = None  # so we'll rebuild the map when needed

    # -----------------------------------------------------------------
    def PickVehicleType(self) :
        if not self.VehicleTypeList :
            self.VehicleTypeList = []
            for name, rate in self.VehicleTypeMap.iteritems() :
                self.VehicleTypeList.extend([name for _ in range(rate)])

        return random.choice(self.VehicleTypeList)

    # -----------------------------------------------------------------
    def Dump(self) :
        result = Decoration.Dump(self)
        result['VehicleTypeMap'] = self.VehicleTypeMap

        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class VehicleDecoration(Decoration) :
    DecorationName = 'Vehicle'

    # -----------------------------------------------------------------
    @staticmethod
    def Load(graph, info) :
        return VehicleDecoration(info['VehicleName'], info['VehicleType'])

    # -----------------------------------------------------------------
    def __init__(self, name, vehicletype) :
        """
        Args:
            name -- string, the name of the vehicle
            vehicletype -- string, the name of the vehicle type
        """
        Decoration.__init__(self)

        self.VehicleName = name
        self.VehicleType = vehicletype

    # -----------------------------------------------------------------
    def Dump(self) :
        result = Decoration.Dump(self)
        result['VehicleName'] = self.VehicleName
        result['VehicleType'] = self.VehicleType

        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class PreferenceDecoration(Decoration) :
    DecorationName = 'Preference'

    # -----------------------------------------------------------------
    @staticmethod
    def Load(graph, info) :
        decr = PreferenceDecoration()
        for name, weight in info['PreferenceMap'].iteritems() :
            decr.SetWeight(name, weight)

        return decr

    # -----------------------------------------------------------------
    def __init__(self) :
        """
        Args:
        """
        Decoration.__init__(self)

        self.PreferenceMap = {}

    # -----------------------------------------------------------------
    def SetWeight(self, name, weight) :
        if weight < 0 or 1 < weight :
            raise ValueError('invalid preference weight; %f' % weight)

        self.PreferenceMap[name] = weight
        return self.PreferenceMap[name]

    # -----------------------------------------------------------------
    def AddWeight(self, name, weight) :
        if weight < 0 or 1 < weight :
            raise ValueError('invalid preference weight')

        if name not in self.PreferenceMap :
            self.PreferenceMap[name] = 0

        # a relatively simple cuumulative weighting algorithm
        self.PreferenceMap[name] = self.PreferenceMap[name] + weight - self.PreferenceMap[name] * weight
        return self.PreferenceMap[name]

    # -----------------------------------------------------------------
    def GetWeight(self, name, defaultweight = None) :
        return self.PreferenceMap.get(name, defaultweight)

    # -----------------------------------------------------------------
    def Dump(self) :
        result = Decoration.Dump(self)
        result['PreferenceMap'] = self.PreferenceMap

        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
CommonDecorations = [ EmploymentProfileDecoration, ServiceProfileDecoration, BusinessProfileDecoration,
                      JobDescriptionDecoration,
                      VehicleTypeDecoration, VehicleDecoration,
                      PreferenceDecoration ]

