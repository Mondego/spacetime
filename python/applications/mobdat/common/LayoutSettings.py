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

@file    LayoutSettings.py
@author  Mic Bowman
@date    2013-12-03

This file defines classes used to store general information about the
traffic network being constructed. Each of the builder modules can tag
information in the nodes for later use in rezzing the network.

"""

import os, sys

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class IntersectionInfo :

    # -----------------------------------------------------------------
    def __init__(self, settings) :
        self.Name = settings["Name"]
        self.AssetID = settings["AssetID"]
        self.Padding = settings["Padding"]
        self.ZOffset = settings["ZOffset"]
        self.Signature = settings["Signature"]
        self.IntersectionTypes = settings["IntersectionTypes"]

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class RoadInfo :

    # -----------------------------------------------------------------
    def __init__(self, settings) :
        self.Name = settings["Name"]
        self.RoadTypes = settings["RoadTypes"]
        self.AssetID = settings["AssetID"]
        self.ZOffset = settings["ZOffset"]

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class VehicleTypeInfo :

    # -----------------------------------------------------------------
    def __init__(self, settings) :
        self.Name = settings["Name"]
        self.Description = settings["Description"]

        # Social connector attributes
        self.Rate = settings["Rate"]
        self.ProfileTypes = settings["ProfileTypes"]

        # Sumo connector attributes
        self.Acceleration = settings["Acceleration"]
        self.Deceleration = settings["Deceleration"]
        self.Sigma = settings["Sigma"]
        self.Length = settings["Length"]
        self.MinGap = settings["MinGap"]
        self.MaxSpeed = settings["MaxSpeed"]

        # OpenSim connector attributes
        self.AssetID = settings["AssetID"]
        self.StartParameter = settings.get("StartParameter","{}")

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class LayoutSettings :

    # -----------------------------------------------------------------
    def __init__(self, settings) :
        self.RoadTypes = {}
        self.IntersectionTypes = {}
        self.VehicleTypes = {}

        self.IntersectionTypeMap = {}
        self.RoadTypeMap = {}
        self.Cities = {}
        self.CityConnections = []

        self.ProcessSettings(settings)

    # -----------------------------------------------------------------
    def ProcessSettings(self, settings) :
        for rtype in settings["RoadTypes"] :
            rinfo = RoadInfo(rtype)
            self.RoadTypes[rinfo.Name] = rinfo

            for et in rinfo.RoadTypes :
                if et not in self.RoadTypeMap :
                    self.RoadTypeMap[et] = []
                self.RoadTypeMap[et].append(rinfo)

        for ntype in settings["IntersectionTypes"] :
            ninfo = IntersectionInfo(ntype)
            self.IntersectionTypes[ninfo.Name] = ninfo

            for nt in ninfo.IntersectionTypes :
                if nt not in self.IntersectionTypeMap :
                    self.IntersectionTypeMap[nt] = []
                self.IntersectionTypeMap[nt].append(ninfo)

        for vtype in settings["VehicleTypes"] :
            vinfo = VehicleTypeInfo(vtype)
            self.VehicleTypes[vinfo.Name] = vinfo

        for cities in settings["Cities"]:
            name = cities["Name"]
            offset = cities["Offset"]
            self.Cities[name] = {}
            self.Cities[name]["Offset"] = offset

        self.CityConnections = settings["CityConnections"]
