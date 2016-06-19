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

@file    LayoutInfo.py
@author  Mic Bowman
@date    2013-12-03

This file defines routines used to build features of a mobdat traffic
network such as building a grid of roads.

"""

import os, sys
import logging
from applications.mobdat.common.graph import Graph, LayoutDecoration, SocialDecoration,\
    SocialEdges

# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

import json

logger = logging.getLogger(__name__)


## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class WorldInfo(Graph.Graph) :

    # -----------------------------------------------------------------
    @staticmethod
    def LoadFromFile(filename) :
        with open(filename, 'r') as fp :
            data = json.load(fp)

        graph = WorldInfo()
        graph.Load(data)

        return graph

    # -----------------------------------------------------------------
    def __init__(self) :
        Graph.Graph.__init__(self)

        for dtype in LayoutDecoration.CommonDecorations :
            self.AddDecorationHandler(dtype)

        for dtype in SocialDecoration.CommonDecorations :
            self.AddDecorationHandler(dtype)

    # =================================================================
    # =================================================================

    # -----------------------------------------------------------------
    def AddPersonProfile(self, profile) :
        """
        Args:
            profile -- SocialNodes.PersonProfile
        """
        self.AddNode(profile)

    # -----------------------------------------------------------------
    def AddPerson(self, person) :
        """
        Args:
            person -- SocialNodes.Person
        """
        self.AddNode(person)

    # =================================================================
    # =================================================================

    # -----------------------------------------------------------------
    def AddBusinessProfile(self, profile) :
        """
        Args:
            profile -- object of type SocialNodes.BusinessProfile
        """
        self.AddNode(profile)

    # -----------------------------------------------------------------
    def AddBusiness(self, business) :
        """
        Args:
            business -- object of type SocialNodes.Business
        """

        self.AddNode(business)

    # -----------------------------------------------------------------
    def RelateDerivedProfiles(self, child, parent) :
        """
        Add an edge that captures the DerivedFrom relationship
        between two profiles

        Args:
            child -- object of type SocialNodes.*Profile
            parent -- object of type SocialNodes.*Profile
        """
        self.AddEdge(SocialEdges.DerivedFrom(child, parent))

    # =================================================================
    # =================================================================

    # -----------------------------------------------------------------
    def SetEmployer(self, person, business) :
        """
        Args:
            person -- object of type SocialNodes.Person
            business -- object of type SocialNodes.Business
        """

        self.AddEdge(SocialEdges.EmployedBy(person, business))

    # -----------------------------------------------------------------
    def SetResidence(self, entity, location) :
        """
        Args:
            entity -- object of type SocialNodes.Person or SocialNodes.Business
            location -- object of type LayoutNodes.BusinessLocation, LayoutNodes.ResidentialLocation or LayoutNodes.EndPoint
        """

        self.AddEdge(SocialEdges.ResidesAt(entity, location))

    # =================================================================
    # =================================================================

    # -----------------------------------------------------------------
    def AddEndPoint(self, endpoint) :
        """
        Args:
            endpoint -- object of type LayoutNodes.Intersection
        """
        self.AddNode(endpoint)

    # -----------------------------------------------------------------
    def AddLocationCapsule(self, capsule) :
        """
        Args:
            capsule -- object of type LayoutNodes.LocationCapsule
        """
        self.AddNode(capsule)

    # -----------------------------------------------------------------
    def AddBusinessLocationProfile(self, profile) :
        """
        Args:
            profile -- object of type LayoutNodes.BusinessLocationProfile
        """
        self.AddNode(profile)

    # -----------------------------------------------------------------
    def AddResidentialLocationProfile(self, profile) :
        """
        Args:
            profile -- object of type LayoutNodes.ResidentialLocationProfile
        """
        self.AddNode(profile)

    # -----------------------------------------------------------------
    def AddBusinessLocation(self, location) :
        """
        Args:
            location -- object of type LayoutNodes.BusinessLocation
        """
        self.AddNode(location)

    # -----------------------------------------------------------------
    def AddResidentialLocation(self, location) :
        """
        Args:
            location -- object of type LayoutNodes.ResidentialLocation
        """
        self.AddNode(location)

    # =================================================================
    # =================================================================

    # -----------------------------------------------------------------
    def AddIntersectionType(self, itype) :
        """
        Args:
            itype -- object of type LayoutNodes.IntersectionType
        """
        self.AddNode(itype)

    # -----------------------------------------------------------------
    def AddIntersection(self, intersection) :
        """
        Args:
            intersection -- object of type LayoutNodes.Intersection
        """
        self.AddNode(intersection)

    # =================================================================
    # =================================================================

    # -----------------------------------------------------------------
    def AddRoadType(self, roadtype) :
        """
        Args:
            roadtype -- object of type LayoutNodes.RoadType
        """
        self.AddNode(roadtype)

    # -----------------------------------------------------------------
    def AddRoad(self, road) :
        """
        Args:
            profile -- object of type LayoutEdges.Road
            etype -- object of type LayoutInfo.RoadType (Graph.Node)
        """
        self.AddEdge(road)
