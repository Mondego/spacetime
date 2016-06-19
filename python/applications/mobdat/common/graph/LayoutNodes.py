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

@file    LayoutNodes.py
@author  Mic Bowman
@date    2013-12-03

This file defines routines used to build features of a mobdat traffic
network such as building a grid of roads.

"""

import os, sys
import logging

# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

import Node, LayoutDecoration

logger = logging.getLogger(__name__)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class IntersectionType(Node.Node) :
    """
    The IntersectionType class is used to specify parameters for rendering
    intersections in Sumo and OpenSim.
    """

    # -----------------------------------------------------------------
    def __init__(self, name, itype, render) :
        """
        Args:
            name -- string
            itype -- string, indicates the stop light type for the intersection
            render -- boolean, flag to indicate that opensim should render the object
        """
        Node.Node.__init__(self, name = name)

        self.AddDecoration(LayoutDecoration.IntersectionTypeDecoration(name, itype, render))

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class Intersection(Node.Node) :

    # -----------------------------------------------------------------
    def __init__(self, name, itype, x, y) :
        """
        Args:
            name -- string
            itype -- object of type Layout.IntersectionType
            x, y -- integer coordinates
        """
        Node.Node.__init__(self, name = name)
        self.AddDecoration(LayoutDecoration.CoordDecoration(x, y))
        self.AddDecoration(LayoutDecoration.EdgeMapDecoration())
        itype.AddMember(self)


## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
##class EndPoint(Node.Node) :
class EndPoint(Intersection) :
    """
    EndPoint

    This graph node class (a subset of intersections) is the destination
    for a trip.

    Members: None

    Decorations:
        EndPointDecoration

    Edges: None
    """

    # -----------------------------------------------------------------
    def __init__(self, name, itype, x, y) :
        """
        Args:
            name -- string
            itype -- object of type Layout.IntersectionType
            x, y -- integer coordinates
        """
        Intersection.__init__(self, name, itype, x, y)
        self.AddDecoration(LayoutDecoration.EndPointDecoration())

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class LocationCapsule(Node.Node) :
    """
    LocationCapsule

    This graph node class manages a collection of EndPoint nodes.

    Members: EndPoints, typically one endpoint for a residential
    location and multiple endpoints for a business location

    Decorations:
        CapsuleDecoration

    Edges: None
    """

    # -----------------------------------------------------------------
    def __init__(self, name) :
        """
        Args:
            name -- string
            itype -- object of type Layout.IntersectionType
            x, y -- integer coordinates
        """
        Node.Node.__init__(self, name = name)
        self.AddDecoration(LayoutDecoration.CapsuleDecoration())

    # -----------------------------------------------------------------
    def AddEndPointToCapsule(self, endpoint) :
        """
        Args:
            endpoint -- object of type LayoutNodes.EndPoint
        """
        self.AddMember(endpoint)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class BusinessLocation(Node.Node) :
    """
    BusinessLocation

    This graph node class manages a business neighborhood consisting of
    a collection of LocationCapsule objects

    Members:- Typically one LocationCapsule nodes that contains multiple
    EndPoint nodes

    MemberOf:
        BusinessLocationProfile

    Decorations:
        BusinessLocationDecoration

    Edges: None
    """

    # -----------------------------------------------------------------
    def __init__(self, name, profile) :
        """
        Args:
            name -- string
            profile -- object of type BusinessLocationProfile
        """
        Node.Node.__init__(self, name = name)

        self.AddDecoration(LayoutDecoration.BusinessLocationDecoration())
        profile.AddMember(self)

    # -----------------------------------------------------------------
    def AddCapsuleToLocation(self, capsule) :
        """
        Args:
            capsule -- object of type LayoutNodes.LocationCapsule
        """
        self.AddMember(capsule)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class ResidentialLocation(Node.Node) :
    """
    ResidentialLocation

    This graph node class manages a residential neighborhood consisting of
    a collection of LocationCapsule objects

    Members: Typically several LocationCapsule nodes that each contain
    a single EndPoint node

    MemberOf:
        ResidentialLocationProfile

    Decorations:
        ResidentialLocationDecoration

    Edges: None
    """

    # -----------------------------------------------------------------
    def __init__(self, name, profile) :
        """
        Args:
            name -- string
            profile -- object of type ResidentialLocationProfile
        """
        Node.Node.__init__(self, name = name)

        self.AddDecoration(LayoutDecoration.ResidentialLocationDecoration())
        profile.AddMember(self)

    # -----------------------------------------------------------------
    def AddCapsuleToLocation(self, capsule) :
        """
        Args:
            capsule -- object of type LayoutNodes.LocationCapsule
        """
        self.AddMember(capsule)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class BusinessLocationProfile(Node.Node) :

    # -----------------------------------------------------------------
    def __init__(self, name, employees, customers, types) :
        """
        Args:
            name -- string
            employees -- integer, max number of employees per node
            customers -- integer, max number of customers per node
            types -- dict mapping Business.BusinessTypes to count
        """
        Node.Node.__init__(self, name = name)

        self.AddDecoration(LayoutDecoration.BusinessLocationProfileDecoration(employees, customers, types))

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class ResidentialLocationProfile(Node.Node) :

    # -----------------------------------------------------------------
    def __init__(self, name, residents) :
        """
        Args:
            residents -- integer, max number of residents per node
        """
        Node.Node.__init__(self, name = name)

        self.AddDecoration(LayoutDecoration.ResidentialLocationProfileDecoration(residents))

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class RoadType(Node.Node) :
    """
    The RoadType class is used to specify parameters for rendering roads
    in Sumo and OpenSim.
    """

    # -----------------------------------------------------------------
    def __init__(self, name, lanes, pri, speed, wid, sig, render, center) :
        """
        Args:
            name -- string
            lanes -- integer, number of lanes in the road
            pri -- integer, priority for stop lights
            speed -- float, maximum speed allowed on the road
            sig -- string, signature
            render -- boolean, flag to indicate whether opensim should render
            center -- boolean, flag to indicate the coordinate origin
        """
        Node.Node.__init__(self, name = name)

        self.AddDecoration(LayoutDecoration.RoadTypeDecoration(name, lanes, pri, speed, wid, sig, render, center))
