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

@file    OpenSimBuilder.py
@author  Mic Bowman
@date    2013-12-03

This file defines the opensim builder class for mobdat traffic networks.
The functions in this file will rez a mobdat network in an OpenSim region.
"""
from __future__ import absolute_import

import os, sys
import logging
from datamodel.mobdat.datamodel import Road, Person, VehicleInfo, JobDescription,\
    BusinessNode, Capsule, ResidentialNode, SimulationNode
from datamodel.common.datamodel import Vector3
from common.converter import create_jsondict

from applications.mobdat.common.graph.LayoutDecoration import EdgeMapDecoration
from applications.mobdat.common.graph import Edge
from applications.mobdat.common.Utilities import AuthByUserName, GenCoordinateMap,\
    CalculateOSCoordinates, CalculateOSCoordinatesFromOrigin
import json

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class DataBuilder :
    # -----------------------------------------------------------------
    def __init__(self, settings, world, laysettings) :
        self.Logger = logging.getLogger(__name__)
        self.employedby = {}
        self.livesat = {}

        self.World = world
        self.LayoutSettings = laysettings
        self.DataFolder = None

        self.RoadMap = {}
        self.NodeMap = {}

        self.cadis_roads = []

        try :
            self.RegionMap = GenCoordinateMap(settings)
            woffs = settings["OpenSimConnector"]["BuildOffset"]
            self.BuildOffsetX = woffs[0]
            self.BuildOffsetY = woffs[1]

            rsize = settings["OpenSimConnector"]["RegionSize"]
            self.RegionSizeX = rsize[0]
            self.RegionSizeY = rsize[1]

            self.WorldScale = settings["OpenSimConnector"].get("Scale",0.5)

            self.DataFolder = settings["General"]["Data"]
        except NameError as detail:
            self.Logger.warn("Failed processing OpenSim configuration; name error %s", (str(detail)))
            sys.exit(-1)
        except KeyError as detail:
            self.Logger.warn("unable to locate OpenSim configuration value for %s", (str(detail)))
            sys.exit(-1)
        except :
            exctype, value =  sys.exc_info()[:2]
            self.Logger.warn('handler failed with exception type %s; %s', exctype, str(value))
            sys.exit(-1)

    # -----------------------------------------------------------------
    def ComputeRotation(self, sig1, sig2) :
        for i in range(4) :
            success = True
            for j in range(4) :
                if sig1[j] != sig2[(i + j) % 4] and sig2[(i + j) % 4] != '*/*' :
                    success = False
                    break

            if success :
                return i

        return -1

    # -----------------------------------------------------------------
    def ComputeLocation(self, snode, enode) :
        if snode.Name not in self.NodeMap :
            self.Logger.warn('cannot find node %s in the node map' % (snode.Name))
            return False

        if enode.Name not in self.NodeMap :
            self.Logger.warn('cannot find node %s in the node map' % (enode.Name))
            return False

        #sbump = self.NodeMap[snode.Name].Padding
        #ebump = self.NodeMap[enode.Name].Padding

        deltax = enode.Coord.X - snode.Coord.X
        deltay = enode.Coord.Y - snode.Coord.Y

        swidths = snode.EdgeMap.Widths(scale=self.WorldScale)
        ewidths = enode.EdgeMap.Widths(scale=self.WorldScale)

        # west
        if deltax < 0 and deltay == 0 :
            sbump = max(swidths[EdgeMapDecoration.SOUTH], swidths[EdgeMapDecoration.NORTH]) / 2.0
            ebump = max(ewidths[EdgeMapDecoration.SOUTH], ewidths[EdgeMapDecoration.NORTH]) / 2.0
            s1x = snode.Coord.X - sbump
            s1y = snode.Coord.Y
            e1x = enode.Coord.X + ebump
            e1y = enode.Coord.Y

        # north
        elif deltax == 0 and deltay > 0 :
            sbump = max(swidths[EdgeMapDecoration.EAST], swidths[EdgeMapDecoration.WEST]) / 2.0
            ebump = max(ewidths[EdgeMapDecoration.EAST], ewidths[EdgeMapDecoration.WEST]) / 2.0
            s1x = snode.Coord.X
            s1y = snode.Coord.Y + sbump
            e1x = enode.Coord.X
            e1y = enode.Coord.Y - ebump

        # east
        elif deltax > 0 and deltay == 0 :
            sbump = max(swidths[EdgeMapDecoration.SOUTH], swidths[EdgeMapDecoration.NORTH]) / 2.0
            ebump = max(ewidths[EdgeMapDecoration.SOUTH], ewidths[EdgeMapDecoration.NORTH]) / 2.0
            s1x = snode.Coord.X + sbump
            s1y = snode.Coord.Y
            e1x = enode.Coord.X - ebump
            e1y = enode.Coord.Y

        # south
        elif deltax == 0 and deltay < 0 :
            sbump = max(swidths[EdgeMapDecoration.EAST], swidths[EdgeMapDecoration.WEST]) / 2.0
            ebump = max(ewidths[EdgeMapDecoration.EAST], ewidths[EdgeMapDecoration.WEST]) / 2.0
            s1x = snode.Coord.X
            s1y = snode.Coord.Y - sbump
            e1x = enode.Coord.X
            e1y = enode.Coord.Y + ebump

        else :
            self.Logger.warn('something went wrong computing the signature')
            return(0,0,0,0)

        return CalculateOSCoordinatesFromOrigin(s1x + self.BuildOffsetX, s1y + self.BuildOffsetY, e1x + self.BuildOffsetX, e1y + self.BuildOffsetY, self)

    # -----------------------------------------------------------------
    def PushNetworkToFile(self) :
        self.CreateNodes()
        self.CreateRoads()

    # -----------------------------------------------------------------
    def CreateRoads(self) :
        cadis_roads = []
        for rname, road in self.World.IterEdges(edgetype = 'Road') :
            if rname in self.RoadMap :
                continue

            if road.RoadType.Name not in self.LayoutSettings.RoadTypeMap :
                self.Logger.warn('Failed to find asset for %s' % (road.RoadType.Name))
                continue

            # check to see if we need to render this road at all
            if road.RoadType.Render :
                zoff = self.LayoutSettings.RoadTypeMap[road.RoadType.Name][0].ZOffset

                (p1x, p1y),(p2x, p2y),scene = self.ComputeLocation(road.StartNode, road.EndNode)
                # Create CADIS road objects for run-time usage
                cadis_road = Road()
                cadis_road.StartingPoint = Vector3(p1x, p1y, zoff)
                cadis_road.EndPoint = Vector3(p2x, p2y, zoff)
                cadis_road.Width = road.RoadType.TotalWidth(scale=self.WorldScale)
                cadis_road.Type = road.RoadType.Dump()
                cadis_roads.append(create_jsondict(cadis_road))

            # build the map so that we do render the reverse roads
            self.RoadMap[Edge.GenEdgeName(road.StartNode, road.EndNode)] = True
            self.RoadMap[Edge.GenEdgeName(road.EndNode, road.StartNode)] = True

        if self.DataFolder and cadis_roads:
            f = open(os.path.join(self.DataFolder,"roads.js"), "w")
            f.write(json.dumps(cadis_roads))
            f.close()

    # -----------------------------------------------------------------
    def CreatePerson(self, name, node, list_nodes):
        person = Person()
        person.Vehicle = VehicleInfo(node.Vehicle.VehicleName, node.Vehicle.VehicleType)
        jobd = node.JobDescription.JobDescription
        person.JobDescription = JobDescription(jobd.Salary, jobd.FlexibleHours, jobd.Schedule.__dict__)
        person.Preference = node.Preference.PreferenceMap
        person.Name = name
        if name in self.livesat:
            person.LivesAt = self.livesat[name]
        if name in self.employedby:
            person.EmployedBy = self.employedby[name]
        list_nodes.append(create_jsondict(person))

    # -----------------------------------------------------------------
    def CreateNode(self, name, node, list_nodes) :
        tname = node.IntersectionType.Name
        sig1 = node.EdgeMap.Signature()

        if tname not in self.LayoutSettings.IntersectionTypeMap :
            self.Logger.warn('Unable to locate node type %s' % (tname))
            return

        success = False
        for itype in self.LayoutSettings.IntersectionTypeMap[tname] :
            sig2 = itype.Signature

            rot = self.ComputeRotation(sig1, sig2)
            if rot >= 0 :
                self.NodeMap[name] = itype

                (p1x,p1y),sim = CalculateOSCoordinates(node.Coord.X + self.BuildOffsetX, node.Coord.Y + self.BuildOffsetY, self)
                p1z = itype.ZOffset
                if node.NodeType.Name == "EndPoint":
                    if "BusinessLocation" in node.InheritedDecorations:
                        busloc = node.BusinessLocation.HostObject
                        capsule = busloc.Capsule
                        simnode = BusinessNode()
                        simnode.Rezcap = Capsule()
                        simnode.Rezcap.SourceName = capsule.SourceName
                        simnode.Rezcap.DestinationName = capsule.DestinationName
                        simnode.PeakCustomerCount = node.BusinessLocation.PeakCustomerCount
                        simnode.PeakEmployeeCount = node.BusinessLocation.PeakEmployeeCount
                        simnode.CustomersPerNode = node.BusinessLocation.HostObject.BusinessLocationProfile.CustomersPerNode
                        simnode.EmployeesPerNode = node.BusinessLocation.HostObject.BusinessLocationProfile.EmployeesPerNode
                        simnode.PreferredBusinessTypes = node.BusinessLocation.HostObject.BusinessLocationProfile.PreferredBusinessTypes
                        for edge in busloc.InputEdges:
                            if edge.NodeType.Name == "ResidesAt":
                                bnode = edge.StartNode
                                if "EmploymentProfile" in bnode.Decorations:
                                    ep = bnode.EmploymentProfile.HostObject
                                    for wedge in ep.InputEdges:
                                        self.employedby[wedge.StartNode.Name] = name
                    elif "ResidentialLocation" in node.InheritedDecorations:
                        resloc = node.ResidentialLocation.HostObject
                        capsule = resloc.Capsule
                        simnode = ResidentialNode()
                        simnode.Rezcap = Capsule()
                        simnode.Rezcap.SourceName = capsule.SourceName
                        simnode.Rezcap.DestinationName = capsule.DestinationName
                        simnode.ResidentsPerNode = resloc.ResidentialLocationProfile.ResidentsPerNode
                        simnode.ResidentCount = node.ResidentialLocation.ResidentCount
                        simnode.ResidenceList = []
                        for edge in resloc.InputEdges:
                            if edge.NodeType.Name == "ResidesAt":
                                simnode.ResidenceList.append(edge.StartNode.Name)
                                self.livesat[edge.StartNode.Name] = name

                else:
                    # Create CADIS node objects for run-time usage
                    simnode = SimulationNode()

                simnode.Name = name
                simnode.Center = Vector3(p1x, p1y, p1z)
                simnode.Angle = 90.0 * rot
                simnode.Width = node.EdgeMap.Widths(scale=self.WorldScale)
                list_nodes.append(create_jsondict(simnode))
                success = True
            else:
                self.Logger.warn("Rotation < 0 for node %s", node)

        if not success :
            self.NodeMap[name] = self.LayoutSettings.IntersectionTypeMap[tname][0]
            self.Logger.warn("No match for node %s with type %s and signature %s" % (name, tname, sig1))

    # -----------------------------------------------------------------
    def CreateNodes(self) :
        intersections = []
        residential = []
        business = []
        people = []

        for name, node in self.World.IterNodes(nodetype = 'Intersection') :
            self.CreateNode(name, node, intersections)

        for name, node in self.World.IterNodes(nodetype = 'EndPoint') :
            if "BusinessLocation" in node.InheritedDecorations:
                self.CreateNode(name, node, business)
            elif "ResidentialLocation" in node.InheritedDecorations:
                self.CreateNode(name, node, residential)

        for name, person in self.World.IterNodes(nodetype = 'Person') :
            self.CreatePerson(name, person, people)

        if self.DataFolder and intersections:
            f = open(os.path.join(self.DataFolder,"intersections.js"), "w")
            f.write(json.dumps(intersections))
            f.close()

        if self.DataFolder and people:
            f = open(os.path.join(self.DataFolder,"people.js"), "w")
            f.write(json.dumps(people))
            f.close()

        if self.DataFolder and business:
            f = open(os.path.join(self.DataFolder,"business.js"), "w")
            f.write(json.dumps(business))
            f.close()

        if self.DataFolder and residential:
            f = open(os.path.join(self.DataFolder,"residential.js"), "w")
            f.write(json.dumps(residential))
            f.close()
