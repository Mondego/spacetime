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

@file    SumoBuilder.py
@author  Mic Bowman
@date    2013-12-03

This file defines the SumoBuilder class that translates mobdat traffic
network into a network specification suitable for driving the sumo
traffic simulator.

"""

import os, sys
import logging

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class SumoBuilder :

    # -----------------------------------------------------------------
    def __init__(self, settings, world, laysettings) :
        self.Logger = logging.getLogger(__name__)

        self.World = world
        self.LayoutSettings = laysettings

        self.ScaleValue = 3.0

        try :
            self.Path = settings["SumoConnector"].get("SumoNetworkPath",".")
            self.Prefix = settings["SumoConnector"].get("SumoDataFilePrefix","network")
            self.ScaleValue = settings["SumoConnector"].get("NetworkScaleFactor",3.0)
            self.VehicleScaleValue = settings["SumoConnector"].get("VehicleScaleFactor",2.0)
        except NameError as detail:
            self.Logger.warn("Failed processing sumo configuration; name error %s", (str(detail)))
            sys.exit(-1)
        except KeyError as detail:
            self.Logger.warn("unable to locate sumo configuration value for %s", (str(detail)))
            sys.exit(-1)
        except :
            self.Logger.warn("SumoBuilder configuration failed; %s", (sys.exc_info()[0]))
            sys.exit(-1)

    # -----------------------------------------------------------------
    def Scale(self, value) :
        return self.ScaleValue * value

    # -----------------------------------------------------------------
    def VehicleScale(self, value) :
        return self.VehicleScaleValue * value

    # -----------------------------------------------------------------
    def CreateRoads(self) :
        fname = os.path.join(self.Path,self.Prefix + '.edg.xml')

        with open(fname, 'w') as fp :
            fp.write("<edges>\n")

            for ename, edge in self.World.IterEdges(edgetype = 'Road') :
                sn = edge.StartNode.Name
                en = edge.EndNode.Name
                etype = edge.RoadType.Name
                cn = 'center' if edge.RoadType.Center else 'right'
                fp.write("  <edge id=\"%s\" spreadType=\"%s\" from=\"%s\" to=\"%s\" type=\"%s\" />\n" % (ename, cn, sn, en, etype))

            fp.write("</edges>\n")

    # -----------------------------------------------------------------
    def CreateIntersections(self) :
        fname = os.path.join(self.Path,self.Prefix + '.nod.xml')

        with open(fname, 'w') as fp :
            fp.write("<nodes>\n")

            for name, node in self.World.IterNodes(nodetype = 'Intersection') :
                itype = node.IntersectionType.IntersectionType
                fp.write("  <node id=\"%s\" x=\"%d\" y=\"%d\" z=\"0\"  type=\"%s\" />\n" % (name, self.Scale(node.Coord.X), self.Scale(node.Coord.Y), itype))

            for name, node in self.World.IterNodes(nodetype = 'EndPoint') :
                itype = node.IntersectionType.IntersectionType
                fp.write("  <node id=\"%s\" x=\"%d\" y=\"%d\" z=\"0\"  type=\"%s\" />\n" % (name, self.Scale(node.Coord.X), self.Scale(node.Coord.Y), itype))

            fp.write("</nodes>\n")

    # -----------------------------------------------------------------
    def CreateConnections(self) :
        fname = os.path.join(self.Path,self.Prefix + '.con.xml')

        fstring = "  <connection from=\"{0}\" to=\"{1}\" fromLane=\"{2}\" toLane=\"{3}\" />\n"
        with open(fname, 'w') as fp :
            fp.write("<connections>\n")

            for name, node in self.World.IterNodes(nodetype = 'Intersection') :

                if not node.EdgeMap.Signature() == ['2L/2L', '2L/2L', '2L/2L', '2L/2L' ] :
                    continue

                oedges = node.EdgeMap.OutputEdgeMap()
                iedges = node.EdgeMap.InputEdgeMap()
                for pos in range(4) :
                    lpos = (pos + 1) % 4 # left turn
                    spos = (pos + 2) % 4 # straight across
                    rpos = (pos + 3) % 4 # right turn

                    fp.write(fstring.format(iedges[pos].Name, oedges[lpos].Name, 1, 1))
                    fp.write(fstring.format(iedges[pos].Name, oedges[spos].Name, 1, 1))
                    fp.write(fstring.format(iedges[pos].Name, oedges[spos].Name, 0, 0))
                    fp.write(fstring.format(iedges[pos].Name, oedges[rpos].Name, 0, 0))

            fp.write("</connections>\n")

    # -----------------------------------------------------------------
    def CreateRoadTypes(self) :
        fname = os.path.join(self.Path,self.Prefix + '.typ.xml')

        with open(fname, 'w') as fp :
            fp.write("<types>\n")

            for name, rtype in self.World.IterNodes(nodetype = 'RoadType') :
                etype = rtype.RoadType
                fp.write("  <type id=\"%s\" priority=\"%d\" numLanes=\"%d\" speed=\"%f\" width=\"%f\" />\n" %
                         (etype.Name, etype.Priority, etype.Lanes, self.Scale(etype.Speed), self.VehicleScale(etype.Width)))

            fp.write("</types>\n")

    # -----------------------------------------------------------------
    def CreateRoutes(self) :
        vtfmt = '  <vType id="{0}" accel="{1}" decel="{2}" sigma="{3}" length="{4}" minGap="{5}" maxSpeed="{6}" guiShape="passenger"/>'

        fname = os.path.join(self.Path,self.Prefix + '.rou.xml')

        with open(fname, 'w') as fp :
            fp.write("<routes>\n")

            for v in self.LayoutSettings.VehicleTypes :
                vtype = self.LayoutSettings.VehicleTypes[v]
                fp.write(vtfmt.format(v, self.Scale(vtype.Acceleration), self.Scale(vtype.Deceleration),
                                      vtype.Sigma, self.VehicleScale(vtype.Length), self.VehicleScale(vtype.MinGap), self.Scale(vtype.MaxSpeed)) + "\n")

            fp.write("\n")

            for name, node in self.World.IterNodes(nodetype = 'EndPoint') :
                name = None
                for edge in node.OutputEdges :
                    for redge in edge.EndNode.OutputEdges :
                        if redge.EndNode == node :
                            name = node.EndPoint.DestinationName
                            edges = edge.Name + " " + redge.Name
                            fp.write("  <route id=\"%s\" edges=\"%s\" />\n" % (name, edges))
                            break

                if not name :
                    self.Logger.warn('cannot find route for %s', node.Name)

            fp.write("</routes>\n")

    # -----------------------------------------------------------------
    def PushNetworkToSumo(self) :
        self.CreateIntersections()
        self.CreateRoads()
        self.CreateRoadTypes()
        self.CreateRoutes()
        self.CreateConnections()
