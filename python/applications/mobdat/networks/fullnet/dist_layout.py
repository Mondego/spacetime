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

@file    fullnet/builder.py
@author  Mic Bowman
@date    2013-12-03

This file contains the programmatic specification of the fullnet
traffic network. It depends on the network builder package from
mobdat.

"""

import os, sys
import logging

from applications.mobdat.builder import WorldBuilder
from applications.mobdat.common.Utilities import GenName, GenNameFromCoordinates
from applications.mobdat.common.graph.SocialDecoration import BusinessType
from copy import deepcopy, copy

logger = logging.getLogger('layout')
if 'world' not in globals() and 'world' not in locals():
    world = None
    exit('no world defined')

if 'laysettings' not in globals() and 'laysettings' not in locals():
    laysettings = None
    exit('no laysettings defined')
# -----------------------------------------------------------------
# -----------------------------------------------------------------
def ConvertNodeCoordinate(prefix, p) :
    return GenNameFromCoordinates(p[0], p[1], prefix)

# -----------------------------------------------------------------
# -----------------------------------------------------------------
def ConvertEdgeCoordinate(prefix, p1, p2) :
    return ConvertNodeCoordinate(prefix, p1) + '=O=' + ConvertNodeCoordinate(prefix, p2)

def BuildCity(city_name,empty_world):
    city_name += ':'
    # -----------------------------------------------------------------
    # -----------------------------------------------------------------
    # residence and business nodes
    rntype = empty_world.AddIntersectionType('townhouse', 'priority')
    antype = empty_world.AddIntersectionType('apartment', 'priority', False)
    bntype = empty_world.AddIntersectionType('business', 'priority', False)

    # basic roadway nodes and edges
    pntype = empty_world.AddIntersectionType('priority','priority_stop')
    sntype = empty_world.AddIntersectionType('stoplight','traffic_light')

    e1A = empty_world.AddRoadType('etype1A', 1, 70, 2.0, sig='1L')
    e1B = empty_world.AddRoadType('etype1B', 1, 40, 1.5, sig='1L')
    e1C = empty_world.AddRoadType('etype1C', 1, 20, 1.0, sig='1L')
    e2A = empty_world.AddRoadType('etype2A', 2, 70, 3.0, sig='2L')
    e2B = empty_world.AddRoadType('etype2B', 2, 40, 2.0, sig='2L')
    e2C = empty_world.AddRoadType('etype2C', 2, 20, 1.0, sig='2L')
    e3A = empty_world.AddRoadType('etype3A', 3, 70, 3.0, sig='3L')
    e3B = empty_world.AddRoadType('etype3B', 3, 40, 2.0, sig='3L')
    e3C = empty_world.AddRoadType('etype3C', 3, 20, 1.0, sig='3L')

    e2oneway = empty_world.AddRoadType('1way2lane', 2, 40, 2.0, sig='2L', center=True)
    e3oneway = empty_world.AddRoadType('1way3lane', 3, 40, 2.0, sig='3L', center=True)

    # driveway
    dntype = empty_world.AddIntersectionType('driveway_node', 'priority_stop')
    edrv = empty_world.AddRoadType('driveway_road', 1, 10, 0.5, wid=2.0, sig='D')

    # parking lots
    #plotnode  = empty_world.AddIntersectionType('parking_drive_intersection', 'priority', False)
    #plotentry = empty_world.AddRoadType('parking_entry', 1, 20, 1.0, sig='1L', render=False)
    #plotdrive = empty_world.AddRoadType('parking_drive', 1, 10, 0.5, sig='D', render=False)
    plotnode  = empty_world.AddIntersectionType('parking_drive_intersection', 'priority')
    plotentry = empty_world.AddRoadType('parking_entry', 1, 20, 1.0,wid=2.0, sig='P')
    plotdrive = empty_world.AddRoadType('parking_drive', 1, 10, 0.5,wid=2.0, sig='D')

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # MAIN GRIDS
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    # Create the main east and west grids and drop the corner nodes
    empty_world.GenerateGrid(-400, -400, -100, 400, 100, 100, sntype, e2A, city_name + 'main')
    empty_world.GenerateGrid(100, -400, 400, 400, 100, 100, sntype, e2A, city_name + 'main')
    empty_world.DropNodes(pattern = city_name + 'main400[EW][34]00[SN]')

    # All of these nodes should be four way stops, they are the
    # two lane intersections

    # empty_world.SetIntersectionTypeByPattern('main[24]00[EW][24]00[NS]',pntype)
    # empty_world.SetIntersectionTypeByPattern('main[24]00[EW]0N',pntype)

    # And then set a bunch of the edges to be two lane instead
    # of the four lane edges we created for the rest of the grid
    empty_world.SetRoadTypeByPattern(city_name + 'main[0-9]*[EW]200[NS]=O=' + city_name + 'main[0-9]*[EW]200[NS]',e1A)
    empty_world.SetRoadTypeByPattern(city_name + 'main[0-9]*[EW]400[NS]=O=' + city_name + 'main[0-9]*[EW]400[NS]',e1A)

    empty_world.SetRoadTypeByPattern(city_name + 'main300[EW][0-9]*[NS]=O=' + city_name + 'main300[EW][0-9]*[NS]',e1A)
    empty_world.SetRoadTypeByPattern(city_name + 'main400[EW][0-9]*[NS]=O=' + city_name + 'main400[EW][0-9]*[NS]',e1A)

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # PLAZA GRID
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    # Create the plaza grid in the center of the map
    empty_world.GenerateGrid(-50, -250, 50, 250, 50, 50, sntype, e1B, city_name + 'plaza')

    # Make the east and west edges one way
    # pattern = 'plaza50W{0}{2}=O=plaza50W{1}{3}'
    for ns in range(-250, 250, 50) :
        empty_world.DropEdgeByName(ConvertEdgeCoordinate(city_name + 'plaza', (-50, ns), (-50, ns + 50)))

    for ns in range(-200, 300, 50) :
        empty_world.DropEdgeByName(ConvertEdgeCoordinate(city_name + 'plaza', ( 50, ns), ( 50, ns - 50)))

    # Make the north and south most east/west streets one way as well
    empty_world.DropEdgeByName(city_name + 'plaza50E250S=O=' + city_name + 'plaza0E250S')
    empty_world.DropEdgeByName(city_name + 'plaza0E250S=O=' + city_name + 'plaza50W250S')
    empty_world.DropEdgeByName(city_name + 'plaza50W250N=O=' + city_name + 'plaza0E250N')
    empty_world.DropEdgeByName(city_name + 'plaza0E250N=O=' + city_name + 'plaza50E250N')

    # The one way streets are all 2 lanes
    empty_world.SetRoadTypeByPattern(city_name + 'plaza50[EW].*=O=' + city_name + 'plaza50[EW].*',e3oneway)
    empty_world.SetRoadTypeByPattern(city_name + 'plaza.*250[NS]=O=' + city_name + 'plaza.*250[NS]',e3oneway)

    # The central north/south road is four lane
    empty_world.SetRoadTypeByPattern(city_name + 'plaza[0-9]*[EW]100[NS]=O=' + city_name + 'plaza[0-9]*[EW]100[NS]',e3A)
    empty_world.SetRoadTypeByPattern(city_name + 'plaza[0-9]*[EW]0N=O=' + city_name + 'plaza[0-9]*[EW]0N',e3A)
    empty_world.SetRoadTypeByPattern(city_name + 'plaza0E[0-9]*[NS]=O=' + city_name + 'plaza0E[0-9]*[NS]',e3A)
    empty_world.SetRoadTypeByPattern(city_name + 'plaza0E[0-9]*[NS]=O=' + city_name + 'plaza0E[0-9]*[NS]',e3A)

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # CONNECT THE GRIDS
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    # Create nodes at the north and south ends of the plaza
    empty_world.AddIntersection(0, -300, sntype, city_name + 'main') # south end of the plaza
    empty_world.AddIntersection(0, 300, sntype, city_name + 'main')  # north end of the plaza

    # And connect them to the east and west main grids
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100W300N'],empty_world.Nodes[city_name + 'main0E300N'],e3A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100E300N'],empty_world.Nodes[city_name + 'main0E300N'],e3A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100W300S'],empty_world.Nodes[city_name + 'main0E300S'],e3A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100E300S'],empty_world.Nodes[city_name + 'main0E300S'],e3A)

    # Connect the plaza nodes to the north & south ends
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'plaza0E250S'],empty_world.Nodes[city_name + 'main0E300S'],e3A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'plaza0E250N'],empty_world.Nodes[city_name + 'main0E300N'],e3A)

    # Connect the plaza nodes to the east and west roads
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100W100N'],empty_world.Nodes[city_name + 'plaza50W100N'],e3A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100W100S'],empty_world.Nodes[city_name + 'plaza50W100S'],e3A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100E100N'],empty_world.Nodes[city_name + 'plaza50E100N'],e3A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100E100S'],empty_world.Nodes[city_name + 'plaza50E100S'],e3A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100W0N'],empty_world.Nodes[city_name + 'plaza50W0N'],e3A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100E0N'],empty_world.Nodes[city_name + 'plaza50E0N'],e3A)

    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100W200S'], empty_world.Nodes[city_name + 'plaza50W200S'], e1A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100E200S'], empty_world.Nodes[city_name + 'plaza50E200S'], e1A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100W200N'], empty_world.Nodes[city_name + 'plaza50W200N'], e1A)
    empty_world.ConnectIntersections(empty_world.Nodes[city_name + 'main100E200N'], empty_world.Nodes[city_name + 'plaza50E200N'], e1A)

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # BUILD THE RESIDENTIAL NEIGHBORHOODS
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    empty_world.AddBusinessLocationProfile(city_name + 'plaza', 50, 25,  { BusinessType.Factory : 1.0, BusinessType.Service : 0.5, BusinessType.Food : 0.25 })
    empty_world.AddBusinessLocationProfile(city_name + 'mall',  18, 75,  { BusinessType.Factory : 0.1, BusinessType.Service : 1.0, BusinessType.Food : 1.0 })
    empty_world.AddBusinessLocationProfile(city_name + 'civic', 25, 150, { BusinessType.School : 1.0, BusinessType.Civic : 1.0 })

    empty_world.AddResidentialLocationProfile(city_name + 'townhouse_rp', 13)
    empty_world.AddResidentialLocationProfile(city_name + 'apartment_rp', 27)

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    rgenv = WorldBuilder.ResidentialGenerator(e1C, dntype, edrv, rntype, bspace = 20, spacing = 10, driveway = 5)
    rgenh = WorldBuilder.ResidentialGenerator(e1C, dntype, edrv, rntype, bspace = 40, spacing = 10)

    for ew in [-400, -300, 300, 400] :
        for ns in range (-200, 200, 100) :
            node1 = empty_world.Nodes[ConvertNodeCoordinate(city_name + 'main', (ew, ns))]
            node2 = empty_world.Nodes[ConvertNodeCoordinate(city_name + 'main', (ew, ns + 100))]
            empty_world.AddResidentialLocation(city_name + 'townhouse_rp', empty_world.GenerateResidential(node1, node2, rgenv, prefix=city_name+'thouse'))

    for ns in [-400, 400] :
        for ew in [-300, -200, 100, 200] :
            node1 = empty_world.Nodes[ConvertNodeCoordinate(city_name + 'main', (ew, ns))]
            node2 = empty_world.Nodes[ConvertNodeCoordinate(city_name + 'main', (ew + 100, ns))]
            empty_world.AddResidentialLocation(city_name + 'townhouse_rp', empty_world.GenerateResidential(node1, node2, rgenv, prefix=city_name+'thouse'))

    rgenv.BothSides = False
    empty_world.AddResidentialLocation(city_name + 'townhouse_rp', empty_world.GenerateResidential(empty_world.Nodes[city_name + 'main300W200N'],empty_world.Nodes[city_name + 'main400W200N'], rgenv, city_name+'res'))
    empty_world.AddResidentialLocation(city_name + 'townhouse_rp', empty_world.GenerateResidential(empty_world.Nodes[city_name + 'main300E200N'],empty_world.Nodes[city_name + 'main400E200N'], rgenv, city_name+'res'))

    rgenv.DrivewayLength = - rgenv.DrivewayLength
    empty_world.AddResidentialLocation(city_name + 'townhouse_rp', empty_world.GenerateResidential(empty_world.Nodes[city_name + 'main400W200S'],empty_world.Nodes[city_name + 'main300W200S'], rgenv, city_name+'res'))
    empty_world.AddResidentialLocation(city_name + 'townhouse_rp', empty_world.GenerateResidential(empty_world.Nodes[city_name + 'main400E200S'],empty_world.Nodes[city_name + 'main300E200S'], rgenv, city_name+'res'))

    # some of the malls to be marked as residential apartments
    rgenplR = WorldBuilder.ResidentialGenerator(plotentry, plotnode, plotdrive, antype, driveway = -8, bspace = 5, spacing = 5, both = False)
    rgenplL = WorldBuilder.ResidentialGenerator(plotentry, plotnode, plotdrive, antype, driveway = 8, bspace = 5, spacing = 5, both = False)

    for n in ['main200W200S', 'main100E200S', 'main200E200S', 'main300W200N', 'main200W200N', 'main100E200N'] :
        empty_world.AddResidentialLocation(city_name + 'apartment_rp', empty_world.BuildSimpleParkingLotEW(empty_world.Nodes[city_name + n], pntype, rgenplR, city_name + 'apartment', offset=-15, slength=40, elength=60))
        empty_world.AddResidentialLocation(city_name + 'apartment_rp', empty_world.BuildSimpleParkingLotEW(empty_world.Nodes[city_name + n], pntype, rgenplL, city_name + 'apartment', offset=15, slength=40, elength=60))

    for n in ['main200W200S', 'main200W100S', 'main200W200N', 'main200W100N'] :
        empty_world.AddResidentialLocation(city_name + 'apartment_rp', empty_world.BuildSimpleParkingLotNS(empty_world.Nodes[city_name + n], pntype, rgenplR, city_name + 'apartment', offset=-30))
        empty_world.AddResidentialLocation(city_name + 'apartment_rp', empty_world.BuildSimpleParkingLotNS(empty_world.Nodes[city_name + n], pntype, rgenplL, city_name + 'apartment', offset=30))

    for n in ['main200E100S', 'main200E0N', 'main200E100N'] :
        empty_world.AddResidentialLocation(city_name + 'apartment_rp', empty_world.BuildSimpleParkingLotNS(empty_world.Nodes[city_name + n], pntype, rgenplR, city_name + 'apartment', offset=-30))
        empty_world.AddResidentialLocation(city_name + 'apartment_rp', empty_world.BuildSimpleParkingLotNS(empty_world.Nodes[city_name + n], pntype, rgenplL, city_name + 'apartment', offset=30))

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # BUILD THE BUSINESS NEIGHBORHOODS
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    rgenplR = WorldBuilder.ResidentialGenerator(plotentry, plotnode, plotdrive, bntype, driveway = 8, bspace = 5, spacing = 5, both = False)
    rgenplL = WorldBuilder.ResidentialGenerator(plotentry, plotnode, plotdrive, bntype, driveway = -8, bspace = 5, spacing = 5, both = False)

    # mark some school
    for n in ['main300W200S', 'main200E200N'] :
        empty_world.AddBusinessLocation(city_name + 'civic', empty_world.BuildSimpleParkingLotEW(empty_world.Nodes[city_name + n], pntype, rgenplR, city_name + 'civic', offset=-15, slength=40, elength=60))
        empty_world.AddBusinessLocation(city_name + 'civic', empty_world.BuildSimpleParkingLotEW(empty_world.Nodes[city_name + n], pntype, rgenplL, city_name + 'civic', offset=15, slength=40, elength=60))

    # these are the downtown work and shopping plazas
    for ns in range(-200, 300, 50) :
        wname = ConvertNodeCoordinate(city_name + 'plaza', (-50, ns))
        empty_world.AddBusinessLocation(city_name + 'plaza', empty_world.BuildSimpleParkingLotSN(empty_world.Nodes[wname], pntype, rgenplL, city_name + 'plaza', offset=25, slength = 17.5, elength=32.5))

        ename = ConvertNodeCoordinate(city_name + 'plaza', (50, ns-50))
        empty_world.AddBusinessLocation(city_name + 'plaza', empty_world.BuildSimpleParkingLotNS(empty_world.Nodes[ename], pntype, rgenplR, city_name + 'plaza', offset=-25, slength = 17.5, elength=32.5))

    # these are the main business areas
    for n in ['main200W300S', 'main200W0N'] :
        empty_world.AddBusinessLocation(city_name + 'mall', empty_world.BuildSimpleParkingLotNS(empty_world.Nodes[city_name + n], pntype, rgenplR, city_name + 'mall', offset=-30))
        empty_world.AddBusinessLocation(city_name + 'mall', empty_world.BuildSimpleParkingLotNS(empty_world.Nodes[city_name + n], pntype, rgenplL, city_name + 'mall', offset=30))

    for n in ['main200E300S', 'main200E200S', 'main200E200N'] :
        empty_world.AddBusinessLocation(city_name + 'mall', empty_world.BuildSimpleParkingLotNS(empty_world.Nodes[city_name + n], pntype, rgenplR, city_name + 'mall', offset=-30))
        empty_world.AddBusinessLocation(city_name + 'mall', empty_world.BuildSimpleParkingLotNS(empty_world.Nodes[city_name + n], pntype, rgenplL, city_name + 'mall', offset=30))

    return empty_world

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

#world1 = world
#world2 = WorldBuilder.WorldBuilder()
#BuildCity('City00',world1)
#BuildCity('City11',world2)
#for name,node in world2.IterNodes():
#    if "Coord" in node.Decorations.keys():
#        node.Decorations["Coord"].X += 800
#        node.Decorations["Coord"].Y += 800
#    world1.AddNode(node)

for name,city in laysettings.Cities.items():
    new_world = WorldBuilder.WorldBuilder()
    BuildCity(name,new_world)
    for name,node in new_world.IterNodes():
        if "Coord" in node.Decorations.keys():
            node.Decorations["Coord"].X += city["Offset"][0]
            node.Decorations["Coord"].Y += city["Offset"][1]
        try:
            world.AddNode(node)
        except:
            pass

    for name,edge in new_world.IterEdges():
        world.AddEdge(edge)

for conn in laysettings.CityConnections:
    world.ConnectIntersections(world.Nodes[conn[0]],world.Nodes[conn[1]],world.FindNodeByName('etype2A'))

logger.info("Loaded fullnet network builder extension file")
