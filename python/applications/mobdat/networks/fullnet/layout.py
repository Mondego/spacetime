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

logger = logging.getLogger('layout')
if 'world' not in globals() and 'world' not in locals():
    world = None
    exit('no world global variable')

if 'laysettings' not in globals() and 'laysettings' not in locals():
    laysettings = None
    exit('no laysettings variable')
# -----------------------------------------------------------------
# -----------------------------------------------------------------
def ConvertNodeCoordinate(prefix, p) :
    return GenNameFromCoordinates(p[0], p[1], prefix)

# -----------------------------------------------------------------
# -----------------------------------------------------------------
def ConvertEdgeCoordinate(prefix, p1, p2) :
    return ConvertNodeCoordinate(prefix, p1) + '=O=' + ConvertNodeCoordinate(prefix, p2)

# -----------------------------------------------------------------
# -----------------------------------------------------------------
# residence and business nodes
rntype = world.AddIntersectionType('townhouse', 'priority')
antype = world.AddIntersectionType('apartment', 'priority', False)
bntype = world.AddIntersectionType('business', 'priority', False)

# basic roadway nodes and edges
pntype = world.AddIntersectionType('priority','priority_stop')
sntype = world.AddIntersectionType('stoplight','traffic_light')

e1A = world.AddRoadType('etype1A', 1, 70, 2.0, sig='1L')
e1B = world.AddRoadType('etype1B', 1, 40, 1.5, sig='1L')
e1C = world.AddRoadType('etype1C', 1, 20, 1.0, sig='1L')
e2A = world.AddRoadType('etype2A', 2, 70, 3.0, sig='2L')
e2B = world.AddRoadType('etype2B', 2, 40, 2.0, sig='2L')
e2C = world.AddRoadType('etype2C', 2, 20, 1.0, sig='2L')
e3A = world.AddRoadType('etype3A', 3, 70, 3.0, sig='3L')
e3B = world.AddRoadType('etype3B', 3, 40, 2.0, sig='3L')
e3C = world.AddRoadType('etype3C', 3, 20, 1.0, sig='3L')

e2oneway = world.AddRoadType('1way2lane', 2, 40, 2.0, sig='2L', center=True)
e3oneway = world.AddRoadType('1way3lane', 3, 40, 2.0, sig='3L', center=True)

# driveway
dntype = world.AddIntersectionType('driveway_node', 'priority_stop')
edrv = world.AddRoadType('driveway_road', 1, 10, 0.5, wid=2.0, sig='D')

# parking lots
#plotnode  = world.AddIntersectionType('parking_drive_intersection', 'priority', False)
#plotentry = world.AddRoadType('parking_entry', 1, 20, 1.0, sig='1L', render=False)
#plotdrive = world.AddRoadType('parking_drive', 1, 10, 0.5, sig='D', render=False)
plotnode  = world.AddIntersectionType('parking_drive_intersection', 'priority')
plotentry = world.AddRoadType('parking_entry', 1, 20, 1.0, wid=2.0, sig='P')
plotdrive = world.AddRoadType('parking_drive', 1, 10, 0.5, wid=2.0, sig='D')

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# MAIN GRIDS
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

# Create the main east and west grids and drop the corner nodes
world.GenerateGrid(-400, -400, -100, 400, 100, 100, sntype, e2A, 'main')
world.GenerateGrid(100, -400, 400, 400, 100, 100, sntype, e2A, 'main')
world.DropNodes(pattern = 'main400[EW][34]00[SN]')

# All of these nodes should be four way stops, they are the
# two lane intersections

# world.SetIntersectionTypeByPattern('main[24]00[EW][24]00[NS]',pntype)
# world.SetIntersectionTypeByPattern('main[24]00[EW]0N',pntype)

# And then set a bunch of the edges to be two lane instead
# of the four lane edges we created for the rest of the grid
world.SetRoadTypeByPattern('main[0-9]*[EW]200[NS]=O=main[0-9]*[EW]200[NS]',e1A)
world.SetRoadTypeByPattern('main[0-9]*[EW]400[NS]=O=main[0-9]*[EW]400[NS]',e1A)

world.SetRoadTypeByPattern('main300[EW][0-9]*[NS]=O=main300[EW][0-9]*[NS]',e1A)
world.SetRoadTypeByPattern('main400[EW][0-9]*[NS]=O=main400[EW][0-9]*[NS]',e1A)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# PLAZA GRID
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

# Create the plaza grid in the center of the map
world.GenerateGrid(-50, -250, 50, 250, 50, 50, sntype, e1B, 'plaza')

# Make the east and west edges one way
# pattern = 'plaza50W{0}{2}=O=plaza50W{1}{3}'
for ns in range(-250, 250, 50) :
    world.DropEdgeByName(ConvertEdgeCoordinate('plaza', (-50, ns), (-50, ns + 50)))

for ns in range(-200, 300, 50) :
    world.DropEdgeByName(ConvertEdgeCoordinate('plaza', ( 50, ns), ( 50, ns - 50)))

# Make the north and south most east/west streets one way as well
world.DropEdgeByName('plaza50E250S=O=plaza0E250S')
world.DropEdgeByName('plaza0E250S=O=plaza50W250S')
world.DropEdgeByName('plaza50W250N=O=plaza0E250N')
world.DropEdgeByName('plaza0E250N=O=plaza50E250N')

# The one way streets are all 2 lanes
world.SetRoadTypeByPattern('plaza50[EW].*=O=plaza50[EW].*',e3oneway)
world.SetRoadTypeByPattern('plaza.*250[NS]=O=plaza.*250[NS]',e3oneway)

# The central north/south road is four lane
world.SetRoadTypeByPattern('plaza[0-9]*[EW]100[NS]=O=plaza[0-9]*[EW]100[NS]',e3A)
world.SetRoadTypeByPattern('plaza[0-9]*[EW]0N=O=plaza[0-9]*[EW]0N',e3A)
world.SetRoadTypeByPattern('plaza0E[0-9]*[NS]=O=plaza0E[0-9]*[NS]',e3A)
world.SetRoadTypeByPattern('plaza0E[0-9]*[NS]=O=plaza0E[0-9]*[NS]',e3A)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# CONNECT THE GRIDS
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

# Create nodes at the north and south ends of the plaza
world.AddIntersection(0, -300, sntype, 'main') # south end of the plaza
world.AddIntersection(0, 300, sntype, 'main')  # north end of the plaza

# And connect them to the east and west main grids
world.ConnectIntersections(world.Nodes['main100W300N'],world.Nodes['main0E300N'],e3A)
world.ConnectIntersections(world.Nodes['main100E300N'],world.Nodes['main0E300N'],e3A)
world.ConnectIntersections(world.Nodes['main100W300S'],world.Nodes['main0E300S'],e3A)
world.ConnectIntersections(world.Nodes['main100E300S'],world.Nodes['main0E300S'],e3A)

# Connect the plaza nodes to the north & south ends
world.ConnectIntersections(world.Nodes['plaza0E250S'],world.Nodes['main0E300S'],e3A)
world.ConnectIntersections(world.Nodes['plaza0E250N'],world.Nodes['main0E300N'],e3A)

# Connect the plaza nodes to the east and west roads
world.ConnectIntersections(world.Nodes['main100W100N'],world.Nodes['plaza50W100N'],e3A)
world.ConnectIntersections(world.Nodes['main100W100S'],world.Nodes['plaza50W100S'],e3A)
world.ConnectIntersections(world.Nodes['main100E100N'],world.Nodes['plaza50E100N'],e3A)
world.ConnectIntersections(world.Nodes['main100E100S'],world.Nodes['plaza50E100S'],e3A)
world.ConnectIntersections(world.Nodes['main100W0N'],world.Nodes['plaza50W0N'],e3A)
world.ConnectIntersections(world.Nodes['main100E0N'],world.Nodes['plaza50E0N'],e3A)

world.ConnectIntersections(world.Nodes['main100W200S'], world.Nodes['plaza50W200S'], e1A)
world.ConnectIntersections(world.Nodes['main100E200S'], world.Nodes['plaza50E200S'], e1A)
world.ConnectIntersections(world.Nodes['main100W200N'], world.Nodes['plaza50W200N'], e1A)
world.ConnectIntersections(world.Nodes['main100E200N'], world.Nodes['plaza50E200N'], e1A)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# BUILD THE RESIDENTIAL NEIGHBORHOODS
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
world.AddBusinessLocationProfile('plaza', 50, 25,  { BusinessType.Factory : 1.0, BusinessType.Service : 0.5, BusinessType.Food : 0.25 })
world.AddBusinessLocationProfile('mall',  18, 75,  { BusinessType.Factory : 0.1, BusinessType.Service : 1.0, BusinessType.Food : 1.0 })
world.AddBusinessLocationProfile('civic', 25, 150, { BusinessType.School : 1.0, BusinessType.Civic : 1.0 })

world.AddResidentialLocationProfile('townhouse_rp', 13)
world.AddResidentialLocationProfile('apartment_rp', 27)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
rgenv = WorldBuilder.ResidentialGenerator(e1C, dntype, edrv, rntype, bspace = 20, spacing = 10, driveway = 5)
rgenh = WorldBuilder.ResidentialGenerator(e1C, dntype, edrv, rntype, bspace = 40, spacing = 10)

for ew in [-400, -300, 300, 400] :
    for ns in range (-200, 200, 100) :
        node1 = world.Nodes[ConvertNodeCoordinate('main', (ew, ns))]
        node2 = world.Nodes[ConvertNodeCoordinate('main', (ew, ns + 100))]
        world.AddResidentialLocation('townhouse_rp', world.GenerateResidential(node1, node2, rgenv, prefix='thouse'))

for ns in [-400, 400] :
    for ew in [-300, -200, 100, 200] :
        node1 = world.Nodes[ConvertNodeCoordinate('main', (ew, ns))]
        node2 = world.Nodes[ConvertNodeCoordinate('main', (ew + 100, ns))]
        world.AddResidentialLocation('townhouse_rp', world.GenerateResidential(node1, node2, rgenv, prefix='thouse'))

rgenv.BothSides = False
world.AddResidentialLocation('townhouse_rp', world.GenerateResidential(world.Nodes['main300W200N'],world.Nodes['main400W200N'], rgenv))
world.AddResidentialLocation('townhouse_rp', world.GenerateResidential(world.Nodes['main300E200N'],world.Nodes['main400E200N'], rgenv))

rgenv.DrivewayLength = - rgenv.DrivewayLength
world.AddResidentialLocation('townhouse_rp', world.GenerateResidential(world.Nodes['main400W200S'],world.Nodes['main300W200S'], rgenv))
world.AddResidentialLocation('townhouse_rp', world.GenerateResidential(world.Nodes['main400E200S'],world.Nodes['main300E200S'], rgenv))

# some of the malls to be marked as residential apartments
rgenplR = WorldBuilder.ResidentialGenerator(plotentry, plotnode, plotdrive, antype, driveway = -8, bspace = 5, spacing = 5, both = False)
rgenplL = WorldBuilder.ResidentialGenerator(plotentry, plotnode, plotdrive, antype, driveway = 8, bspace = 5, spacing = 5, both = False)

for n in ['main200W200S', 'main100E200S', 'main200E200S', 'main300W200N', 'main200W200N', 'main100E200N'] :
    world.AddResidentialLocation('apartment_rp', world.BuildSimpleParkingLotEW(world.Nodes[n], pntype, rgenplR, 'apartment', offset=-15, slength=40, elength=60))
    world.AddResidentialLocation('apartment_rp', world.BuildSimpleParkingLotEW(world.Nodes[n], pntype, rgenplL, 'apartment', offset=15, slength=40, elength=60))

for n in ['main200W200S', 'main200W100S', 'main200W200N', 'main200W100N'] :
    world.AddResidentialLocation('apartment_rp', world.BuildSimpleParkingLotNS(world.Nodes[n], pntype, rgenplR, 'apartment', offset=-30))
    world.AddResidentialLocation('apartment_rp', world.BuildSimpleParkingLotNS(world.Nodes[n], pntype, rgenplL, 'apartment', offset=30))

for n in ['main200E100S', 'main200E0N', 'main200E100N'] :
    world.AddResidentialLocation('apartment_rp', world.BuildSimpleParkingLotNS(world.Nodes[n], pntype, rgenplR, 'apartment', offset=-30))
    world.AddResidentialLocation('apartment_rp', world.BuildSimpleParkingLotNS(world.Nodes[n], pntype, rgenplL, 'apartment', offset=30))

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# BUILD THE BUSINESS NEIGHBORHOODS
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

rgenplR = WorldBuilder.ResidentialGenerator(plotentry, plotnode, plotdrive, bntype, driveway = 8, bspace = 5, spacing = 5, both = False)
rgenplL = WorldBuilder.ResidentialGenerator(plotentry, plotnode, plotdrive, bntype, driveway = -8, bspace = 5, spacing = 5, both = False)

# mark some school
for n in ['main300W200S', 'main200E200N'] :
    world.AddBusinessLocation('civic', world.BuildSimpleParkingLotEW(world.Nodes[n], pntype, rgenplR, 'civic', offset=-15, slength=40, elength=60))
    world.AddBusinessLocation('civic', world.BuildSimpleParkingLotEW(world.Nodes[n], pntype, rgenplL, 'civic', offset=15, slength=40, elength=60))

# these are the downtown work and shopping plazas
for ns in range(-200, 300, 50) :
    wname = ConvertNodeCoordinate('plaza', (-50, ns))
    world.AddBusinessLocation('plaza', world.BuildSimpleParkingLotSN(world.Nodes[wname], pntype, rgenplL, 'plaza', offset=25, slength = 17.5, elength=32.5))

    ename = ConvertNodeCoordinate('plaza', (50, ns-50))
    world.AddBusinessLocation('plaza', world.BuildSimpleParkingLotNS(world.Nodes[ename], pntype, rgenplR, 'plaza', offset=-25, slength = 17.5, elength=32.5))

# these are the main business areas
for n in ['main200W300S', 'main200W0N'] :
    world.AddBusinessLocation('mall', world.BuildSimpleParkingLotNS(world.Nodes[n], pntype, rgenplR, 'mall', offset=-30))
    world.AddBusinessLocation('mall', world.BuildSimpleParkingLotNS(world.Nodes[n], pntype, rgenplL, 'mall', offset=30))

for n in ['main200E300S', 'main200E200S', 'main200E200N'] :
    world.AddBusinessLocation('mall', world.BuildSimpleParkingLotNS(world.Nodes[n], pntype, rgenplR, 'mall', offset=-30))
    world.AddBusinessLocation('mall', world.BuildSimpleParkingLotNS(world.Nodes[n], pntype, rgenplL, 'mall', offset=30))

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
logger.info("Loaded fullnet network builder extension file")
