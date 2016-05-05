#!/usr/bin/python
#----------------------------------------------------------------
# load OSM data file into memory
#
#------------------------------------------------------
# Usage:
#   data = LoadOsm(filename)
# or:
#   loadOsm.py filename.osm
#------------------------------------------------------
# Copyright 2007, Oliver White
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#------------------------------------------------------
# Changelog:
#  2007-11-04  OJW  Modified from pyroute.py
#  2007-11-05  OJW  Multiple forms of transport
#------------------------------------------------------
import sys
import os
from xml.sax import make_parser, handler
import xml
from util_binary import *
from struct import *

execfile(os.path.join(os.path.dirname(__file__), "weights.py"))

class LoadOsm(handler.ContentHandler):
  """Parse an OSM file looking for routing information, and do routing with it"""
  def __init__(self, filename, storeMap = 0):
    """Initialise an OSM-file parser"""
    self.routing = {}
    self.routeTypes = ('cycle','car','train','foot','horse')
    self.routeableNodes = {}
    for routeType in self.routeTypes:
      self.routing[routeType] = {}
      self.routeableNodes[routeType] = {}
    self.nodes = {}
    self.ways = []
    self.storeMap = storeMap

    if(filename == None):
      return
    self.loadOsm(filename)

  def loadOsm(self, filename):
    if(not os.path.exists(filename)):
      print "No such data file %s" % filename
      return
    try:
      parser = make_parser()
      parser.setContentHandler(self)
      parser.parse(filename)
    except xml.sax._exceptions.SAXParseException:
      print "Error loading %s" % filename

  def report(self):
    """Display some info about the loaded data"""
    report = "Loaded %d nodes,\n" % len(self.nodes.keys())
    report = report + "%d ways, and...\n" % len(self.ways)
    for routeType in self.routeTypes:
      report = report + " %d %s routes\n" % ( \
        len(self.routing[routeType].keys()),
        routeType)
    return(report)

  def savebin(self,filename):
    self.newIDs = {}

    f = open(filename,"wb")
    f.write(pack('L',len(self.nodes.keys())))
    count = 0
    for id, n in self.nodes.items():
      self.newIDs[id] = count
      f.write(encodeLL(n[0],n[1]))
      count = count + 1

    f.write(pack('B', len(self.routing.keys())))
    errors = 0
    for routeType, data in self.routing.items():
      f.write(pack('B', len(routeType)))
      f.write(routeType)

      f.write(pack('L', len(data.keys())))
      for fr, destinations in data.items():
        try:
          f.write(pack('L', self.newIDs[fr]))
        except KeyError:
          f.write(pack('L', 0))
          errors = errors + 1
          continue

        f.write(pack('B', len(destinations.keys())))
        for to, weight in destinations.items():
          try:
            f.write(pack('Lf', self.newIDs[to], weight))
          except KeyError:
            f.write(pack('Lf', 0, 0))
            errors = errors + 1

    print "%d key errors" % errors
    f.close()

  def loadbin(self,filename):
    f = open(filename,"rb")
    n = unpack('L', f.read(4))[0]
    print "%u nodes" % n
    id = 0
    for i in range(n):
      lat,lon = decodeLL(f.read(8))
      #print "%u: %f, %f" % (id,lat,lon)
      id = id + 1

    numTypes = unpack('B', f.read(1))[0]
    print "%u types:" % numTypes
    for typ in range(numTypes):
      lenName = unpack('B', f.read(1))[0]
      name = f.read(lenName)
      numLinks = 0

      numHubs = unpack('L', f.read(4))[0]
      for hub in range(numHubs):
        fr = unpack('L', f.read(4))[0]
        numDest = unpack('B', f.read(1))[0]
        for dest in range(numDest):
          to,weight = unpack('Lf', f.read(8))
          numLinks = numLinks + 1
      print "  \"%s\" (%u segments)" % (name, numLinks)

    f.close()

  def startElement(self, name, attrs):
    """Handle XML elements"""
    if name in('node','way','relation'):
      self.tags = {}
      self.waynodes = []
      if name == 'node':
        """Nodes need to be stored"""
        id = int(attrs.get('id'))
        lat = float(attrs.get('lat'))
        lon = float(attrs.get('lon'))
        self.nodes[id] = (lat,lon)
    elif name == 'nd':
      """Nodes within a way -- add them to a list"""
      self.waynodes.append(int(attrs.get('ref')))
    elif name == 'tag':
      """Tags - store them in a hash"""
      k,v = (attrs.get('k'), attrs.get('v'))
      if not k in ('created_by'):
        self.tags[k] = v

  def endElement(self, name):
    """Handle ways in the OSM data"""
    if name == 'way':
      highway = self.equivalent(self.tags.get('highway', ''))
      railway = self.equivalent(self.tags.get('railway', ''))
      oneway = self.tags.get('oneway', '')
      reversible = not oneway in('yes','true','1')

      # Calculate what vehicles can use this route
      access = {}
      access['cycle'] = highway in ('primary','secondary','tertiary','unclassified','minor','cycleway','residential', 'track','service')
      access['car'] = highway in ('motorway','trunk','primary','secondary','tertiary','unclassified','minor','residential', 'service')
      access['train'] = railway in('rail','light_rail','subway')
      access['foot'] = access['cycle'] or highway in('footway','steps')
      access['horse'] = highway in ('track','unclassified','bridleway')

      # Store routing information
      last = -1
      for i in self.waynodes:
        if last != -1:
          #print "%d -> %d & v.v." % (last, i)
          for routeType in self.routeTypes:
            if(access[routeType]):
              weight = getWeight(routeType, highway)
              self.addLink(last, i, routeType, weight)
              if reversible or routeType == 'foot':
                self.addLink(i, last, routeType, weight)
        last = i

      # Store map information
      if(self.storeMap):
        wayType = self.WayType(self.tags)
        if(wayType):
          self.ways.append({ \
            't':wayType,
            'n':self.waynodes})

  def addLink(self,fr,to, routeType, weight=1):
    """Add a routeable edge to the scenario"""
    self.routeablefrom(fr,routeType)
    try:
      if to in self.routing[routeType][fr].keys():
        return
      self.routing[routeType][fr][to] = weight
    except KeyError:
      self.routing[routeType][fr] = {to: weight}

  def WayType(self, tags):
    # Look for a variety of tags (priority order - first one found is used)
    for key in ('highway','railway','waterway','natural'):
      value = tags.get('highway', '')
      if value:
        return(self.equivalent(value))
    return('')
  def equivalent(self,tag):
    """Simplifies a bunch of tags to nearly-equivalent ones"""
    equivalent = { \
      "primary_link":"primary",
      "trunk":"primary",
      "trunk_link":"primary",
      "secondary_link":"secondary",
      "tertiary":"secondary",
      "tertiary_link":"secondary",
      "residential":"unclassified",
      "minor":"unclassified",
      "steps":"footway",
      "driveway":"service",
      "pedestrian":"footway",
      "bridleway":"cycleway",
      "track":"cycleway",
      "arcade":"footway",
      "canal":"river",
      "riverbank":"river",
      "lake":"river",
      "light_rail":"railway"
      }
    try:
      return(equivalent[tag])
    except KeyError:
      return(tag)

  def routeablefrom(self,fr,routeType):
    self.routeableNodes[routeType][fr] = 1

  def findNode(self,lat,lon,routeType):
    """Find the nearest node to a point.
    Filters for nodes which have a route leading from them"""
    maxDist = 1000
    nodeFound = None
    for id in self.routeableNodes[routeType].keys():
      if id not in self.nodes:
        print "Ignoring undefined node %s" % id
        continue
      n = self.nodes[id]
      dlat = n[0] - lat
      dlon = n[1] - lon
      dist = dlat * dlat + dlon * dlon
      if(dist < maxDist):
        maxDist = dist
        nodeFound = id
    return(nodeFound)

# Parse the supplied OSM file
if __name__ == "__main__":
  #print "Loading data..."
  #data = LoadOsm(sys.argv[1], True)
  #print data.report()
  #print "Saving binary..."
  #data.savebin("data/routing.bin")
  print "Loading binary..."
  data2 = LoadOsm(None, False)
  data2.loadbin("data/routing.bin")
  print "Done"
