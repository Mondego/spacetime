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

@file    Graph.py
@author  Mic Bowman
@date    2013-12-03

This file defines routines used to build features of a mobdat traffic
network such as building a grid of roads.

"""

import os, sys
import logging
from applications.mobdat.common.graph.Decoration import CommonDecorations
import re

# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

from applications.mobdat.common.graph.Node import Node
from applications.mobdat.common.graph.Edge import Edge
import json

logger = logging.getLogger(__name__)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class Graph :

    # -----------------------------------------------------------------
    @staticmethod
    def LoadFromFile(filename) :
        with open(filename, 'r') as fp :
            netdata = json.load(fp)

        graph = Graph()
        graph.Load(netdata)

        return graph

    # -----------------------------------------------------------------
    def __init__(self) :
        self.DecorationMap = {}

        self.Edges = {}
        self.Nodes = {}

        for dtype in CommonDecorations :
            self.AddDecorationHandler(dtype)

    # -----------------------------------------------------------------
    def Dump(self) :
        result = dict()

        nodes = []
        for node in self.Nodes.itervalues() :
            nodes.append(node.Dump())
        result['Nodes'] = nodes

        edges = []
        for edge in self.Edges.itervalues() :
            edges.append(edge.Dump())
        result['Edges'] = edges

        return result

    # -----------------------------------------------------------------
    def Load(self, info) :
        """
        Load the graph from the dictionary representation
        """
        for ninfo in info['Nodes'] :
            self.AddNode(Node.Load(self, ninfo))

        for einfo in info['Edges'] :
            self.AddEdge(Edge.Load(self, einfo))

        # setting up the membership after creating all the
        # collections makes it possible to have collections within collections
        for ninfo in info['Nodes'] :
            Node.LoadMembers(self, ninfo)

    # =================================================================
    # DECORATION METHODS
    # =================================================================

    # -----------------------------------------------------------------
    def AddDecorationHandler(self, handler) :
        self.DecorationMap[handler.DecorationName] = handler

    # -----------------------------------------------------------------
    def LoadDecoration(self, dinfo) :
        handler = self.DecorationMap[dinfo['__TYPE__']]
        if handler :
            return handler.Load(self, dinfo)

        # if we don't have a handler, thats ok we just dont load the
        # decoration, note the missing decoration in the logs however
        logger.info('no decoration handler found for type %s', dinfo['__TYPE__'])
        return None

    # =================================================================
    # UTILITY METHODS
    # =================================================================

    # -----------------------------------------------------------------
    def FindByName(self, name) :
        if name in self.Nodes :
            return self.Nodes[name]
        elif name in self.Edges :
            return self.Edges[name]
        else :
            raise NameError("graph contains no object named %s" % name)

    # =================================================================
    # NODE methods
    # =================================================================

    # -----------------------------------------------------------------
    def AddNode(self, node) :
        if node.Name in self.Nodes :
            raise NameError("node with name {0} already exists in the graph".format(node.Name))

        self.Nodes[node.Name] = node

    # -----------------------------------------------------------------
    def DropNode(self, node) :
        # need to use values because dropping the member in the collection
        # will change the list of connections here

        # drop this node from other nodes where it is a member
        for collection in node.Collections.values() :
            collection.DropMember(node)

        # drop all nodes that are a member of this one
        for obj in node.Members :
            node.DropMember(obj)

        for edge in node.InputEdges[:] :
            self.DropEdge(edge)

        for edge in node.OutputEdges[:] :
            self.DropEdge(edge)

        del self.Nodes[node.Name]

    # -----------------------------------------------------------------
    def FindNodeByName(self, name) :
        if name in self.Nodes :
            return self.Nodes[name]
        else :
            raise NameError("graph contains no node named %s" % name)

    # -----------------------------------------------------------------
    def DropNodeByName(self, name) :
        if name not in self.Nodes :
            return

        self.DropNode(self.Nodes[name])

    # -----------------------------------------------------------------
    def DropNodes(self, pattern = None, nodetype = None) :
        """
        Args:
            pattern -- string representing a regular expression
            nodetype -- string name of a node type
        """
        nodes = self.FindNodes(pattern, nodetype)
        for node in nodes :
            self.DropNode(node)

        return True

    # -----------------------------------------------------------------
    def FindNodes(self, pattern = None, nodetype = None, predicate = None) :
        """
        Args:
            pattern -- string representing a regular expression
            nodetype -- string name of a node type
        """
        nodes = []
        for _, node in self.IterNodes(pattern, nodetype, predicate) :
            nodes.append(node)

        return nodes

    # -----------------------------------------------------------------
    def IterNodes(self, pattern = None, nodetype = None, predicate = None) :
        """
        Args:
            pattern -- string representing a regular expression
            nodetype -- string name of a node type
        """
        for name, node in self.Nodes.iteritems() :
            if nodetype and node.NodeType.Name != nodetype :
                continue

            if pattern and not re.match(pattern, name) :
                continue

            if predicate and not predicate(node) :
                continue

            yield name, node

    # =================================================================
    # EDGE methods
    # =================================================================

    # -----------------------------------------------------------------
    def AddEdge(self, edge) :
        if edge.Name in self.Edges :
            raise NameError("edge with name {0} already exists in the graph".format(edge.Name))

        self.Edges[edge.Name] = edge
        return edge

    # -----------------------------------------------------------------
    def DropEdge(self, edge) :
        # need to use values because dropping the member in the collection
        # will change the list of connections here
        for collection in edge.Collections.values() :
            collection.DropMember(edge)

        edge.StartNode.OutputEdges.remove(edge)
        edge.EndNode.InputEdges.remove(edge)

        del self.Edges[edge.Name]
        return True

    # -----------------------------------------------------------------
    def DropEdgeByName(self, name) :
        if name not in self.Edges :
            return True

        return self.DropEdge(self.Edges[name])

    # -----------------------------------------------------------------
    def DropEdges(self, pattern = None, edgetype = None) :
        """
        Args:
            pattern -- string representing a regular expression
            edgetype -- string name of a edge type
        """
        edges = self.FindEdges(pattern, edgetype)
        for edge in edges :
            self.DropEdge(edge)

        return True

    # -----------------------------------------------------------------
    def FindEdges(self, pattern = None, edgetype = None) :
        """
        Args:
            pattern -- string representing a regular expression
            edgetype -- string name of a edge type
        """
        edges = []
        for name, edge in self.Edges.iteritems() :
            if edgetype and edge.NodeType.Name != edgetype :
                continue

            if pattern and not re.match(pattern, name) :
                continue

            edges.append(edge)

        return edges

    # -----------------------------------------------------------------
    def IterEdges(self, pattern = None, edgetype = None) :
        """
        Args:
            pattern -- string representing a regular expression
            edgetype -- string name of a edge type
        """
        for name, edge in self.Edges.iteritems() :
            if edgetype and edge.NodeType.Name != edgetype :
                continue

            if pattern and not re.match(pattern, name) :
                continue

            yield name, edge


    # -----------------------------------------------------------------
    def DropEdgesByPattern(self, pattern) :
        # need to use items because dropping the member in the collection
        # will change the list of connections here
        for name, edge in self.Edges.items() :
            if re.match(pattern, name) :
                self.DropEdge(edge)

        return True

    # -----------------------------------------------------------------
    def FindEdgeByName(self, name) :
        if name in self.Edges :
            return self.Edges[name]
        else :
            raise NameError("graph contains no edge named %s" % name)

    # -----------------------------------------------------------------
    def FindEdgeBetweenNodes(self, node1, node2) :
        for e in node1.OutputEdges :
            if e.EndNode == node2 :
                return e
        return None


## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
if __name__ == '__main__' :
    """
    from mobdat.common.graph.Node import *
    from mobdat.common.graph.Edge import *
    from mobdat.common.graph.Decoration import *
    from mobdat.common.Utilities import GenNameFromCoordinates

    # -----------------------------------------------------------------
    class TestDecoration(Decoration) :
        DecorationName = 'TestDecoration'

        # -----------------------------------------------------------------
        @staticmethod
        def Load(graph, info) :
            return TestDecoration(info['Value1'], info['Value2'])

        # -----------------------------------------------------------------
        def __init__(self, val1, val2) :
            Decoration.__init__(self)

            self.Value1 = val1
            self.Value2 = val2

        # -----------------------------------------------------------------
        def Dump(self) :
            result = Decoration.Dump(self)

            result['Value1'] = self.Value1
            result['Value2'] = self.Value2

            return result

    # -----------------------------------------------------------------
    class EdgeTypeDecoration(Decoration) :
        DecorationName = 'EdgeType'

        # -----------------------------------------------------------------
        @staticmethod
        def Load(graph, info) :
            return EdgeTypeDecoration(info['Name'], info['Weight'])

        # -----------------------------------------------------------------
        def __init__(self, name, weight) :
            Decoration.__init__(self)

            self.Name = name
            self.Weight = weight

        # -----------------------------------------------------------------
        def Dump(self) :
            result = Decoration.Dump(self)

            result['Name'] = self.Name
            result['Weight'] = self.Weight

            return result


    net1 = Graph()
    net1.AddDecorationHandler(TestDecoration)
    net1.AddDecorationHandler(EdgeTypeDecoration)

    edges1 = Collection(name = 'type1edges')
    edges1.AddDecoration(EdgeTypeDecoration('type1', 25))
    net1.AddCollection(edges1)

    edges2 = Collection(name = 'type2edges')
    edges2.AddDecoration(EdgeTypeDecoration('type2', 5))
    net1.AddCollection(edges2)

    for x in range(0, 5) :
        for y in range(0, 5) :
            node = Node(GenNameFromCoordinates(x, y))
            node.AddDecoration(CoordDecoration(x, y))
            net1.AddNode(node)
            if x > 0 :
                if y > 0 :
                    edge = Edge(node, net1.Nodes[GenNameFromCoordinates(x-1,y-1)])
                    edges1.AddMember(edge)
                    net1.AddEdge(edge)

            d = TestDecoration(x, y)
            node.AddDecoration(d)

    for edge in net1.Edges.itervalues() :
        if edge.EndNode.Coord.X % 2 == 0 :
            edges = edge.FindDecorationProvider('EdgeType')
            edges.DropMember(edge)
            edges2.AddMember(edge)

    net2 = Graph()
    net2.AddDecorationHandler(TestDecoration)
    net2.AddDecorationHandler(EdgeTypeDecoration)

    net2.Load(net1.Dump())

    # print json.dumps(net2.Dump(),indent=2)
    for e in net2.Nodes.itervalues() :
        print "{0} = {1}".format(e.Name, e.TestDecoration.Value1)

    print "type1edges"
    for e in net2.Collections['type1edges'].Members :
        print "{0} has weight {1}".format(e.Name, e.EdgeType.Weight)

    print "type2edges"
    for e in net2.Collections['type2edges'].Members :
        print "{0} has weight {1}".format(e.Name, e.EdgeType.Weight)
    """
