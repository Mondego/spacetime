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

@file    Edge.py
@author  Mic Bowman
@date    2013-12-03

This file defines routines used to build features of a mobdat traffic
network such as building a grid of roads.

"""

import os, sys
import logging
from applications.mobdat.common.graph.Decoration import EdgeWeightDecoration
from applications.mobdat.common.graph.GraphObject import GraphObject


# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

logger = logging.getLogger(__name__)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
def GenEdgeName(snode, enode) :
    return snode.Name + '=O=' + enode.Name

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class Edge(GraphObject) :

    # -----------------------------------------------------------------
    @staticmethod
    def Load(graph, einfo) :
        sname = einfo['StartNode']
        snode = graph.Nodes[sname] if sname in graph.Nodes else graph.Collections[sname]
        ename = einfo['EndNode']
        enode = graph.Nodes[ename] if ename in graph.Nodes else graph.Collections[ename]
        edge = Edge(snode, enode, einfo['Name'])

        edge.LoadDecorations(graph, einfo)

        return edge

    # -----------------------------------------------------------------
    def __init__(self, snode, enode, name = None) :
        if not name : name = GenEdgeName(snode, enode)
        GraphObject.__init__(self, name)

        self.StartNode = snode
        self.EndNode = enode

        snode.AddOutputEdge(self)
        enode.AddInputEdge(self)

    # -----------------------------------------------------------------
    def Dump(self) :
        result = GraphObject.Dump(self)

        result['StartNode'] = self.StartNode.Name
        result['EndNode'] = self.EndNode.Name

        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class WeightedEdge(Edge) :

    # -----------------------------------------------------------------
    def __init__(self, node1, node2, weight = 1.0) :
        """
        Args:
            node1 -- object of type SocialNodes.Node
            node2 -- object of type SocialNodes.Node
        """
        Edge.__init__(self, node1, node2)

        self.AddDecoration(EdgeWeightDecoration(weight))

