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

@file    Generator.py
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

from Edge import *
from applications.mobdat.common.ValueTypes import WeightedChoice

import random, math

logger = logging.getLogger(__name__)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
def _RequireEdge(graph, node1, node2, weight, edgetype) :
    edge = edgetype(node1, node2, weight)
    if edge.Name in graph.Edges :
        if edge.NodeType.Name != edgetype.__name__ :
            raise NameError('duplicate social edge with different type; {0} to {1}'.format(edge.NodeType.Name, edgetype.__name__))

        edge.Weight.AddWeight(weight)
        return edge

    return graph.AddEdge(edge)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class WeightGenerator :

    # -----------------------------------------------------------------
    def __init__(self, weight, minweight = 0.0, maxweight = 1.0) :
        self.MinWeight = minweight
        self.MaxWeight = maxweight

        if weight < self.MinWeight or self.MaxWeight < weight :
            raise ValueError('Edge weight must be between %s and %s', self.MinWeight, self.MaxWeight)

        self.Weight = weight

    # -----------------------------------------------------------------
    def GenWeight(self, snode, enode) :
        return self.Weight

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class GaussianWeightGenerator(WeightGenerator) :

    # -----------------------------------------------------------------
    def __init__(self, weight = 0.5, deviation = 0.2, minweight = 0.0, maxweight = 1.0) :
        WeightGenerator.__init__(self, weight, minweight, maxweight)
        self.Deviation = deviation

    # -----------------------------------------------------------------
    def GenWeight(self, snode, enode) :
        while True :
            value = random.gauss(self.Weight, self.Deviation)
            if self.MinWeight <= value and value <= self.MaxWeight :
                return value

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
def RMAT(graph, nodelist, edgefactor = 3, quadrants = (4, 8, 12, 16), weightgen = None, edgetype = None) :
    """
    Generate a social graph from the provided nodes using the R-MAT, recursive
    matrix routine (http://www.cs.cmu.edu/~christos/PUBLICATIONS/siam04.pdf)

    Args:
        graph -- Graph object
        nodelist -- list of Node objects
        edgefactor -- integer, relative number of edges between nodes
        quadrants -- vector of integers that distributes the density of small world effects
        edgetype -- subtype of Edge.WeightedEdge
    """

    if not weightgen : weightgen = WeightGenerator(0.5)
    if not edgetype : edgetype = WeightedEdge

    nodes = nodelist[:]     # make a copy so we can pop off elements as necessary
    edgecount = edgefactor * len(nodes)
    scale = int(math.log(len(nodes), 2) + 1)

    quadpicker = WeightedChoice({0 : quadrants[0], 1 : quadrants[1], 2 : quadrants[2], 3 : quadrants[3]})
    quadmap = {}
    edgemap = {}

    edges = 0

    while nodes or edges < edgecount :
        n1 = 0
        n2 = 0

        for j in range(scale) :
            quadrant = quadpicker.Choose()
            n1 = (n1 << 1) | (quadrant >> 1)
            n2 = (n2 << 1) | (quadrant & 1)

        # make sure we have nodes for the quadrant identifiers
        if n1 not in quadmap :
            quadmap[n1] = nodes.pop() if nodes else random.choice(quadmap.values())
        node1 = quadmap[n1]

        if n2 not in quadmap :
            quadmap[n2] = nodes.pop() if nodes else random.choice(quadmap.values())
        node2 = quadmap[n2]

        # and now create the edges between the nodes
        edges += 1
        if edges % 100 == 0 :
            logger.debug('created %d of %d social edges so far', edges, edgecount)

        if (node1, node2) not in edgemap :
            edgemap[(node1, node2)] = _RequireEdge(graph, node1, node2, weightgen.GenWeight(node1, node2), edgetype)
            edgemap[(node2, node1)] = _RequireEdge(graph, node2, node1, weightgen.GenWeight(node2, node1), edgetype)
        else :
            edgemap[(node1, node2)].Weight.AddWeight(weightgen.GenWeight(node1, node2))
            edgemap[(node2, node1)].Weight.AddWeight(weightgen.GenWeight(node2, node1))

    logger.info('created %d social connections', edges)

