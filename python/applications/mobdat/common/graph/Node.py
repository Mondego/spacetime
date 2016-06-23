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

@file    Node.py
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

from applications.mobdat.common.graph.GraphObject import GraphObject

from applications.mobdat.common.Utilities import GenName

logger = logging.getLogger(__name__)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
def GenNodeName(prefix = 'node') :
    return GenName(prefix)

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class Node(GraphObject) :

    # -----------------------------------------------------------------
    @staticmethod
    def Load(graph, ninfo) :
        node = Node(name = ninfo['Name'])
        node.LoadDecorations(graph, ninfo)

        return node

    # -----------------------------------------------------------------
    @staticmethod
    def LoadMembers(graph, ninfo) :
        node = graph.Nodes[ninfo['Name']]
        for mname in ninfo['Members'] :
            node.AddMember(graph.FindByName(mname))

    # -----------------------------------------------------------------
    def __init__(self, members = [], name = None, prefix = 'node') :
        if not name : name = GenNodeName(prefix)
        GraphObject.__init__(self, name)

        self.Members = set()
        for member in members :
            self.AddMember(member)

    # -----------------------------------------------------------------
    def AddMember(self, member) :
        # add to the object the reference to the group
        member.AddToCollection(self)

        # add to the group the reference to the object
        if member not in self.Members:
            self.Members.add(member)
        else:
            raise

    # -----------------------------------------------------------------
    def DropMember(self, member) :
        # drop the reference to the collection from the object
        member.DropFromCollection(self)

        # drop the object
        self.Members.remove(member)

    # -----------------------------------------------------------------
    def Dump(self) :
        result = GraphObject.Dump(self)

        result['Members'] = []
        for member in self.Members :
            result['Members'].append(member.Name)

        return result

