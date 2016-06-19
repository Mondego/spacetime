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

@file    Decoration.py
@author  Mic Bowman
@date    2013-12-03

This file defines routines used to build features of a mobdat traffic
network such as building a grid of roads.

"""

import os, sys

# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class Decoration :
    DecorationName = 'Decoration'

    # -----------------------------------------------------------------
    @staticmethod
    def Load(graph, info) :
        return(Decoration())

    # -----------------------------------------------------------------
    def __init__(self) :
        self.HostObject = None

    # -----------------------------------------------------------------
    def SetHostObject(self, obj) :
        """
        Args:
            obj -- object of type Graph.GraphObject
        """
        self.HostObject = obj

    # -----------------------------------------------------------------
    def Dump(self) :
        result = dict()
        result['__TYPE__'] = self.DecorationName

        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class NodeTypeDecoration(Decoration) :
    DecorationName = 'NodeType'

    # -----------------------------------------------------------------
    @staticmethod
    def Load(graph, info) :
        return NodeTypeDecoration(info['Name'])

    # -----------------------------------------------------------------
    def __init__(self, name) :
        Decoration.__init__(self)

        self.Name = name

    # -----------------------------------------------------------------
    def Dump(self) :
        result = Decoration.Dump(self)
        result['Name'] = self.Name

        return result

## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EdgeTypeDecoration(Decoration) :
    DecorationName = 'EdgeType'

    # -----------------------------------------------------------------
    @staticmethod
    def Load(graph, info) :
        return EdgeTypeDecoration(info['Name'])

    # -----------------------------------------------------------------
    def __init__(self, name) :
        Decoration.__init__(self)
        self.Name = name

    # -----------------------------------------------------------------
    def Dump(self) :
        result = Decoration.Dump(self)
        result['Name'] = self.Name

        return result


## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class EdgeWeightDecoration(Decoration) :
    DecorationName = 'Weight'

    # -----------------------------------------------------------------
    @staticmethod
    def Load(graph, info) :
        return EdgeWeightDecoration(info['Weight'])

    # -----------------------------------------------------------------
    def __init__(self, weight = 1.0) :
        if weight < 0 or 1 < weight :
            raise ValueError('invalid preference weight')

        Decoration.__init__(self)
        self.Weight = weight

    # -----------------------------------------------------------------
    def AddWeight(self, adjustment) :
        self.Weight = self.Weight + adjustment - self.Weight * adjustment
        # self.Weight += adjustment

    # -----------------------------------------------------------------
    def Dump(self) :
        result = Decoration.Dump(self)
        result['Weight'] = self.Weight

        return result


## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
## XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
CommonDecorations = [Decoration, NodeTypeDecoration, EdgeTypeDecoration, EdgeWeightDecoration]

