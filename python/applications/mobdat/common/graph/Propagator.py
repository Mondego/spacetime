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

@file    Propagator.py
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

import random
import time, platform

logger = logging.getLogger(__name__)


from heapq import heapify, heappush, heappop

class priority_dict(dict):
    """Dictionary that can be used as a priority queue.

    Keys of the dictionary are items to be put into the queue, and values
    are their respective priorities. All dictionary methods work as expected.
    The advantage over a standard heapq-based priority queue is
    that priorities of items can be efficiently updated (amortized O(1))
    using code as 'thedict[item] = new_priority.'

    The 'smallest' method can be used to return the object with lowest
    priority, and 'pop_smallest' also removes it.

    The 'sorted_iter' method provides a destructive sorted iterator.
    """

    def __init__(self, *args, **kwargs):
        super(priority_dict, self).__init__(*args, **kwargs)
        self._rebuild_heap()

    def _rebuild_heap(self):
        self._heap = [(v, k) for k, v in self.iteritems()]
        heapify(self._heap)

    def pop_smallest(self):
        """Return the item with the lowest priority and remove it.

        Raises IndexError if the object is empty.
        """

        heap = self._heap
        v, k = heappop(heap)
        while k not in self or self[k] != v:
            v, k = heappop(heap)
        del self[k]
        return k

    def __setitem__(self, key, val):
        # We are not going to remove the previous value from the heap,
        # since this would have a cost O(n).

        super(priority_dict, self).__setitem__(key, val)

        if len(self._heap) < 2 * len(self):
            heappush(self._heap, (val, key))
        else:
            # When the heap grows larger than 2 * len(self), we rebuild it
            # from scratch to avoid wasting too much memory.
            self._rebuild_heap()

    def update(self, *args, **kwargs):
        # Reimplementing dict.update is tricky -- see e.g.
        # http://mail.python.org/pipermail/python-ideas/2007-May/000744.html
        # We just rebuild the heap from scratch after passing to super.

        super(priority_dict, self).update(*args, **kwargs)
        self._rebuild_heap()

# -----------------------------------------------------------------
def _Timer() :
    return time.clock() if platform.system() == 'Windows' else time.time()

# -----------------------------------------------------------------
def PropagateMaximumPreference(seeds, preference, seedweight, minweight) :
    """
    Propagate preferences through a social network by pushing the maximum
    influence to each adjacent node in the network

    Args:
        seeds -- list of seed nodes
        preference -- string key of the preference to be propagated
        seedweight -- initial preference given to the seed nodes
        minweight -- the minimum weight allowed to be propagated
    """
    stime = _Timer()
    nodeq = priority_dict()

    processed = 0
    for seed in seeds :
        weight = random.uniform(seedweight[0], seedweight[1])
        seed.Preference.SetWeight(preference, weight)
        nodeq[seed] = 1.0 - weight

    while nodeq :
        processed += 1
        node = nodeq.pop_smallest()
        weight = node.Preference.GetWeight(preference, 0.0)

        for edge in node.IterOutputEdges(edgetype = 'ConnectedTo') :
            oldweight = edge.EndNode.Preference.GetWeight(preference, 0.0)
            newweight = weight * edge.Weight.Weight
            if newweight > oldweight and newweight > minweight :
                edge.EndNode.Preference.SetWeight(preference, newweight)
                nodeq[edge.EndNode] = 1.0 - newweight

    logger.info('total nodes processed {0} for preference {1}'.format(processed, preference))
    logger.info('time taken {0}'.format(_Timer() - stime))

# -----------------------------------------------------------------
def PropagateAveragePreference(seeds, preference, seedweight, mindelta) :
    """
    Propagate preferences through a social network by computing the
    average influence from adjacent nodes in the network

    Args:
        seeds -- list of seed nodes
        preference -- string key of the preference to be propagated
        seedweight -- initial preference given to the seed nodes
        mindelta -- the minimum difference that can be propagated
    """

    stime = _Timer()

    # priority queue ordered by the adjacent delta that caused it to be added
    nodequeue = priority_dict()
    lasttime = dict()

    # set the initial weights for the seed nodes, add adjacent nodes
    # to the queue to be processed
    for seed in seeds :
        weight = random.uniform(seedweight[0], seedweight[1])
        seed.Preference.SetWeight(preference, weight)
        for edge in seed.IterOutputEdges(edgetype = 'ConnectedTo') :
            nodequeue[edge.EndNode] = 1.0 - weight

    # process the queue, this is a little dangerous because of the
    # potential for lack of convergence or at least the potential
    # for convergence taking a very long time
    totalprocessed = 0
    while len(nodequeue) > 0 :
        totalprocessed += 1

        node = nodequeue.pop_smallest()
        if node in seeds :
            continue

        # oldweight = node.Preference.GetWeight(preference, 0.0)
        oldweight = lasttime.get(node, 0.0)

        # compute the weight for this node as the weighted average
        # of all the nodes that point to it
        count = 0
        aggregate = 0
        for edge in node.IterInputEdges(edgetype = 'ConnectedTo') :
            count += 1
            aggregate += edge.StartNode.Preference.GetWeight(preference, 0.0) * edge.Weight.Weight

        newweight = aggregate / count
        # if newweight < oldweight :
        #     logger.warn('{0} less than {1}'.format(newweight, oldweight))
        #     continue

        node.Preference.SetWeight(preference, newweight)

        # only propagate the change if the delta is large enough to matter
        if newweight - oldweight > mindelta :
            lasttime[node] = newweight
            for edge in node.IterOutputEdges(edgetype = 'ConnectedTo') :
                nodequeue[edge.EndNode] = 1.0 - (newweight - oldweight)

        # node.Preference.SetWeight(preference, newweight)
        # if newweight > mindelta :
        #     for edge in node.IterOutputEdges(edgetype = 'ConnectedTo') :
        #         nodequeue.add(edge.EndNode)

    logger.info('total nodes process {0} for preference {1}'.format(totalprocessed, preference))
    logger.info('time taken {0}'.format(_Timer() - stime))



# -----------------------------------------------------------------
def xPropagateAveragePreference(seeds, preference, seedweight, mindelta) :
    """
    Propagate preferences through a social network by computing the
    average influence from adjacent nodes in the network

    Args:
        seeds -- list of seed nodes
        preference -- string key of the preference to be propagated
        seedweight -- initial preference given to the seed nodes
        mindelta -- the minimum difference that can be propagated
    """

    stime = _Timer()

    # priority queue ordered by the adjacent delta that caused it to be added
    nodequeue = set()
    lasttime = dict()

    # set the initial weights for the seed nodes, add adjacent nodes
    # to the queue to be processed
    for seed in seeds :
        weight = random.uniform(seedweight[0], seedweight[1])
        seed.Preference.SetWeight(preference, weight)
        for edge in seed.IterOutputEdges(edgetype = 'ConnectedTo') :
            nodequeue.add(edge.EndNode)

    # process the queue, this is a little dangerous because of the
    # potential for lack of convergence or at least the potential
    # for convergence taking a very long time
    totalprocessed = 0
    while len(nodequeue) > 0 :
        totalprocessed += 1

        node = nodequeue.pop()
        if node in seeds :
            continue

        # oldweight = node.Preference.GetWeight(preference, 0.0)
        oldweight = lasttime.get(node, 0.0)

        # compute the weight for this node as the weighted average
        # of all the nodes that point to it
        count = 0
        aggregate = 0
        for edge in node.IterInputEdges(edgetype = 'ConnectedTo') :
            count += 1
            aggregate += edge.StartNode.Preference.GetWeight(preference, 0.0) * edge.Weight.Weight

        newweight = aggregate / count
        # if newweight < oldweight :
        #     logger.warn('{0} less than {1}'.format(newweight, oldweight))
        #     continue

        node.Preference.SetWeight(preference, newweight)

        # only propagate the change if the delta is large enough to matter
        if newweight - oldweight > mindelta :
            lasttime[node] = newweight
            for edge in node.IterOutputEdges(edgetype = 'ConnectedTo') :
                nodequeue.add(edge.EndNode)

        # node.Preference.SetWeight(preference, newweight)
        # if newweight > mindelta :
        #     for edge in node.IterOutputEdges(edgetype = 'ConnectedTo') :
        #         nodequeue.add(edge.EndNode)

    logger.info('total nodes process {0} for preference {1}'.format(totalprocessed, preference))
    logger.info('time taken {0}'.format(_Timer() - stime))

