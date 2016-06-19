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

@file    fullnet/people.py
@author  Mic Bowman
@date    2014-02-04

This file contains the programmatic specification of the fullnet
social framework including people and businesses.

"""

import os, sys
import time
import traceback
import inspect
import logging

from multiprocessing import Pool

sys.path.append(os.path.join(os.environ.get("SUMO_HOME"), "tools"))
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))

#sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
#sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

from applications.mobdat.common.Utilities import GenName
from applications.mobdat.common.graph import Generator, SocialEdges, SocialNodes
from applications.mobdat.common.graph.Propagator import PropagateAveragePreference
from applications.mobdat.common.graph.SocialDecoration import BusinessProfileDecoration, BusinessType

import random, math

logger = logging.getLogger('people')
if 'world' not in globals() and 'world' not in locals():
    world = None
    exit('no world variable')

if 'laysettings' not in globals() and 'laysettings' not in locals():
    laysettings = None
    exit('no laysettings variable')


# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
wprof = world.AddPersonProfile('worker')
sprof = world.AddPersonProfile('student')
hprof = world.AddPersonProfile('homemaker')

pmap = {}
pmap['worker'] = wprof
pmap['student'] = sprof
pmap['homemaker'] = hprof

for vtype in laysettings.VehicleTypes.itervalues() :
    for ptype in vtype.ProfileTypes :
        pmap[ptype].VehicleType.AddVehicleType(vtype.Name, vtype.Rate)

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

# -----------------------------------------------------------------
def IrwinHallDistribution(mean) :
    count = 5
    v = math.fsum([random.random() for n in range(0, count)]) / float(count)
    # v is now distributed across the range 0 to 1
    if v < 0.5 :
        return v * mean / 0.5
    else :
        return mean + (v - 0.5) * (1 - mean) / 0.5

# -----------------------------------------------------------------
RulePreferences = {
    'CoffeeBeforeWork' : 0.6,
    'LunchDuringWork' : 0.75,
    'RestaurantAfterWork' : 0.8,
    'ShoppingTrip' : 0.8
    }

def SetRulePreferences(person) :
    global RulePreferences

    for key, val in RulePreferences.iteritems() :
        person.Preference.SetWeight('rule_' + key, IrwinHallDistribution(val))

# -----------------------------------------------------------------
ResidentialNodes = None

def PlacePerson(person) :
    global ResidentialNodes
    if not ResidentialNodes :
        ResidentialNodes = world.FindNodes(nodetype = 'ResidentialLocation')

    bestloc = None
    bestfit = 0
    for location in ResidentialNodes :
        fitness = location.ResidentialLocation.Fitness(person)
        if fitness > bestfit :
            bestfit = fitness
            bestloc = location

    if bestloc :
        endpoint = bestloc.ResidentialLocation.AddResident(person)
        world.SetResidence(person, endpoint)

    return bestloc

# -----------------------------------------------------------------
def PlacePeople() :
    global world

    profile = world.FindNodeByName('worker')

    bizlist = {}
    for name, biz in world.IterNodes(nodetype = 'Business') :
        bizlist[name] = biz

    people = 0
    for name, biz in bizlist.iteritems() :
        bprof = biz.EmploymentProfile
        for job, demand in bprof.JobList.iteritems() :
            for _ in range(0, demand) :
                people += 1
                name = GenName(wprof.Name)
                person = world.AddPerson(name, wprof)
                world.SetEmployer(person, biz)

                SocialNodes.Person.SetJob(person, job)
                SocialNodes.Person.SetVehicle(person, wprof.VehicleType.PickVehicleType())
                SetRulePreferences(person)

                location = PlacePerson(person)
                if not location :
                    logger.warn('ran out of residences after %d people', people)
                    return

    logger.info('created %d people', people)

PlacePeople()

# -----------------------------------------------------------------
def ConnectPeople(people, edgefactor, quadrants, edgeweight = 0.5) :
    """
    Args:
        people -- list of SocialNodes.People objects
        edgefactor -- relative number of edges between people
        quadrants -- vector integers that distributes the density of small world effects
    """

    global world
    weightgen = Generator.GaussianWeightGenerator(edgeweight)
    Generator.RMAT(world, people, edgefactor, quadrants, weightgen = weightgen, edgetype = SocialEdges.ConnectedTo)

# Connect the world
ConnectPeople(world.FindNodes(nodetype = 'Person'), 2, (4, 5, 6, 7), 0.4)

# Connect people who work at the same business
for name, biz in world.IterNodes(nodetype = 'Business') :
    employees = []
    for edge in biz.IterInputEdges(edgetype = 'EmployedBy') :
        employees.append(edge.StartNode)

    try :
        ConnectPeople(employees, 3, (4, 5, 6, 7), 0.7)
    except :
        logger.warn('failed to create social network for {0} employees of business {1}'.format(len(employees), name))

# Connect people who live in the same area
# TBD

# -----------------------------------------------------------------
bizcache = {}
people = {}
def PropagateBusinessPreference(people, biztype, bizclass, seedsize = (7, 13)) :
    global bizcache, world

    if (biztype, bizclass) not in bizcache :
        bizcache[(biztype, bizclass)] = BusinessProfileDecoration.FindByType(world, biztype, bizclass)
    bizlist = bizcache[(biztype, bizclass)]
    incr = len(people) / 100.0
    bizcount = len(bizlist)
    ret_people = []
    for biz in bizlist :
        #logger.info('generating preferences for {0}, {1} remaining'.format(biz.Name, bizcount))
        seedcount = random.randint(int(incr * seedsize[0]), int(incr * seedsize[1]))
        seeds = set(random.sample(people, seedcount))
        # Propagator.PropagateMaximumPreference(seeds, biz.Name, (0.3, 0.9), 0.1)
        PropagateAveragePreference(seeds, biz.Name, (0.7, 0.9), 0.1)
        bizcount -= 1
        for person in seeds:
            ret_people.append((person.Name, biz.Name, person.Preference.GetWeight(biz.Name)))
    return ret_people

def MultiprocessPropagation(btype,sclass):
    ppl = world.FindNodes(nodetype = 'Person')
    res_value = PropagateBusinessPreference(ppl,btype,sclass)
    return res_value

people = world.FindNodes(nodetype = 'Person')
pool = Pool(processes=8)

args = ((BusinessType.Food,'coffee'),(BusinessType.Food,'fastfood'),(BusinessType.Food,'small-restaurant'),
    (BusinessType.Food,'large-restaurant'),(BusinessType.Service,None))
results = []

for arg in args:
    results.append(pool.apply_async(MultiprocessPropagation, arg))

for res in results:
    try:
        solution = res.get()
        for avg_pref in solution:
            node = world.FindNodeByName(avg_pref[0])
            node.Preference.SetWeight(avg_pref[1],avg_pref[2])
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=None, file=sys.stdout)
        raise

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
logger.info("Loaded fullnet people builder extension file")
