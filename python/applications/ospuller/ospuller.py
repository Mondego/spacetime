'''
Created on May 16, 2016

@author: Arthur Valadares
'''
import copy
import logging
import os
from random import choice

from datamodel.common.datamodel import Vehicle
from datamodel.nodesim.datamodel import Vector3
from spacetime_local import IApplication
from spacetime_local.declarations import Getter
import OpenSimRemoteControl
import sys
import time

import uuid
import json
from uuid import UUID

@Getter(Vehicle)
class OpenSimPuller(IApplication.IApplication):
    def __init__(self, frame):
        self.frame = frame
        self.logger = logging.getLogger(__name__)
        self.endpoint = "http://ucigridb.nacs.uci.edu:9000/Dispatcher/"
        self.lifespan = 3600000
        self.avname = "Arthur Valadares"
        self.passwd = "arthur123"
        self.scene_name = ""
        self.assets = json.load(open('applications/ospuller/assets.js'))

        self.carids = {}

    def initialize(self):
        self.AuthByUserName()

    def update(self):
        new_vehicles = self.frame.get_new(Vehicle)
        for v in new_vehicles:
            assetid = choice(self.assets["Sedan"].values())
            result = self.rc.CreateObject(
                assetid, objectid=v.ID, name=v.Name, async=True,
                pos = [v.Position.X, v.Position.Y, v.Position.Z],
                vel = [v.Velocity.X, v.Velocity.Y, v.Velocity.Z])
            #if not result:
            #    self.logger.error("could not create vehicle %s", v.ID)

        mod_vehicles = self.frame.get_mod(Vehicle)
        for v in mod_vehicles:
            vpos = [v.Position.X, v.Position.Y, v.Position.Z]
            vvel = [v.Velocity.X, v.Velocity.Y, v.Velocity.Z]
            vrot = [] # TODO: Calculate rotation
            update = OpenSimRemoteControl.BulkUpdateItem(v.ID, vpos, vvel, vrot)
            result = self.rc.BulkDynamics(update, True)
            #if not result:
            #    self.logger.error("error updating vehicle %s", v.ID)

        del_vehicles = self.frame.get_deleted(Vehicle)
        for v in del_vehicles:
            result = self.rc.DeleteObject(v.ID, async=True)

    def shutdown(self):
        for v in self.frame.get(Vehicle):
            result = self.rc.DeleteObject(v.ID, async=False)
            if not result:
                self.logger.warn("could not clean up vehicle %s", v.ID)

    def AuthByUserName(self):
        rc = OpenSimRemoteControl.OpenSimRemoteControl(self.endpoint)
        rc.DomainList = ['Dispatcher', 'RemoteControl']
        response = rc.AuthenticateAvatarByName(self.avname,self.passwd,self.lifespan)
        if not response['_Success'] :
            print 'Failed: ' + response['_Message']
            sys.exit(-1)

        expires = response['LifeSpan'] + int(time.time())
        print >> sys.stderr, 'capability granted, expires at %s' % time.asctime(time.localtime(expires))

        print "Capability of %s is %s" % (self.scene_name,response['Capability'])
        self.capability = response['Capability'].encode('ascii')
        self.lifespan = response['LifeSpan']

        rc.Capability = uuid.UUID(self.capability)
        rc.Scene = self.scene_name
        rc.Binary = True
        self.rc = rc
        return True