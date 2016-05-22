'''
Created on May 16, 2016

@author: Arthur Valadares
'''
import copy
import logging
import os
from random import choice

from datamodel.common.datamodel import Vehicle, Quaternion
from datamodel.nodesim.datamodel import Vector3
from spacetime_local import IApplication
from spacetime_local.declarations import Getter, Tracker
import OpenSimRemoteControl
import sys
import time

import uuid
import json
from uuid import UUID

def fetch_assets(host, user, pwd, schema, fpath):
    import MySQLdb
    assets = {}
    assets["Sedan"] = {}
    assets["Truck"] = {}
    assets["Taxi"] = {}

    # Open database connection
    db = MySQLdb.connect(host, user, pwd, schema)

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # execute SQL query using execute() method.
    cursor.execute("select inventoryName, assetID from inventoryitems where inventoryName like '%sedan%'")

    # Fetch a single row using fetchone() method.
    sedan_data = cursor.fetchall()
    for tup in sedan_data:
        assets["Sedan"][tup[0]] = tup[1]

    # execute SQL query using execute() method.
    cursor.execute("select inventoryName, assetID from inventoryitems where inventoryName like '%truck%'")

    truck_data = cursor.fetchall()
    for tup in truck_data:
        assets["Truck"][tup[0]] = tup[1]

    # execute SQL query using execute() method.
    cursor.execute("select inventoryName, assetID from inventoryitems where inventoryName like '%taxi%'")

    taxi_data = cursor.fetchall()
    for tup in taxi_data:
        assets["Taxi"][tup[0]] = tup[1]


    base_path = os.path.dirname(os.path.realpath(__file__))
    final_path = os.path.join(base_path, fpath)

    with open(final_path, 'w') as outfile:
        json.dump(assets, outfile)

    # disconnect from server
    db.close()

@Getter(Vehicle)
class OpenSimPuller(IApplication.IApplication):
    def __init__(self, frame, args):
        self.frame = frame
        self.logger = logging.getLogger(__name__)
        self.endpoint = args.url + "/Dispatcher/"
        self.lifespan = 3600000
        self.avname = args.user
        self.passwd = args.password
        self.scene_name = args.scene

        base_path = os.path.dirname(os.path.realpath(__file__))
        final_path = os.path.join(base_path, os.path.join('data', 'assets_' + args.dbhost + '.js'))
        logging.info('Getting assets from ' + final_path)
        if args.fetch:
            fetch_assets(args.dbhost, args.dbuser, args.dbpassword, args.dbschema, final_path)
        self.assets = json.load(open(final_path))
        self.step = 0

        self.carids = {}

    def initialize(self):
        self.AuthByUserName()

    def update(self):
        new_vehicles = self.frame.get_new(Vehicle)
        #print "new:", new_vehicles
        for v in new_vehicles:
            assetid = UUID(choice(self.assets["Sedan"].values()))
            result = self.rc.CreateObject(
                assetid, objectid=v.ID, name=v.Name, async=True,
                pos = [v.Position.X, v.Position.Y, v.Position.Z],
                vel = [v.Velocity.X, v.Velocity.Y, v.Velocity.Z])
            self.logger.info("New vehicle: %s", v.ID)
            #if not result:
            #    self.logger.error("could not create vehicle %s", v.ID)

        mod_vehicles = self.frame.get_mod(Vehicle)
        update_list = []
        for v in mod_vehicles:
            #self.logger.info("Vehicle %s is in %s", v.ID, v.Position)
            #self.logger.info("[%s] Pulller Position: %s", self.step, v.Position)
            vpos = [v.Position.X, v.Position.Y, v.Position.Z]
            vvel = [v.Velocity.X, v.Velocity.Y, v.Velocity.Z]
            vrot = Quaternion.FromVector3(v.Velocity).ToList()
            update_list.append(OpenSimRemoteControl.BulkUpdateItem(v.ID, vpos, vvel, vrot))
            #if not result:
            #    self.logger.error("error updating vehicle %s", v.ID)

        del_vehicles = self.frame.get_deleted(Vehicle)
        for v in del_vehicles:
            #self.logger.info("Deleting vehicle %s", v.ID)
            result = self.rc.DeleteObject(v.ID, async=False)

        result = self.rc.BulkDynamics(update_list, False)
        self.step += 1
        #print "result is ", result

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
