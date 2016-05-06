#!/usr/bin/python
'''
Created on Feb 19, 2016
author: arthurvaladares, Rohan Achar
'''
from functools import wraps
from flask import Flask, request
from flask.helpers import make_response
from flask_restful import Api, Resource, reqparse
import json
import os
import sys
import signal
import logging, logging.handlers
import urllib2
import time

from store import *

from datamodel.all import DATAMODEL_TYPES

parser = reqparse.RequestParser()
parser.add_argument('update_dict')
parser.add_argument('insert_list')
parser.add_argument('obj')
parser.add_argument('get_types')

class FlaskConfig(object):
    RESTFUL_JSON = {}

    @staticmethod
    def init_app(app):
        app.config['RESTFUL_JSON']['cls'] = app.json_encoder = json.JSONEncoder

app = Flask(__name__)
app.config.from_object(FlaskConfig)
# app.json_encoder = CADISEncoder()
FlaskConfig.init_app(app)
api = Api(app)

def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    server.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def handle_exceptions(f):
    @wraps(f)
    def wrapped(*args, **kwds):
        try:
            ret = f(*args, **kwds)
        except Exception, e:
            logger.exception("Exception handling function %s:", f.func_name)
            raise
        return ret
    return wrapped

class GetAllUpdated(Resource):
    @handle_exceptions
    def get(self, sim):
        args = parser.parse_args()
        types = json.loads(args["get_types"])["types"]
        (all_new, all_updated, all_deleted) = ({}, {}, {})
        for tp in types:
            typeObj = FrameServer.name2class[tp]
            (new, updated, deleted) = FrameServer.Store.get_update(typeObj, sim)
            all_new.update(new)
            all_updated.update(updated)
            all_deleted.update(deleted)
        ret = {}
        ret["new"] = all_new
        ret["updated"] = all_updated
        ret["deleted"] = all_deleted
        return ret

class GetAllTracked(Resource):
    @handle_exceptions
    def get(self, sim):
        args = parser.parse_args()
        types = json.loads(args["get_types"])["types"]
        (all_new, all_updated, all_deleted) = ({}, {}, {})
        for tp in types:
            typeObj = FrameServer.name2class[tp]
            (new, updated, deleted) = FrameServer.Store.get_update(typeObj, sim, tracked_only=True)
            all_new.update(new)
            all_updated.update(updated)
            all_deleted.update(deleted)
        ret = {}
        ret["new"] = all_new
        ret["updated"] = all_updated
        ret["deleted"] = all_deleted
        return ret

class GetUpdated(Resource):
    @handle_exceptions
    def get(self, sim, t):
        typeObj = FrameServer.name2class[t]
        (new, updated, deleted) = FrameServer.Store.get_update(typeObj, sim)
        ret = {}
        ret["new"] = new
        ret["updated"] = updated
        ret["deleted"] = deleted
        return ret

class GetTracked(Resource):
    @handle_exceptions
    def get(self, sim, t):
        typeObj = FrameServer.name2class[t]
        (new, updated, deleted) = FrameServer.Store.get_update(typeObj, sim, tracked_only=True)
        ret = {}
        ret["new"] = new
        ret["updated"] = updated
        ret["deleted"] = deleted
        return ret


class GetPushType(Resource):
    @handle_exceptions
    def get(self, sim, t):
        typeObj = FrameServer.name2class[t]
        mod, new, deleted = FrameServer.Store.get_update(typeObj, sim, tracked_only = False)
        ret = {}
        ret["new"] = new
        ret["updated"] = mod
        ret["deleted"] = deleted
        return ret

    @handle_exceptions
    def put(self, sim, t):
        typeObj = FrameServer.name2class[t]
        list_objs = json.loads(request.form["insert_list"])
        for o in list_objs:
            # FIX THIS OBJ
            FrameServer.Store.insert(obj, sim)
        return {}

    @handle_exceptions
    def post(self, sim, t):
        typeObj = FrameServer.name2class[t]
        args = parser.parse_args()
        # update dict is a dictionary of dictionaries: { primary_key : {
        # property_name : property_value } }
        data = args["update_dict"]
        update_dict = json.loads(data)
        new, mod, deleted = update_dict["new"], update_dict["mod"], update_dict["deleted"]
        FrameServer.Store.put_update(sim, typeObj, new, mod, deleted)
        return {}

class GetInsertDeleteObject(Resource):
    @handle_exceptions
    def get(self, sim, t, uid):
        typeObj = FrameServer.name2class[t]
        obj = FrameServer.Store.get(typeObj, UUID(uid))
        return obj

    @handle_exceptions
    def put(self, sim, t, uid):
        # typeObj = FrameServer.name2class[t]
        typeObj = FrameServer.name2class[t]
        o = json.loads(request.form["obj"])
        # FIX THIS OBJ
        FrameServer.Store.insert(obj, sim)

    @handle_exceptions
    def delete(self, sim, t, uid):
        raise NotImplementedError()
        typeObj = FrameServer.name2class[t]
        FrameServer.Store.delete(typeObj, UUID(uid), sim)

class Register(Resource):
    @handle_exceptions
    def put(self, sim):
        data = urllib2.unquote(request.data.replace("+", " "))
        typemap = json.loads(data)["sim_typemap"]
        FrameServer.Store.register_app(sim, typemap, FrameServer.name2class, FrameServer.name2baseclasses)

def SetupLoggers() :
    global logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    folder = os.path.join(os.path.dirname(__file__), "../logs/")
    if not os.path.exists(folder):
        os.mkdir(folder)
    logfile = filename = os.path.join(folder, "frameserver.log")
    flog = logging.handlers.RotatingFileHandler(logfile, maxBytes=10 * 1024 * 1024, backupCount=50, mode='w')
    flog.setLevel(logging.WARN)
    flog.setFormatter(logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
    logger.addHandler(flog)

    clog = logging.StreamHandler()
    # clog.addFilter(logging.Filter(name='mobdat'))
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    clog.setLevel(logging.DEBUG)
    logger.addHandler(clog)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)

class FrameServer(object):
    '''
    Store server for CADIS
    '''
    Store = dataframe()
    name2class = dict([(tp.Class().__name__, tp) for tp in DATAMODEL_TYPES])
    name2baseclasses = dict([(tp.Class().__name__, tp.__pcc_bases__) for tp in DATAMODEL_TYPES])
    Shutdown = False
    def __init__(self):
        global server
        SetupLoggers()
        self.app = app
        self.api = api
        FrameServer.app = app
        FrameServer.api = self.api
        self.api.add_resource(GetInsertDeleteObject, '/<string:sim>/<string:t>/<string:uid>')
        self.api.add_resource(GetPushType, '/<string:sim>/<string:t>')
        self.api.add_resource(GetUpdated, '/<string:sim>/updated/<string:t>')
        self.api.add_resource(GetTracked, '/<string:sim>/tracked/<string:t>')
        self.api.add_resource(GetAllUpdated, '/<string:sim>/updated')
        self.api.add_resource(GetAllTracked, '/<string:sim>/tracked')
        self.api.add_resource(Register, '/<string:sim>')
        server = self
        self.app.run(port=12000, debug=False, threaded = True)

    def shutdown(self):
        FrameServer.Shutdown = True
