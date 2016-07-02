#!/usr/bin/python
'''
Created on Feb 19, 2016
author: arthurvaladares, Rohan Achar
'''
from functools import wraps
import json
import logging, logging.handlers
import os
import signal
import sys
from threading import Timer
import time
import urllib2

from flask import Flask, request
from flask.helpers import make_response
from flask_restful import Api, Resource, reqparse

from datamodel.all import DATAMODEL_TYPES
from store import *


parser = reqparse.RequestParser()
parser.add_argument('update_dict')
parser.add_argument('insert_list')
parser.add_argument('obj')
parser.add_argument('get_types')

def handle_exceptions(f):
    @wraps(f)
    def wrapped(*args, **kwds):
        try:
            FrameServer.app_gc_timers[kwds["sim"]] = time.time()
            if type(args[0]) is not Register:
                if kwds["sim"] not in FrameServer.Store.get_app_list():
                    return {}, 401
            ret = f(*args, **kwds)
        except Exception, e:
            logger.exception("Exception handling function %s:", f.func_name)
            raise
        return ret
    return wrapped

def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    server.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

class FlaskConfig(object):
    RESTFUL_JSON = {}

    @staticmethod
    def init_app(app):
        app.config['RESTFUL_JSON']['cls'] = app.json_encoder = json.JSONEncoder

app = Flask(__name__)
app.config.from_object(FlaskConfig)
FlaskConfig.init_app(app)
api = Api(app)

class GetAllUpdatedTracked(Resource):
    @handle_exceptions
    def get(self, sim):
        args = parser.parse_args()
        types_tracked = json.loads(args["get_types"])["types_tracked"]
        types_updated = json.loads(args["get_types"])["types_updated"]
        (all_new, all_updated, all_deleted) = ({}, {}, {})
        all_touched_types_t = set()
        all_touched_types_u = set()
        with FrameServer.Store.get_lock(sim):
            only_tracked_types = set(types_tracked).difference(set(types_updated))
            for tp in only_tracked_types:
                typeObj = FrameServer.name2class[tp]
                (new, updated, deleted, touched_types) = FrameServer.Store.get_update(typeObj, sim, tracked_only=True)
                all_new.update(new)
                all_updated.update(updated)
                all_deleted.update(deleted)
                all_touched_types_t.update(touched_types)

            for tp in types_updated:
                typeObj = FrameServer.name2class[tp]
                (new, updated, deleted, touched_types) = FrameServer.Store.get_update(typeObj, sim)
                all_new.update(new)
                all_updated.update(updated)
                all_deleted.update(deleted)
                all_touched_types_u.update(touched_types)

            for tp in set(all_touched_types_t).union(set(all_touched_types_u)):
                if FrameServer.name2class[tp].__PCC_BASE_TYPE__:
                    FrameServer.Store.clear_buffer(sim, tp, tracked_only = tp in all_touched_types_t.difference(all_touched_types_u))
                
        ret = {}
        ret["new"] = all_new
        ret["updated"] = all_updated
        ret["deleted"] = all_deleted
        return ret

    @handle_exceptions
    def post(self, sim):
        args = parser.parse_args()
        # update dict is a dictionary of dictionaries: { primary_key : {
        # property_name : property_value } }
        data = args["update_dict"]
        update_dict = json.loads(data)
        for strtp in update_dict.keys():
            typeObj = FrameServer.name2class[strtp]
            new, mod, deleted = update_dict[strtp]["new"], update_dict[strtp]["mod"], update_dict[strtp]["deleted"]
            FrameServer.Store.put_update(sim, typeObj, new, mod, deleted)
        return {}


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
        raise Exception("PUT operation for GetPushType not yet implemented")
        #typeObj = FrameServer.name2class[t]
        #list_objs = json.loads(request.form["insert_list"])
        #for o in list_objs:
        #    # FIX THIS OBJ
        #    FrameServer.Store.insert(obj, sim)
        #return {}

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
        raise Exception("GET operation for GetInsertDeleteObject not yet implemented")
        #typeObj = FrameServer.name2class[t]
        #obj = FrameServer.Store.get(typeObj, UUID(uid))
        #return obj

    @handle_exceptions
    def put(self, sim, t, uid):
        raise Exception("PUT operation for GetInsertDeleteObject not yet implemented")
        # typeObj = FrameServer.name2class[t]
        #typeObj = FrameServer.name2class[t]
        #o = json.loads(request.form["obj"])
        # FIX THIS OBJ
        #FrameServer.Store.insert(obj, sim)

    @handle_exceptions
    def delete(self, sim, t, uid):
        raise Exception("DELETE operation for GetInsertDeleteObject not yet implemented")

class Register(Resource):
    @handle_exceptions
    def put(self, sim):
        data = urllib2.unquote(request.data.replace("+", " "))
        typemap = json.loads(data)["sim_typemap"]
        FrameServer.Store.register_app(sim, typemap, FrameServer.name2class, FrameServer.name2baseclasses)

    @handle_exceptions
    def delete(self, sim):
        FrameServer.disconnect(sim)

def SetupLoggers(debug) :
    global logger
    if debug:
        logl = logging.DEBUG
    else:
        logl = logging.INFO

    logger = logging.getLogger()
    logger.setLevel(logl)
    folder = os.path.join(os.path.dirname(__file__), "../logs/")
    if not os.path.exists(folder):
        os.mkdir(folder)
    logfile = filename = os.path.join(folder, "frameserver.log")
    flog = logging.handlers.RotatingFileHandler(logfile, maxBytes=10 * 1024 * 1024, backupCount=50, mode='w')
    flog.setLevel(logl)
    flog.setFormatter(logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
    logger.addHandler(flog)

    clog = logging.StreamHandler()
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    if debug:
        clog.setLevel(logging.DEBUG)
    else:
        clog.setLevel(logging.INFO)
    logger.addHandler(clog)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

class FrameServer(object):
    '''
    Store server for CADIS
    '''
    Store = dataframe()
    name2class = dict([(tp.__realname__, tp) for tp in DATAMODEL_TYPES])
    name2baseclasses = dict([(tp.__realname__, tp.__pcc_bases__) for tp in DATAMODEL_TYPES])
    Shutdown = False
    app_gc_timers = {}

    # Garbage collection
    disconnect_timer = None
    timeout = 30.0

    def __init__(self, port, debug, external):
        global server
        SetupLoggers(debug)
        logging.info("Log level is " + str(logger.level))
        self.port = port
        self.external = external
        self.app = app
        self.api = api
        FrameServer.app = app
        FrameServer.api = self.api
        self.DATAMODEL_TYPES = DATAMODEL_TYPES
        # Not currently used
        # self.api.add_resource(GetInsertDeleteObject, '/<string:sim>/<string:t>/<string:uid>')
        self.api.add_resource(GetPushType, '/<string:sim>/<string:t>')
        self.api.add_resource(GetUpdated, '/<string:sim>/updated/<string:t>')
        self.api.add_resource(GetTracked, '/<string:sim>/tracked/<string:t>')
        self.api.add_resource(GetAllUpdatedTracked, '/<string:sim>/updated')
        #self.api.add_resource(GetAllTracked, '/<string:sim>/tracked')
        self.api.add_resource(Register, '/<string:sim>')
        server = self

    def run(self, profiling=False):
        self.profiling = profiling
        FrameServer.start_timer()
        host = '0.0.0.0' if self.external else '127.0.0.1'
        logging.info("Binding to " + host)
        if profiling:
            import cProfile
            if not os.path.exists('stats'):
                os.mkdir('stats')
            self.profile = cProfile.Profile()
            self.profile.enable()
            print "starting profiler"
        self.app.run(host=host, port=self.port, debug=False, threaded=False)

    def reload_dms(self):
        from datamodel.all import DATAMODEL_TYPES
        FrameServer.Store.reload_dms()
        FrameServer.name2class = dict([(tp.__realname__, tp) for tp in DATAMODEL_TYPES])
        FrameServer.name2baseclasses = dict([(tp.__realname__, tp.__pcc_bases__) for tp in DATAMODEL_TYPES])
        print [tp.__realname__ for tp in DATAMODEL_TYPES]
        self.DATAMODEL_TYPES = DATAMODEL_TYPES

    def pause(self):
        logging.info("Pausing all applications...")
        FrameServer.Store.pause()

    def unpause(self):
        logging.info("Unpausing all applications...")
        FrameServer.Store.unpause()

    ##################################################################
    ## Client disconnect timeout + Garbage Collection
    ##################################################################
    @classmethod
    def start_timer(cls):
        cls.disconnect_timer = Timer(cls.timeout, cls.check_disconnect, ())
        cls.disconnect_timer.start()

    @classmethod
    def check_disconnect(cls):
        for sim in cls.app_gc_timers.keys():
            if time.time() - cls.app_gc_timers[sim] > cls.timeout:
                cls.disconnect(sim)
        cls.start_timer()

    @classmethod
    def disconnect(cls, sim):
        cls.Store.gc(sim, cls.name2class)
        del cls.app_gc_timers[sim]

    def shutdown(self):
        if self.profiling:
            strtime = time.strftime("%Y-%m-%d_%H-%M-%S")
            self.profile.disable()
            self.profile.create_stats()
            self.profile.dump_stats(os.path.join('stats', "%s_frameserver.ps" % (strtime)))
        FrameServer.Shutdown = True
