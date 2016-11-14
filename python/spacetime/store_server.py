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
import zlib

from flask import Flask, request, Response, make_response
from flask_restful import Api, Resource, reqparse
#from flask_compress import Compress

from datamodel.all import DATAMODEL_TYPES
from store import dataframe_stores
import platform


parser = reqparse.RequestParser()
parser.add_argument('update_dict')
parser.add_argument('insert_list')
parser.add_argument('obj')
parser.add_argument('get_types')
parser.add_argument('observed_types')

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

signal.signal(signal.SIGINT, signal_handler)  # @UndefinedVariable

class FlaskConfig(object):
    RESTFUL_JSON = {}

    @staticmethod
    def init_app(app):
        app.config['RESTFUL_JSON']['cls'] = app.json_encoder = json.JSONEncoder

app = Flask(__name__)
app.config.from_object(FlaskConfig)
FlaskConfig.init_app(app)
api = Api(app)
#Compress(app)

class GetAllUpdatedTracked(Resource):
    @handle_exceptions
    def get(self, sim):
        data, content_type = FrameServer.Store.getupdates(sim)
        response = make_response(data)
        response.headers["content-type"] = content_type
        return response

    @handle_exceptions
    def post(self, sim):
        args = parser.parse_args()
        data = request.data
        if "content-encoding" in request.headers and request.headers["content-encoding"] == "gzip":
            data = zlib.decompress(data)
        FrameServer.Store.update(sim, data)
        return {}

class GetPushTypeUpdates(Resource):
    @handle_exceptions
    def get(self, sim, t):
        typeObj = FrameServer.name2class[t]
        mod, new, deleted = FrameServer.Store.get_update(typeObj, sim, tracked_only = False)
        ret = {}
        ret["new"] = new
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
        json_dict = json.loads(data)
        typemap = json_dict["sim_typemap"]
        wire_format = json_dict["wire_format"] if "wire_format" in json_dict else "json"
        app_id = json_dict["app_id"]
        FrameServer.Store.register_app(sim, typemap, wire_format = wire_format)

    @handle_exceptions
    def delete(self, sim):
        FrameServer.disconnect(sim)

class GetPutObjectDictionary(Resource):
    @handle_exceptions
    def get(self, sim):
        args = parser.parse_args()
        observed_types = json.loads(args["observed_types"])
        ret = {}
        for t in observed_types:
            typeObj = FrameServer.name2class[t]
            ret[t] = FrameServer.Store.get(typeObj)
        return ret

    @handle_exceptions
    def put(self, sim):
        args = parser.parse_args()
        objects = json.loads(args["insert_list"])
        for t in objects:
            typeObj = FrameServer.name2class[t]
            FrameServer.Store.put(typeObj, objects[t])


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
    name2class = dict([(tp.__realname__, tp) for tp in DATAMODEL_TYPES])
    name2baseclasses = dict([(tp.__realname__, tp.__pcc_bases__) for tp in DATAMODEL_TYPES])
    
    Store = dataframe_stores(name2class)
    Shutdown = False
    app_gc_timers = {}

    # Garbage collection
    disconnect_timer = None
    timeout = 0

    def __init__(self, port, debug, external, timeout, clear_on_exit = False):
        global server
        SetupLoggers(debug)
        logging.info("Log level is " + str(logger.level))
        self.port = port
        if timeout > 0:
            FrameServer.timeout = float(timeout)
        self.external = external
        FrameServer.clear_on_exit = clear_on_exit
        self.app = app
        self.api = api
        FrameServer.app = app
        FrameServer.api = self.api
        self.DATAMODEL_TYPES = DATAMODEL_TYPES
        # Not currently 
        self.api.add_resource(GetPutObjectDictionary, '/<string:sim>/dictupdate')
        self.api.add_resource(GetPushTypeUpdates, '/<string:sim>/updates/<string:t>')
        self.api.add_resource(GetAllUpdatedTracked, '/<string:sim>/updated')
        self.api.add_resource(Register, '/<string:sim>')
        server = self

    def run(self, profiling=False):
        self.profiling = profiling
        if FrameServer.timeout > 0:
            FrameServer.start_timer()
        host = '0.0.0.0' if self.external else '127.0.0.1'
        logging.info("Binding to " + host)
        if profiling:
            try:
                import cProfile  # @UnresolvedImport
                if not os.path.exists('stats'):
                    os.mkdir('stats')
                self.profile = cProfile.Profile()
                self.profile.enable()
                print "starting profiler"
            except:
                self.profiling = False
                if platform.system() == "Java":
                    print "cProfile not available Jython."
                else:
                    print "failed to start profiler."
        self.app.run(host=host, port=self.port, debug=False, threaded=True)

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
        if not cls.Store.pause_servers:
            for sim in cls.app_gc_timers.keys():
                if time.time() - cls.app_gc_timers[sim] > cls.timeout:
                    cls.disconnect(sim)
        cls.start_timer()

    @classmethod
    def disconnect(cls, sim):
        cls.Store.gc(sim)
        del cls.app_gc_timers[sim]
        if len(cls.app_gc_timers) == 0:
            if cls.clear_on_exit:
                logging.info("all simulations are gone, clearing dataframe")
                FrameServer.Store.clear()

    def shutdown(self):
        if self.profiling:
            strtime = time.strftime("%Y-%m-%d_%H-%M-%S")
            self.profile.disable()
            self.profile.create_stats()
            self.profile.dump_stats(os.path.join('stats', "%s_frameserver.ps" % (strtime)))
        FrameServer.Shutdown = True

