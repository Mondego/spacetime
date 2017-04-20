#!/usr/bin/python
'''
Created on Feb 2, 2017
author: Rohan Achar
'''
from functools import wraps
import json
import logging, logging.handlers
import os
import signal
import sys
from threading import Timer, Thread
import time
import urllib2
import zlib
import shelve
from tornado.web import RequestHandler, HTTPError

from datamodel.all import DATAMODEL_TYPES
from store import dataframe_stores
import platform, requests
from Queue import Queue
import tornado.ioloop
from tornado.options import options

def handle_exceptions(f):
    @wraps(f)
    def wrapped(*args, **kwds):
        try:
            FrameServer.app_gc_timers[args[1]] = time.time()
            x_real_ip = args[0].request.headers.get("X-Real-IP")
            remote_ip = x_real_ip or args[0].request.remote_ip
            if FrameServer.has_ip_watcher:
                FrameServer.ip_watcher.put((remote_ip, args[1]))
            if type(args[0]) is not Register:
                if args[1] not in FrameServer.Store.get_app_list():
                    raise HTTPError(500, "%s not registered to the store." % args[1])
            ret = f(*args, **kwds)
        except Exception, e:
            logger.exception("Exception handling function %s:", f.func_name)
            raise HTTPError(500, "Exception handling function %s:" % f.func_name)
        except HTTPError, e:
            raise
        return ret
    return wrapped

is_closing = False

def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    global is_closing
    is_closing = True

def try_exit():
    global is_closing
    if is_closing:
        server.shutdown()
        tornado.ioloop.IOLoop.instance().stop()

signal.signal(signal.SIGINT, signal_handler)  # @UndefinedVariable

class GetAllUpdatedTracked(RequestHandler):
    @handle_exceptions
    def get(self, sim):
        data, content_type = FrameServer.Store.getupdates(sim)
        self.set_header("content-type", content_type)
        self.write(data)

    @handle_exceptions
    def post(self, sim):
        data = self.request.body
        FrameServer.Store.update(sim, data)

class Register(RequestHandler):
    @handle_exceptions
    def put(self, sim):
        data = self.request.body
        #data = urllib2.unquote(request.data.replace("+", " "))
        json_dict = json.loads(data)
        typemap = json_dict["sim_typemap"]
        wire_format = json_dict["wire_format"] if "wire_format" in json_dict else "json"
        app_id = json_dict["app_id"]
        FrameServer.Store.register_app(sim, typemap, wire_format = wire_format)

    @handle_exceptions
    def delete(self, sim):
        FrameServer.disconnect(sim)

def SetupLoggers(debug) :
    global logger
    if debug:
        logl = logging.INFO
    else:
        logl = logging.INFO

    logger = logging.getLogger()
    logger.setLevel(logl)
    folder = "logs/"
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
        clog.setLevel(logging.INFO)
    else:
        clog.setLevel(logging.INFO)
    logger.addHandler(clog)
    #tornado.options.options['log_file_prefix'].set('../logs/tornado.log')
    #tornado.options.parse_command_line()

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("tornado.access").setLevel(logging.WARNING)
    tlog = logging.handlers.RotatingFileHandler(logfile, maxBytes=10 * 1024 * 1024, backupCount=50, mode='w')
    #tlog.setLevel(logging.INFO)
    #logging.getLogger("tornado.access").addHandler(tlog)

def ip_watcher():
    global ip_table
    ip_table = shelve.open(os.path.join(os.path.dirname(__file__), "../logs/ip_tables.shelve"))
    
    while True:
        try:
            ip_and_app_id = FrameServer.ip_watcher.get()
            ip, app_id = ip_and_app_id
            if ip == "127.0.0.1": continue
            if ip not in ip_table or (ip in ip_table and len(ip_table[ip]) == 0):
                req = requests.get("http://freegeoip.net/json/?q={0}".format(ip))
                if req.status_code == 200:
                    resp = req.json()
                    resp["app_id"] = app_id
                    ip_table[ip] = resp
                    ip_table.sync()
                else:
                    ip_table[ip] = dict()
                    ip_table.sync()
        except Exception, e:
            print e
            continue
        

class FrameServer(object):
    name2class = dict([(tp.__realname__, tp) for tp in DATAMODEL_TYPES])
    name2baseclasses = dict([(tp.__realname__, tp.__pcc_bases__) for tp in DATAMODEL_TYPES])
    
    Store = dataframe_stores(name2class)
    Shutdown = False
    app_gc_timers = {}

    # Garbage collection
    disconnect_timer = None
    timeout = 0
    
    @staticmethod
    def make_app():
        return tornado.web.Application([
            (r"/([a-zA-Z0-9_-]+)/updated", GetAllUpdatedTracked),
            (r"/([a-zA-Z0-9_-]+)", Register),
        ])

    def __init__(self, port, debug, timeout, clear_on_exit = False, has_ip_watcher = False):
        global server
        SetupLoggers(debug)
        logging.info("Log level is " + str(logger.level))
        self.port = port
        if timeout > 0:
            FrameServer.timeout = float(timeout)
        FrameServer.clear_on_exit = clear_on_exit
        self.app = FrameServer.make_app()
        FrameServer.app = self.app
        self.DATAMODEL_TYPES = DATAMODEL_TYPES
        FrameServer.has_ip_watcher = has_ip_watcher
        if has_ip_watcher:
            FrameServer.ip_watcher = Queue()
            FrameServer.ip_watcher_thread = Thread(target = ip_watcher)
            FrameServer.ip_watcher_thread.daemon = True
            FrameServer.ip_watcher_thread.start()
        # Not currently 
        server = self

    def run(self, profiling=False):
        self.profiling = profiling
        if FrameServer.timeout > 0:
            FrameServer.start_timer()
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
        self.app.listen(self.port)
        tornado.ioloop.PeriodicCallback(try_exit, 100).start()
        tornado.ioloop.IOLoop.current().start()
        tornado.ioloop.IOLoop.current().close(True)
        sys.exit(0)

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

