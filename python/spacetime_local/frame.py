'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
from __future__ import absolute_import

from threading import Thread as Parallel
#from multiprocessing import Process as Parallel
import requests
import json
import time
import signal
import cmd
import sys
import os
import zlib

from pcc.dataframe.dataframe_client import dataframe_client as dataframe
from .IFrame import IFrame
from pcc.recursive_dictionary import RecursiveDictionary

import logging
from logging import NullHandler
from requests.exceptions import HTTPError, ConnectionError
from common.instrument import SpacetimeInstruments as si
from common.instrument import timethis
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
from requests.sessions import Session
from common.javahttpadapter import MyJavaHTTPAdapter, ignoreJavaSSL
from common.modes import Modes
from pcc.dataframe_changes.IDataframeChanges import DataframeChanges_Base
from common.wire_formats import FORMATS
import platform

class SpacetimeConsole(cmd.Cmd):
    """Command console interpreter for frame."""

    def do_exit(self, line):
        """ exit
        Exits all applications by calling their shutdown methods.
        """
        shutdown()

    def do_quit(self, line):
        """ quit
        Exits all applications by calling their shutdown methods.
        """
        shutdown()

    def do_stop(self, line):
        """ stop
        Stops all applications, but does not exit prompt.
        """
        for f in frame.framelist:
            f._stop()

    def do_restart(self, line):
        """ restart
        Restarts all frames
        """
        for f in frame.framelist:
            f.run_async()

    def emptyline(self):
        pass

    def do_EOF(self, line):
        shutdown()

class frame(IFrame):
    framelist = set()
    def __init__(self, address="http://127.0.0.1:12000/", time_step=500, instrument=False, profiling=False, wire_format="cbor", compress=False):
        frame.framelist.add(self)
        self.thread = None
        self.__app = None
        self.__appname = ""
        self.__host_typemap = {}
        self.__host_wire_format = {}
        self.__typemap = {}
        self.__name2type = {}
        self.object_store = dataframe()
        self.object_store.start_recording = True
        if not address.endswith('/'):
            address += '/'
        self.__address = address
        self.__default_wire_format = wire_format
        self.__compress = compress
        self.__time_step = (float(time_step) / 1000)
        self.__new = {}
        self.__mod = {}
        self.__del = {}
        self.__observed_types = set()
        self.__observed_types_new = set()
        self.__observed_types_mod = set()
        self.__curtime = time.time()
        self.__curstep = 0
        self.__start_time = time.strftime("%Y-%m-%d_%H-%M-%S")
        self.__instrumented = instrument
        self.__profiling = profiling
        self.__sessions = {}
        self.__host_to_push_groupkey = {}
        if instrument:
            self._instruments = {}
            self._instrument_headers = []
            self._instrument_headers.append('bytes sent')
            self._instrument_headers.append('bytes received')

    def __register_app(self, app):
        self.logger = self.__setup_logger("spacetime@" + self.__appname)
        self.__host_typemap = {}
        for address, tpmap in self.__app.__declaration_map__.items():
            if address == "default":
                address = self.__address
            fulladdress = address + self.__appname
            if fulladdress not in self.__host_typemap:
                self.__host_typemap[fulladdress] = tpmap
            else:
                for declaration in tpmap:
                    self.__host_typemap[fulladdress].setdefault(declaration, set()).update(set(tpmap[declaration]))

        self.__default_wire_format = (self.__app.__special_wire_format__["default"] 
                                      if "default" in self.__app.__special_wire_format__ else 
                                      self.__default_wire_format)
        for host in self.__host_typemap:
            self.__host_wire_format[host] = (
                self.__app.__special_wire_format__[host] 
                if host in self.__app.__special_wire_format__ else 
                self.__default_wire_format) 
        all_types = set()
        for host in self.__host_typemap:
            wire_format = self.__host_wire_format[host]
            jobj = dict([(k, [tp.__realname__ for tp in v]) for k, v in self.__host_typemap[host].items()])
            producing, getting, gettingsetting, deleting, setting, tracking = (self.__host_typemap[host].setdefault(Modes.Producing, set()),
                self.__host_typemap[host].setdefault(Modes.Getter, set()),
                self.__host_typemap[host].setdefault(Modes.GetterSetter, set()),
                self.__host_typemap[host].setdefault(Modes.Deleter, set()),
                self.__host_typemap[host].setdefault(Modes.Setter, set()),
                self.__host_typemap[host].setdefault(Modes.Tracker, set()))
            self.__typemap.setdefault(Modes.Producing, set()).update(producing)
            self.__typemap.setdefault(Modes.Getter, set()).update(getting)
            self.__typemap.setdefault(Modes.GetterSetter, set()).update(gettingsetting)
            self.__typemap.setdefault(Modes.Deleter, set()).update(deleting)
            self.__typemap.setdefault(Modes.Setter, set()).update(setting)
            self.__typemap.setdefault(Modes.Tracker, set()).update(tracking)
            
            all_types_host = tracking.union(producing).union(getting).union(gettingsetting).union(deleting).union(setting)
            all_types.update(all_types_host)
            self.__observed_types.update(all_types_host)
            self.__observed_types_new.update(self.__host_typemap[host][Modes.Tracker].union(self.__host_typemap[host][Modes.Getter]).union(self.__host_typemap[host][Modes.GetterSetter]))

            self.__observed_types_mod.update(self.__host_typemap[host][Modes.Getter].union(self.__host_typemap[host][Modes.GetterSetter]))

            jsonobj = json.dumps({"sim_typemap": jobj, "wire_format": wire_format, "app_id": self.__app.app_id})
            try:
                self.__sessions[host] = Session()
                if platform.system() == 'Java':
                    ignoreJavaSSL()
                    self.logger.info("Using custom HTTPAdapter for Jython")
                    self.__sessions[host].mount(host, MyJavaHTTPAdapter())
                    self.__sessions[host].verify=False
                resp = requests.put(host,
                             data = jsonobj,
                             headers = {'content-type': 'application/json'})
            except HTTPError as exc:
                self.__handle_request_errors(resp, exc)
                return False
            except ConnectionError:
                self.logger.exception("Cannot connect to host.")
                self.__disconnected = True
                return False
        self.__name2type = dict([(tp.__realname__, tp) for tp in all_types])
        self.object_store.add_types(all_types)
        for host in self.__host_typemap:
            self.__host_to_push_groupkey[host] = set([self.object_store.get_group_key(tp)
                                                      for tp in self.__host_typemap[host][Modes.GetterSetter].union(
                                                                self.__host_typemap[host][Modes.Setter]).union(
                                                                self.__host_typemap[host][Modes.Producing]).union(
                                                                self.__host_typemap[host][Modes.Deleter])    
                                                     ])
        
        return True

    @staticmethod
    def loop():
        SpacetimeConsole().cmdloop()

    def get_instrumented(self):
        """
        Returns if frame is running instrumentation. (True/False)
        """
        return self.__instrumented

    def get_curtime(self):
        """
        Returns the timestamp of the current step.
        """
        return self.__curtime

    def get_curstep(self):
        """
        Returns the current step value of the simulation.
        """
        return self.__curstep

    def get_timestep(self):
        """
        Returns the time-step value in milliseconds.
        """
        return self.__time_step

    def get_app(self):
        """
        Returns a reference to the application.
        """
        return self.__app

    def attach_app(self, app):
        """
        Receives reference to application (implementing IApplication).

        Arguments:
        app : spacetime-conformant Application

        Exceptions:
        None
        """
        self.__app = app
        self.__appname = app.__class__.__name__  + "_" + self.__app.app_id

    def run_async(self):
        """
        Starts application in non-blocking mode.

        Arguments:
        None

        Exceptions:
        None
        """
        self.thread = Parallel(target = self.__run)
        self.thread.daemon = True
        self.thread.start()

    def run_main(self):
        self.__run()

    def run(self):
        """
        Starts application in blocking mode.

        Arguments:
        None

        Exceptions:
        None
        """
        self.thread = Parallel(target = self.__run)
        self.thread.daemon = True
        self.thread.start()
        self.thread.join()

    def __clear(self):
        self.__disconnected = False
        self.__app.done = False
        self.object_store.clear_all()
        self.__new = {}
        self.__mod = {}
        self.__del = {}

    def __run(self):
        self.__clear()
        if not self.__app:
            raise NotImplementedError("App has not been attached")
        success = self.__register_app(self.__app)
        if success:
            try:
                if self.__profiling:
                    try:
                        from cProfile import Profile  # @UnresolvedImport
                        if not os.path.exists('stats'):
                            os.mkdir('stats')
                        self.__profile = Profile()
                        self.__profile.enable()
                        self.logger.info("starting profiler for %s", self.__appname)
                    except:
                        self.logger.error("Could not import cProfile (not supported in Jython).")
                        self.__profile = None
                        self.__profiling = None

                self.__pull()
                self.__app.initialize()
                self.__push()
                while not self.__app.done:
                    st_time = time.time()
                    self.__pull()
                    self.__app.update()
                    self.__push()
                    end_time = time.time()
                    timespent = end_time - st_time
                    self.__curstep += 1
                    self.__curtime = time.time()
                    # time spent on execution loop
                    if timespent < self.__time_step:
                        time.sleep(float(self.__time_step - timespent))
                    else:
                        self.logger.info("loop exceeded maximum time: %s ms", timespent)

                    # Writes down total time spent in spacetime methods
                    if self.__instrumented:
                        si.record_instruments(timespent, self)
                # One last time, because _shutdown may delete objects from the store
                self.__pull()
                self._shutdown()
                self.__push()
                self.__unregister_app()
            except ConnectionError as cerr:
                self.logger.error("A connection error occurred: %s", cerr.message)
            except HTTPError as herr:
                self.logger.error("A fatal error has occurred while communicating with the server: %s", herr.message)
            except:
                self.logger.exception("An unknown error occurred.")
                raise
            finally:
                if self.__profiling:
                    self.__profile.disable()
                    self.__profile.create_stats()
                    self.__profile.dump_stats(os.path.join('stats', "%s_stats_%s.ps" % (self.__start_time, self.__appname)))
        else:
            self.logger.info("Could not register, exiting run loop...")

    def app_done(self):
        """
        app_done

        Returns whether app has finished running or not
        """
        return self.__app.done

    def get(self, tp, oid=None):
        """
        Retrieves objects from local data storage. If id is provided, returns
        the object identified by id. Otherwise, returns the list of all objects
        matching type tp.

        Arguments:
        tp : PCC set type being fetched
        oid : primary key of an individual object.

        Exceptions:
        - ID does not exist in store
        - Application does not annotate that type
        """
        if tp in self.__observed_types:
            if oid:
                # Have to get this to work
                return self.object_store.get(tp, oid)
            return self.object_store.get(tp)
        else:
            raise Exception("Application %s does not annotate type %s" % (self.__appname, tp))


    def add(self, obj):
        """
        Adds an object to be stored and tracked by spacetime.

        Arguments:
        obj : PCC object to stored

        Exceptions:
        - Application is not annotated as a producer
        """
        if obj.__class__ in self.__typemap[Modes.Producing]:
            self.object_store.append(obj.__class__, obj)
        else:
            raise Exception("Application %s is not a producer of type %s" % (self.__appname, obj.__class__))

    def delete(self, tp, obj):
        """
        Deletes an object currently stored and tracked by spacetime.

        Arguments:
        tp: PCC type of object to be deleted
        obj : PCC object to be deleted

        Exceptions:
        - Application is not annotated as a Deleter
        """

        if tp in self.__typemap[Modes.Deleter]:
            self.object_store.delete(tp, obj)
        else:
            raise Exception("Application %s is not registered to delete %s" % (self.__appname, tp))

    def get_new(self, tp):
        """
        Retrieves new objects of type 'tp' retrieved in last pull (i.e. since
        last tick).

        Arguments:
        tp: PCC type for retrieving list of new objects

        Exceptions:
        None

        Note:
        Application should be annotated as  a Getter, GetterSetter, or Tracker,
        otherwise result is always an empty list.
        """
        if tp in self.__observed_types_new:
            return self.object_store.get_new(tp)
        else:
            self.logger.warn(("Checking for new objects of type %s, but not "
                "a Getter, GetterSetter, or Tracker of type. Empty list "
                "always returned"),tp)
            return []

    def get_mod(self, tp):
        """
        Retrieves objects of type 'tp' that were modified since last pull
        (i.e. since last tick).

        Arguments:
        tp: PCC type for retrieving list of modified objects

        Exceptions:
        None

        Note:
        Application should be annotated as a Getter,or GetterSetter, otherwise
        result is always an empty list.
        """
        if tp in self.__observed_types_mod:
            return self.object_store.get_mod(tp)
        else:
            self.logger.warn(("Checking for modifications in objects of type "
                "%s, but not a Getter or GetterSetter of type. "
                "Empty list always returned"),tp)
            return []

    def get_deleted(self, tp):
        """
        Retrieves objects of type 'tp' that were deleted since last pull
        (i.e. since last tick).

        Arguments:
        tp: PCC type for retrieving list of deleted objects

        Exceptions:
        None

        Note:
        Application should be annotated as a Getter, GetterSetter, or Tracker,
        otherwise result is always an empty list.
        """
        if tp in self.__observed_types_new:
            return self.object_store.get_deleted(tp)
        else:
            self.logger.warn(("Checking for deleted objects of type %s, but "
                "not a Getter, GetterSetter, or Tracker of type. Empty list "
                "always returned"),tp)
            return []

    def __handle_request_errors(self, resp, exc):
        if resp.status_code == 401:
            self.logger.error("This application is not registered at the server. Stopping...")
            raise
        else:
            self.logger.warn("Non-success code received from server: %s %s",
                                          resp.status_code, resp.reason)
    
    @timethis
    def __process_pull_resp(self, resp):
        if resp and "gc" in resp:
            self.object_store.apply_changes(resp)
            self.object_store.clear_record()

    @timethis
    def __pull(self):
        if self.__disconnected:
            return
        if self.__instrumented:
            self._instruments['bytes received'] = 0
        updates = DataframeChanges_Base()
        try:
            for host in self.__host_typemap:
                type_dict = {}
                # Need to give mechanism to selectively ask for some changes. Very hard to implement in current dataframe scheme.
                resp = self.__sessions[host].get(host + "/updated", data = {})
                try:
                    resp.raise_for_status()
                    if self.__instrumented:
                        self._instruments['bytes received'] = len(resp.content)
                    data = resp.content
                    #print data
                    DF_CLS, content_type = FORMATS[self.__host_wire_format[host]]
                    dataframe_change = DF_CLS()
                    dataframe_change.ParseFromString(data)
                    updates.CopyFrom(dataframe_change)
                except HTTPError as exc:
                    self.__handle_request_errors(resp, exc)
            #json.dump(updates, open("pull_" + self.__appname + ".json", "a") , sort_keys = True, separators = (',', ': '), indent = 4)
            self.__process_pull_resp(updates)
        except ConnectionError:
            self.logger.exception("Disconnected from host.")
            self.__disconnected = True
            self._stop()

    @timethis
    def __push(self):
        if self.__disconnected:
            return
        if self.__instrumented:
            self._instruments['bytes sent'] = 0
        changes = self.object_store.get_record()
        #json.dump(changes, open("push_" + self.__appname + ".json", "a") , sort_keys = True, separators = (',', ': '), indent = 4)
            
        for host in self.__host_typemap:
            try:
                DF_CLS, content_type = FORMATS[self.__host_wire_format[host]]
                changes_for_host = DF_CLS()
                changes_for_host["gc"] = RecursiveDictionary([
                    (gck, gc) 
                    for gck, gc in changes["gc"].items() 
                    if gck in self.__host_to_push_groupkey[host]])
                if "types" in changes:
                    changes_for_host["types"] = changes["types"]
                dictmsg = changes_for_host.SerializeToString()
                #update_dict = {"update_dict": protomsg}
                if self.__instrumented:
                    self._instruments['bytes sent'] = sys.getsizeof(dictmsg)
                headers = {'content-type': content_type}
                if self.__compress:
                    headers['content-encoding'] = 'gzip'
                    dictmsg = zlib.compress(dictmsg)
                resp = self.__sessions[host].post(host + "/updated", 
                                                  data = dictmsg, 
                                                  headers = headers)
            except TypeError:
                self.logger.exception("error encoding obj. Object: %s", changes_for_host)
            except HTTPError as exc:
                self.__handle_request_errors(resp, exc)
            except ConnectionError:
                self.logger.exception("Disconnected from host.")
                self.__disconnected = True
                self._stop()

        self.object_store.clear_record()
        self.object_store.clear_buffer()

    def _shutdown(self):
        """
        _shutdown

        Called after the frame execution loop stops, in the last pull/push
        iteration
        """
        self.__app.shutdown()

    def _stop(self):
        """
        _stop

        Called by frame's command prompt on quit/exit
        """
        self.__app.done = True

    def __unregister_app(self):
        for host in self.__host_typemap:
            resp = requests.delete(host)
            self.logger.info("Successfully deregistered from %s", host)

    def __setup_logger(self, name, file_path=None):
        logger = logging.getLogger(name)
        # Set default logging handler to avoid "No handler found" warnings.
        logger.addHandler(NullHandler())
        logger.setLevel(logging.DEBUG)
        logger.debug("Starting logger for %s",name)
        return logger
        #logging.getLogger('requests').setLevel(logging.WARNING)

def shutdown():
    import sys
    print "Shutting down all applications..."
    threads = []
    for f in frame.framelist:
        f._stop()
        threads.append(f.thread)

    [t.join() for t in threads if t]
    sys.exit(0)


def signal_handler(signal, signal_frame):
    shutdown()

signal.signal(signal.SIGINT, signal_handler)

