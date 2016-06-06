'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from threading import Thread as Parallel
#from multiprocessing import Process as Parallel
import requests
import json
import time
import signal
import cmd
import sys

from store import store
from IFrame import IFrame
from pcc.recursive_dictionary import RecursiveDictionary

import logging
from logging import NullHandler
from requests.exceptions import HTTPError, ConnectionError

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
        Stops all applications, but does not exist prompt.
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
    def __init__(self, address="http://127.0.0.1:12000/", time_step=500):
        frame.framelist.add(self)
        self.thread = None
        self.__app = None
        self.__host_typemap = {}
        self.__typemap = {}
        self.__name2type = {}
        self.object_store = store()
        if not address.endswith('/'):
            address += '/'
        self.__address = address
        self._time_step = (float(time_step) / 1000)
        self.__new = {}
        self.__mod = {}
        self.__del = {}
        self.__observed_types = set()
        self.__observed_types_new = set()
        self.__observed_types_mod = set()

    def __register_app(self, app):
        self.logger = self.__setup_logger("spacetime@" + app.__class__.__name__)
        self.__host_typemap = {}
        for address, tpmap in self.__app.__declaration_map__.items():
            if address == "default":
                address = self.__address
            fulladdress = address + self.__app.__class__.__name__
            if fulladdress not in self.__host_typemap:
                self.__host_typemap[fulladdress] = tpmap
            else:
                for declaration in tpmap:
                    self.__host_typemap[fulladdress].setdefault(declaration, set()).update(set(tpmap[declaration]))

        all_types = set()
        for host in self.__host_typemap:
            jobj = dict([(k, [tp.__realname__ for tp in v]) for k, v in self.__host_typemap[host].items()])
            producing, getting, gettingsetting, deleting, setting, tracking = (self.__host_typemap[host].setdefault("producing", set()),
                self.__host_typemap[host].setdefault("getting", set()),
                self.__host_typemap[host].setdefault("gettingsetting", set()),
                self.__host_typemap[host].setdefault("deleting", set()),
                self.__host_typemap[host].setdefault("setting", set()),
                self.__host_typemap[host].setdefault("tracking", set()))
            self.__typemap.setdefault("producing", set()).update(producing)
            self.__typemap.setdefault("getting", set()).update(getting)
            self.__typemap.setdefault("gettingsetting", set()).update(gettingsetting)
            self.__typemap.setdefault("deleting", set()).update(deleting)
            self.__typemap.setdefault("setting", set()).update(setting)
            self.__typemap.setdefault("tracking", set()).update(tracking)

            all_types_host = tracking.union(producing).union(getting).union(gettingsetting).union(deleting).union(setting)
            all_types.update(all_types_host)
            self.__observed_types.update(all_types_host)
            self.__observed_types_new.update(self.__host_typemap[host]["tracking"].union(self.__host_typemap[host]["getting"]).union(self.__host_typemap[host]["gettingsetting"]))

            self.__observed_types_mod.update(self.__host_typemap[host]["getting"].union(self.__host_typemap[host]["gettingsetting"]))

            jsonobj = json.dumps({"sim_typemap": jobj})
            try:
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
        return True

    @staticmethod
    def loop():
        SpacetimeConsole().cmdloop()

    def get_timestep(self):
        """
        Returns the time-step value in milliseconds.
        """
        return self._time_step


    def attach_app(self, app):
        """
        Receives reference to application (implementing IApplication).

        Arguments:
        app : spacetime-conformant Application

        Exceptions:
        None
        """
        self.__app = app

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
                    # time spent on execution loop
                    if timespent < self._time_step:
                        time.sleep(float(self._time_step - timespent))
                    else:
                        self.logger.info("loop exceeded maximum time: %s ms", timespent)

                # One last time, because _shutdown may delete objects from the store
                self.__pull()
                self._shutdown()
                self.__push()
                self.__unregister_app()
            except ConnectionError as cerr:
                self.logger.error("A connection error occurred: %s", cerr.message)
            except HTTPError as herr:
                self.logger.error("A fatal error has occurred while communicating with the server: %s", herr.message)
        else:
            self.logger.info("Could not register, exiting run loop...")


    def get(self, tp, id=None):
        """
        Retrieves objects from local data storage. If id is provided, returns
        the object identified by id. Otherwise, returns the list of all objects
        matching type tp.

        Arguments:
        tp : PCC set type being fetched
        id : primary key of an individual object.

        Exceptions:
        - ID does not exist in store
        - Application does not annotate that type
        """
        if tp in self.__observed_types:
            if id:
                return self.object_store.get_one(tp, id)
            return self.object_store.get(tp)
        else:
            raise Exception("Application does not annotate type %s" % (tp.Class()))

    def add(self, obj):
        """
        Adds an object to be stored and tracked by spacetime.

        Arguments:
        obj : PCC object to stored

        Exceptions:
        - Application is not annotated as a producer
        """
        if obj.__class__ in self.__typemap["producing"]:
            self.object_store.insert(obj)
        else:
            raise Exception("Application is not a producer of type %s" % (obj.__class__.Class()))

    def delete(self, tp, obj):
        """
        Deletes an object currently stored and tracked by spacetime.

        Arguments:
        tp: PCC type of object to be deleted
        obj : PCC object to be deleted

        Exceptions:
        - Application is not annotated as a Deleter
        """

        if tp in self.__typemap["deleting"]:
            self.object_store.delete(tp, obj)
        else:
            raise Exception("Application is not registered to delete %s" % (tp.Class()))

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
                "always returned"),tp.Class())
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
                "Empty list always returned"),tp.Class())
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
                "always returned"),tp.Class())
            return []

    def __handle_request_errors(self, resp, exc):
        if resp.status_code == 401:
            self.logger.error("This application is not registered at the server. Stopping...")
            raise
        else:
            self.logger.warn("Non-success code received from server: %s %s",
                                          resp.status_code, resp.reason)

    def __process_pull_resp(self,resp):
        new, mod, deleted = resp["new"], resp["updated"], resp["deleted"]
        new_objs, mod_objs, deleted_objs = {}, {}, {}
        for tp in new:
            typeObj = self.__name2type[tp]
            new_objs[typeObj] = self.object_store.frame_insert_all(typeObj, new[tp])
        for tp in mod:
            typeObj = self.__name2type[tp]
            mod_objs[typeObj] = self.object_store.update_all(typeObj, mod[tp])
        for tp in deleted:
            typeObj = self.__name2type[tp]
            objlist = []
            for obj_id in deleted[tp]:
                try:
                    objlist.append(self.object_store.get_one(typeObj,obj_id))
                    self.object_store.frame_delete_with_id(typeObj, obj_id)
                except:
                    self.logger.warn("Could not delete object %s of type %s",
                                                        obj_id, typeObj.Class())
            deleted_objs[typeObj] = objlist
        self.object_store.create_incoming_record(new_objs, mod_objs, deleted_objs)

    def __pull(self):
        if self.__disconnected:
            return
        self.object_store.clear_incoming_record()
        updates = RecursiveDictionary()
        try:
            for host in self.__host_typemap:
                type_dict = {}
                type_dict["types_tracked"] = [tp.__realname__ for tp in list(self.__typemap["tracking"])]
                type_dict["types_updated"] = [tp.__realname__
                     for tp in list(self.__typemap["getting"].union(self.__typemap["gettingsetting"]))]
                resp  = requests.get(host + "/updated", data = {
                "get_types":
                json.dumps(type_dict)
                  })
                try:
                    resp.raise_for_status()
                    updates.rec_update(resp.json())
                except HTTPError as exc:
                    self.__handle_request_errors(resp, exc)

            self.__process_pull_resp(updates)
        except ConnectionError:
            self.logger.exception("Disconnected from host.")
            self.__disconnected = True
            self._stop()

    def __push(self):
        if self.__disconnected:
            return
        changes = self.object_store.get_changes()
        for host in self.__host_typemap:
            update_dict = {}
            for tp in self.__host_typemap[host]["producing"]:
                if tp.Class() in changes["new"]:
                    update_dict.setdefault(tp.__realname__, {"new": {}, "mod": {}, "deleted": []})["new"] = changes["new"][tp.Class()]
            for tp in self.__host_typemap[host]["gettingsetting"]:
                if tp.Class() in changes["mod"]:
                    update_dict.setdefault(tp.__realname__, {"new": {}, "mod": {}, "deleted": []})["mod"] = changes["mod"][tp.Class()]
            for tp in self.__host_typemap[host]["setting"]:
                if tp.Class() in changes["mod"]:
                    update_dict.setdefault(tp.__realname__, {"new": {}, "mod": {}, "deleted": []})["mod"] = changes["mod"][tp.Class()]
            for tp in self.__host_typemap[host]["deleting"]:
                if tp.Class() in changes["deleted"]:
                    update_dict.setdefault(tp.__realname__, {"new": {}, "mod": {}, "deleted": []})["deleted"].extend(changes["deleted"][tp.Class()])

            package = {"update_dict": json.dumps(update_dict)}
            try:
                resp = requests.post(host + "/updated", json = package)
            except HTTPError as exc:
                self.__handle_request_errors(resp, exc)
            except ConnectionError:
                self.logger.exception("Disconnected from host.")
                self.__disconnected = True
                self._stop()

        self.object_store.clear_changes()

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

