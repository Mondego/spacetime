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

class SpacetimeConsole(cmd.Cmd):
    """Command console interpreter for frame."""

    def do_exit(self, line):
        """ exit
        Exits all applications by calling their shutdown methods.
        """
        shutdown()

    def emptyline(self):
        pass

    def do_EOF(self, line):
        shutdown()

class frame(IFrame):
    framelist = []
    def __init__(self, address="http://127.0.0.1:12000/", time_step=500):
        frame.framelist.append(self)
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
        self.__host_typemap = dict([(address + "/" + self.__app.__class__.__name__, tpmap) for address, tpmap in self.__app.__declaration_map__.items()])
        all_types = set()
        for host in self.__host_typemap:
            jobj = dict([(k, [tp.Class().__name__ for tp in v]) for k, v in self.__host_typemap[host].items()])
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
            requests.put(host,
                         data = jsonobj,
                         headers = {'content-type': 'application/json'})
        self.__name2type = dict([(tp.Class().__name__, tp) for tp in all_types])
        self.object_store.add_types(all_types)
        return 

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
        self.__register_app(app)

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

    def __run(self):
        if not self.__app:
            raise NotImplementedError("App has not been attached")
        self.__pull()
        self.__app.initialize()
        self.__push()
        while not self.__app.done:
            st_time = time.time()
            self.__pull()
            #take1t = time.time()
            #take1 = take1t - st_time
            self.__app.update()
            #take2t = time.time()
            #take2 = take2t - take1t
            self.__push()
            end_time = time.time()
            #take3 = end_time - take2t
            timespent = end_time - st_time
            #print self.__app.__class__.__name__, take1, take2, take3,
            #timespent
            if timespent < self._time_step:
                time.sleep(float(self._time_step - timespent))

        # One last time, because _shutdown may delete objects from the store
        self.__pull()
        self._shutdown()
        self.__push()


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
                    self.object_store.delete_with_id(typeObj, obj_id)
                except:
                    self.logger.warn("Could delete object %s of type %s",
                                                        obj_id, typeObj.Class())
            deleted_objs[typeObj] = objlist
        self.object_store.create_incoming_record(new_objs, mod_objs, deleted_objs)

    def __pull(self):
        self.object_store.clear_incoming_record()
        final_resp = RecursiveDictionary()
        for host in self.__host_typemap:
            resp = requests.get(host + "/tracked", data = {
            "get_types":
            json.dumps({"types": [tp.Class().__name__ for tp in list(self.__typemap["tracking"])]})
              })
            final_resp.rec_update(resp.json())
            resp = requests.get(host + "/updated", data = {
            "get_types":
            json.dumps({
                "types":
                [tp.Class().__name__
                 for tp in list(self.__typemap["getting"].union(self.__typemap["gettingsetting"]))]
              })
          })
            final_resp.rec_update(resp.json())

        self.__process_pull_resp(final_resp)

    def __push(self):
        changes = self.object_store.get_changes()
        for host in self.__host_typemap:
            update_dict = {}
            for tp in self.__host_typemap[host]["producing"]:
                if tp.Class() in changes["new"]:
                    update_dict.setdefault(tp.Class().__name__, {"new": {}, "mod": {}, "deleted": []})["new"] = changes["new"][tp.Class()]
            for tp in self.__host_typemap[host]["gettingsetting"]:
                if tp.Class() in changes["mod"]:
                    update_dict.setdefault(tp.Class().__name__, {"new": {}, "mod": {}, "deleted": []})["mod"] = changes["mod"][tp.Class()]
            for tp in self.__host_typemap[host]["setting"]:
                if tp.Class() in changes["mod"]:
                    update_dict.setdefault(tp.Class().__name__, {"new": {}, "mod": {}, "deleted": []})["mod"] = changes["mod"][tp.Class()]
            for tp in self.__host_typemap[host]["deleting"]:
                if tp.Class() in changes["deleted"]:
                    update_dict.setdefault(tp.Class().__name__, {"new": {}, "mod": {}, "deleted": []})["deleted"].extend(changes["deleted"][tp.Class()])
                    #self.logger.debug( "deleting %s %s", tp.Class().__name__, update_dict[tp.Class().__name__]["deleted"])
            for tp in update_dict:
                package = {"update_dict": json.dumps(update_dict[tp])}
                requests.post(host + "/" + tp, json = package)
        self.object_store.clear_changes()

    def _shutdown(self):
        self.__app.shutdown()
        #frame.framelist.remove(self)
        #self.__unregister_app()

    def _stop(self):
        self.__app.done = True
        #frame.framelist.remove(self)
        #self.__unregister_app()

    def __unregister_app(self):
        raise NotImplementedError()

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
    
    [t.join() for t in threads]
    sys.exit(0)


def signal_handler(signal, signal_frame):
    shutdown()

signal.signal(signal.SIGINT, signal_handler)

