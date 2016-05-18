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
    def __init__(self, address = "http://127.0.0.1:12000/", time_step = 500):
        frame.framelist.append(self)
        self.__app = None
        self.__typemap = {}
        self.__name2type = {}
        self.object_store = store()
        if not address.endswith('/'):
            address += '/'
        self.__address = address
        self.__base_address = None
        self._time_step = (float(time_step)/1000)
        self.__new = {}
        self.__mod = {}
        self.__del = {}

    def __register_app(self, app):
        self.logger = self.__setup_logger("spacetime@" + app.__class__.__name__)
        self.__base_address = self.__address + self.__app.__class__.__name__
        (producing, tracking, getting, gettingsetting, setting, deleting) = (
          self.__app.__class__.__pcc_producing__ if hasattr(self.__app.__class__, "__pcc_producing__") else set(),
          self.__app.__class__.__pcc_tracking__ if hasattr(self.__app.__class__, "__pcc_tracking__") else set(),
          self.__app.__class__.__pcc_getting__ if hasattr(self.__app.__class__, "__pcc_getting__") else set(),
          self.__app.__class__.__pcc_gettingsetting__ if hasattr(self.__app.__class__, "__pcc_gettingsetting__") else set(),
          self.__app.__class__.__pcc_setting__ if hasattr(self.__app.__class__, "__pcc_setting__") else set(),
          self.__app.__class__.__pcc_deleting__ if hasattr(self.__app.__class__, "__pcc_deleting__") else set())
        self.__typemap["producing"] = producing
        self.__typemap["tracking"] = tracking
        self.__typemap["gettingsetting"] = gettingsetting
        self.__typemap["getting"] = getting.difference(gettingsetting)
        self.__typemap["setting"] = setting.difference(gettingsetting)
        self.__typemap["deleting"] = deleting

        jobj = dict([(k, [tp.Class().__name__ for tp in v]) for k, v in self.__typemap.items()])

        all_types = tracking.union(producing).union(getting).union(gettingsetting).union(deleting).union(setting)
        self.__observed_types = all_types
        self.__observed_types_new = self.__typemap["tracking"].union(
                                    self.__typemap["getting"]).union(
                                    self.__typemap["gettingsetting"])

        self.__observed_types_mod = self.__typemap["getting"].union(
                                    self.__typemap["gettingsetting"])


        self.__name2type = dict([(tp.Class().__name__, tp) for tp in all_types])
        self.object_store.add_types(all_types)

        jsonobj = json.dumps({"sim_typemap": jobj})
        return requests.put(self.__base_address,
                     data = jsonobj,
                     headers = {'content-type': 'application/json'})

    @staticmethod
    def loop():
        SpacetimeConsole().cmdloop()

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
        p = Parallel(target = self.__run)
        p.daemon = True
        p.start()

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
        p = Parallel(target = self.__run)
        p.daemon = True
        p.start()
        p.join()

    def __run(self):
        if not self.__app:
            raise NotImplementedError("App has not been attached")
        self.__app.initialize()
        self.__push()
        while not self.__app.is_done():
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
            #print self.__app.__class__.__name__, take1, take2, take3, timespent
            if timespent < self._time_step:
                time.sleep(float(self._time_step - timespent))

        self._shutdown()

    def get(self, tp, id = None):
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
            raise Exception("Application does not annotate type %s" % (
                tp.Class()))

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
            raise Exception("Application does not annotate type %s" % (
                obj.__class__.Class()))

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
            raise Exception("Application is not registered to delete %s" % (
                tp.Class()))

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
            self.object_store.frame_insert_all(typeObj, new[tp])
            new_objs[typeObj] = self.get(typeObj)
        for tp in mod:
            typeObj = self.__name2type[tp]
            self.object_store.update_all(typeObj, mod[tp])
            mod_objs[typeObj] = self.get(typeObj)
        for tp in deleted:
            typeObj = self.__name2type[tp]
            objlist = []
            for obj_id in deleted[tp]:
                objlist.append(self.object_store.get_one(typeObj,obj_id))
                self.object_store.delete_with_id(typeObj, obj_id)
            deleted_objs[typeObj] = objlist
        self.object_store.create_incoming_record(new_objs, mod_objs, deleted_objs)

    def __pull(self):
        self.object_store.clear_incoming_record()
        resp = requests.get(self.__base_address + "/tracked", data = {
            "get_types":
            json.dumps({"types": [tp.Class().__name__ for tp in list(self.__typemap["tracking"])]})
          })

        self.__process_pull_resp(resp.json())
        resp = requests.get(self.__base_address + "/updated", data = {
            "get_types":
            json.dumps({
                "types":
                [tp.Class().__name__
                 for tp in list(self.__typemap["getting"].union(self.__typemap["gettingsetting"]))]
              })
          })

        self.__process_pull_resp(resp.json())

    def __push(self):
        changes = self.object_store.get_changes()
        update_dict = {}
        for tp in self.__typemap["producing"]:
            if tp.Class() in changes["new"]:
                update_dict.setdefault(tp.Class().__name__, {"new": {}, "mod": {}, "deleted": []})["new"] = changes["new"][tp.Class()]
        for tp in self.__typemap["gettingsetting"]:
            if tp.Class() in changes["mod"]:
                update_dict.setdefault(tp.Class().__name__, {"new": {}, "mod": {}, "deleted": []})["mod"] = changes["mod"][tp.Class()]
        for tp in self.__typemap["setting"]:
            if tp.Class() in changes["mod"]:
                update_dict.setdefault(tp.Class().__name__, {"new": {}, "mod": {}, "deleted": []})["mod"] = changes["mod"][tp.Class()]
        for tp in self.__typemap["deleting"]:
            if tp.Class() in changes["deleted"]:
                update_dict.setdefault(tp.Class().__name__, {"new": {}, "mod": {}, "deleted": []})["deleted"].extend(changes["deleted"][tp.Class()])
        for tp in update_dict:
            package = {"update_dict": json.dumps(update_dict[tp])}
            requests.post(self.__base_address + "/" + tp, json = package)
        self.object_store.clear_changes()

    def _shutdown(self):
        self.__app.shutdown()
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
    for f in frame.framelist:
        f._shutdown()
    sys.exit(0)

def signal_handler(signal, signal_frame):
    shutdown()

signal.signal(signal.SIGINT, signal_handler)

