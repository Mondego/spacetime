'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from threading import Thread as Parallel
#from multiprocessing import Process as Parallel
import requests
import json
import logging
import time

from store import store

class frame(object):
  __Logger = logging.getLogger(__name__)
  def __init__(self, address = "http://localhost:12000/", relax_time = 500):
    self.__app = None
    self.__typemap = {}
    self.__name2type = {}
    self.object_store = store()
    if not address.endswith('/'):
      address += '/'
    self.__address = address
    self.__base_address = None
    self._relax_time = (float(relax_time)/1000)

  def __register_app(self, app):
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
    self.__name2type = dict([(tp.Class().__name__, tp) for tp in all_types])
    self.object_store.add_types(all_types)

    jsonobj = json.dumps({"sim_typemap": jobj})
    return requests.put(self.__base_address, 
                 data = jsonobj,
                 headers = {'content-type': 'application/json'})

    
  def attach_app(self, app):
    self.__app = app
    self.__register_app(app)

  def run_async(self):
    p = Parallel(target = self.__run)
    p.daemon = True
    p.start()
    
  def run(self):
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
      if timespent < self._relax_time:
        time.sleep(float(self._relax_time - timespent))

    self.__shutdown()

  def get(self, tp, *args):
    if args:
      id = args[0]
      return self.object_store.get_one(tp, id)
    return self.object_store.get(tp)

  def add(self, object):
    self.object_store.insert(object)

  def __process_pull_resp(self,resp):
    new, mod, deleted = resp["new"], resp["updated"], resp["deleted"]
    for tp in new:
      typeObj = self.__name2type[tp]
      self.object_store.frame_insert_all(typeObj, new[tp])
    for tp in mod:
      self.object_store.update_all(self.__name2type[tp], mod[tp])
    for tp in deleted:
      for obj_id in deleted[tp]:
        self.object_store.delete_with_id(self.__name2type[tp], obj_id)
    
  def __pull(self):
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

  def __shutdown(self):
    self.__app.shutdown()
    self.__unregister_app()

  def __unregister_app(self):
    raise NotImplementedError()
