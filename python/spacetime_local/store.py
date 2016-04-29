'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from common.recursive_dictionary import RecursiveDictionary
from pcc.attributes import spacetime_property
from threading import currentThread
from common.converter import create_jsondict, create_tracking_obj
class _container():
  pass
class store(object):
  def __init__(self):
    self.__objects = RecursiveDictionary()
    self._changes = RecursiveDictionary({"new": RecursiveDictionary(), "deleted": RecursiveDictionary()})
    self.__deleted = set()

  def add_types(self, types):
    for tp in types:
      self.__objects.setdefault(tp, RecursiveDictionary())

  def get(self, tp):
    return self.__objects[tp].values()

  def get_one(self, tp, id):
    return self.__objects[tp][id]

  def frame_insert(self, tp, id, objjson):
    obj = create_tracking_obj(tp, objjson, self.__objects, False, False)
    obj._primarykey = id
    obj.__primarykey__ = id
    self.__objects.setdefault(tp, RecursiveDictionary())[id] = obj
    obj.__start_tracking__ = True

  def frame_insert_all(self, tp, objjsons):
    for id, obj in objjsons.items():
      self.frame_insert(tp, id, obj)

  def insert(self, obj):
    objjson = create_jsondict(obj)
    self._changes["new"].setdefault(obj.Class(), RecursiveDictionary()).setdefault(obj.__primarykey__, RecursiveDictionary()).rec_update(objjson)
    self.__objects.setdefault(obj.__class__, RecursiveDictionary()).setdefault(obj.__primarykey__, []).append(obj)
    if hasattr(obj.__class__, "__pcc_projection__") and obj.__class__.__pcc_projection__:
      class _dummy(object):
        pass
      new_obj = _dummy()
      new_obj.__class__ = obj.__class__.__ENTANGLED_TYPES__[0]
      for dimension in new_obj.__dimensions__:
        if hasattr(obj, dimension._name):
          setattr(new_obj, dimension._name, getattr(obj, dimension._name))
      self.__objects.setdefault(new_obj.__class__, RecursiveDictionary()).setdefault(new_obj.__primarykey__, []).append(new_obj)
    

  def insert_all(self, objs):
    for obj in objs:
      self.insert(obj)

  def delete(self, tp, obj):
    if tp in self.__objects and obj.__primarykey__ in self.__objects[tp]:
      self._changes["deleted"].setdefault(obj.__class__, set()).add(obj.__primarykey__)
    self.__deleted.add((tp, obj))

  def delete_with_id(self, tp, obj_id):
    obj = self.get_one(tp, obj_id)
    self.delete(tp, obj)

  def __flush_derived_objs(self):
    types = self.__objects.keys()
    for tp in types:
      if not tp.__PCC_BASE_TYPE__:
        self.__objects[tp] = RecursiveDictionary()

  def clear_changes(self):
    self._changes["new"] = RecursiveDictionary.fromkeys(self._changes["new"], RecursiveDictionary())
    if currentThread().getName() in spacetime_property.change_tracker:
      spacetime_property.change_tracker[currentThread().getName()].clear()
    for tp, obj in self.__deleted:
      del self.__objects[tp][obj.__primarykey__]
    self.__deleted.clear()
    self.__flush_derived_objs()

  def get_changes(self):
    mod = spacetime_property.change_tracker[currentThread().getName()] if currentThread().getName() in spacetime_property.change_tracker else {}
    return {"mod": mod, "new": self._changes["new"], "deleted": self._changes["deleted"]}


  def update(self, tp, id, updatejson):
    objjson = create_jsondict(self.get_one(tp, id))
    objjson.rec_update(updatejson)
    starttracking = False
    self.__objects[tp][id].__dict__.update(create_tracking_obj(tp, objjson, self.__objects, starttracking, False).__dict__)
    starttracking = True

  def update_all(self, tp, updatejsons):
    for id, updatejson in updatejsons.items():
      self.update(tp, id, updatejson)
