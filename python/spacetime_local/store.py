'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from __future__ import absolute_import

from common.recursive_dictionary import RecursiveDictionary
from pcc.attributes import spacetime_property
from threading import currentThread
from common.converter import create_jsondict, create_tracking_obj, create_obj
import logging

spacetime_property.GLOBAL_TRACKER = True

class _container():
    pass
class store(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.__objects = RecursiveDictionary()
        self._changes = RecursiveDictionary({"new": RecursiveDictionary(), "deleted": RecursiveDictionary()})
        self.__deleted = set()

    def add_types(self, types):
        for tp in types:
            self.__objects.setdefault(tp, RecursiveDictionary())

    def get(self, tp):
        return self.__objects[tp].values()

    def get_one(self, tp, id):
        if id in self.__objects[tp]:
            return self.__objects[tp][id]
        else:
            raise Exception("Could not find object %s of type %s" % (
                                                        id, tp))

    def frame_insert(self, tp, id, objjson):
        obj = create_tracking_obj(tp, objjson, self.__objects, False, False)
        obj._primarykey = id
        obj.__primarykey__ = id
        self.__objects.setdefault(tp, RecursiveDictionary())[id] = obj
        obj.__start_tracking__ = True
        return obj

    def frame_insert_all(self, tp, objjsons):
        ret = []
        for id, obj in objjsons.items():
            ret.append(self.frame_insert(tp, id, obj))
        return ret

    def insert(self, obj):
        objjson = create_jsondict(obj)
        self._changes["new"].setdefault(obj, RecursiveDictionary()).setdefault(obj.__primarykey__, RecursiveDictionary()).rec_update(objjson)
        self.__objects.setdefault(obj.__class__, RecursiveDictionary())[obj.__primarykey__] = obj
        if hasattr(obj.__class__, "__pcc_projection__") and obj.__class__.__pcc_projection__:
            class _dummy(object):
                pass
            new_obj = _dummy()
            new_obj.__class__ = obj.__class__.__ENTANGLED_TYPES__[0]
            for dimension in new_obj.__dimensions__:
                if hasattr(obj, dimension._name):
                    setattr(new_obj, dimension._name, getattr(obj, dimension._name))
            self.__objects.setdefault(new_obj.__class__, RecursiveDictionary()).setdefault(new_obj.__primarykey__, []).append(new_obj)
        if (hasattr(obj, "__dependent_type__")):
            obj.__class__ = obj
        obj.__start_tracking__ = True

    def insert_all(self, objs):
        for obj in objs:
            self.insert(obj)

    def frame_delete_with_id(self, tp, obj_id):
        obj = self.get_one(tp, obj_id)
        self.__deleted.add((tp, obj))

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

    def clear_all(self, t=None):
        self.clear_changes()
        self.clear_incoming_record()
        if not t:
            for tp in self.__objects.keys():
                self.__objects[tp] = RecursiveDictionary()
        else:
            self.__objects[t] = RecursiveDictionary()


    def clear_changes(self):
        self._changes["new"].clear()
        if currentThread().getName() in spacetime_property.change_tracker:
            spacetime_property.change_tracker[currentThread().getName()].clear()
        for tp, obj in self.__deleted:
            del self.__objects[tp][obj.__primarykey__]
        self.__deleted.clear()
        self.__flush_derived_objs()
        self._changes["deleted"] = {}

    def get_changes(self):
        mod = spacetime_property.change_tracker[currentThread().getName()] if currentThread().getName() in spacetime_property.change_tracker else {}
        for tp in self._changes["deleted"]:
            for id in self._changes["deleted"][tp]:
                if tp in mod and id in mod[tp]:
                    del mod[tp][id]
        for tp in self._changes["new"]:
            for id in self._changes["new"][tp]:
                if tp in mod and id in mod[tp]:
                    del mod[tp][id]
        return {"mod": mod, "new": self._changes["new"], "deleted": self._changes["deleted"]}


    def update(self, tp, id, updatejson):
        try:
            obj = self.__objects[tp][id]
            for dim_name in updatejson:
                if dim_name in tp.__dimensions_name__:
                    # TODO: O(n) search: needs to be improved
                    dimension = None
                    for dim in tp.__dimensions__:
                        if dim._name == dim_name:
                            dimension = dim
                    if not dimension:
                        raise Exception("Could not find dimension with name %s" % dim_name)
                    if hasattr(dimension._type, "__dependent_type__"):
                        setattr(obj, dimension._name, create_tracking_obj(tp, updatejson, self.__objects, True))
                    else:
                        setattr(obj, dimension._name, create_obj(dimension._type, updatejson[dimension._name]))
            obj.__start_tracking__ = True
            return self.__objects[tp][id]
        except:
            self.logger.debug("could not update %s: not found in store.", id)
            return None

    def update_all(self, tp, updatejsons):
        ret = []
        for id, updatejson in updatejsons.items():
            obj = self.update(tp, id, updatejson)
            if obj is not None:
                ret.append(obj)
        return ret

    def clear_incoming_record(self):
        self.__incoming_new = {}
        self.__incoming_mod = {}
        self.__incoming_del = {}

    def create_incoming_record(self, new, mod, deleted):
        for tp in new:
            self.__incoming_new.setdefault(tp, []).extend(new[tp])
        for tp in mod:
            self.__incoming_mod.setdefault(tp, []).extend(mod[tp])
        for tp in deleted:
            self.__incoming_del.setdefault(tp, []).extend(deleted[tp])

    def get_new(self, tp):
        return self.__incoming_new[tp] if tp in self.__incoming_new else []

    def get_mod(self, tp):
        return self.__incoming_mod[tp] if tp in self.__incoming_mod else []

    def get_deleted(self, tp):
        return self.__incoming_del[tp] if tp in self.__incoming_del else []
