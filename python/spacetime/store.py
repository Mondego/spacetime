'''
Created on Apr 19, 2016

@author: Rohan Achar
'''

from multiprocessing import Lock

from flask import Flask, request
from flask.helpers import make_response
from flask_restful import Api, Resource, reqparse
from common.recursive_dictionary import RecursiveDictionary
from common.converter import create_complex_obj, create_jsondict
from datamodel.all import DATAMODEL_TYPES
import pcc
import json
import os
import sys
import uuid
import logging
from threading import RLock

from cache import Cache

def calc_basecls2derived(name2class):
    result = {}
    for tp in DATAMODEL_TYPES:
        if hasattr(tp, "__PCC_BASE_TYPE__") and tp.__PCC_BASE_TYPE__:
            for base in tp.mro():
                if (base != tp.Class()
                        and base.__name__ in name2class
                        and hasattr(name2class[base.__name__], "__PCC_BASE_TYPE__")
                        and name2class[base.__name__].__PCC_BASE_TYPE__):
                    result.setdefault(name2class[base.__name__], []).append(tp)
    #                print base.__name__, tp.__name__
    #print [(tp.__realname__, [der.__realname__ for der in result[tp]]) for tp in result]
    return result



# not active object.
# just stores the basic sets.
# requires set types that it has to store.
# must accept changes.
# must return object when asked for it.
class store(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # type -> {id : object} object is just json style recursive dictionary.
        # Onus on the client side to make objects
        self.__data = RecursiveDictionary()
        self.__base2derived = calc_basecls2derived(
                    dict([(tp.__realname__, tp) for tp in DATAMODEL_TYPES]))

    def reload_dms(self, datamodel_types):
        self.__base2derived = calc_basecls2derived(
                    dict([(tp.__realname__, tp) for tp in datamodel_types]))

    def add_types(self, types):
        # types will be list of string names.
        for tp in types:
            self.__data.setdefault(tp.__realname__, RecursiveDictionary())

    def get_base_types(self):
        # returns string types back.
        return self.__data.keys()

    def get(self, tp, id):
        # assume it is there, else raise exception.
        return self.get_by_type(tp)[id]

    def get_ids(self, tp):
        return self.get_by_type(tp).keys()

    def get_by_type(self, tp):
        alltps = [tp]
        if tp in self.__base2derived:
            alltps.extend(self.__base2derived[tp])
        result = {}
        for subtp in alltps:
            strtp = subtp.__realname__
            if strtp in self.__data:
                result.update(self.__data[strtp])
        return result

    def get_as_dict(self):
        return self.__data

    def put(self, tp, id, object):
        # assume object is just dictionary, and not actual object.
        try:
            strtp = tp.__realname__
            self.__data[strtp][id] = object
        except:
            self.logger.error("error inserting id %s on type %s" % (
                id, tp.Class()))
            raise

    def update(self, tp, id, object_changes):
        alltps = [tp]
        if tp in self.__base2derived:
            alltps.extend(self.__base2derived[tp])
        for subtp in alltps:
            strtp = subtp.__realname__
            if strtp in self.__data and id in self.__data[strtp]:
                self.__data[strtp][id].update(object_changes)

    def delete(self, tp, id, verbose=True):
        count = 0
        alltps = [tp]
        if tp in self.__base2derived:
            alltps.extend(self.__base2derived[tp])
        for subtp in alltps:
            strtp = subtp.__realname__
            if strtp in self.__data:
                if id in self.__data[strtp]:
                    del self.__data[strtp][id]
                    count += 1
        if count == 0 and verbose:
            self.logger.warn("Object ID %s missing from data storage.", id)

# not active object.
# wrapper on store, that allows it to keep track of app, and subset merging etc.
# needs app.
# maintains list of changes to app.
# mod, new, delete are the three sets that have to be maintained for app.
class dataframe(object):
    def __init__(self, ):
        self.logger = logging.getLogger(__name__)
        self.__base_store = store()
        self.__apps = set()
        # Dictionary for garbage collection of objects not deleted after
        # simulation is gone
        self.__gc = {}

        # app -> mod, new, deleted
        # mod, new : tp -> id -> object changes/full object
        # deleted: list of ids deleted
        self.__cache = Cache()

        # app -> list of dynamic types tracked by app.
        self.__app_to_dynamicpcc = {}

        # app -> copylock
        self.__copylock = {}

        # locks the frame, avoiding deadlocks on pause/unpause
        self.__framelock = RLock()

        # Type -> List of apps that use it
        self.__type_to_app = {}

        self.__typename_to_primarykey = {}
        for tp in DATAMODEL_TYPES:
            if tp.__PCC_BASE_TYPE__ or tp.__name__ == "_Join":
                self.__typename_to_primarykey[tp.__realname__] = tp.__primarykey__._name
            else:
                self.__typename_to_primarykey[tp.__realname__] = tp.__ENTANGLED_TYPES__[0].__primarykey__._name

        self.__base2derived = calc_basecls2derived(
                    dict([(tp.__realname__, tp) for tp in DATAMODEL_TYPES]))

    def reload_dms(self):
        from datamodel.all import DATAMODEL_TYPES
        self.__base_store.reload_dms(DATAMODEL_TYPES)

        self.__typename_to_primarykey = {}
        for tp in DATAMODEL_TYPES:
            if tp.__PCC_BASE_TYPE__ or tp.__name__ == "_Join":
                self.__typename_to_primarykey[tp.__realname__] = tp.__primarykey__._name
            else:
                self.__typename_to_primarykey[tp.__realname__] = tp.__ENTANGLED_TYPES__[0].__primarykey__._name
        self.__base2derived = calc_basecls2derived(
                    dict([(tp.__realname__, tp) for tp in DATAMODEL_TYPES]))
    def __convert_to_objects(self, objmap):
        real_objmap = {}
        for tp, objlist in objmap.items():
            real_objmap[tp] = [create_complex_obj(tp, obj, self.__base_store.get_as_dict()) for obj in objlist]
        return real_objmap

    def __set_id_if_none(self, pcctype, objjson):
        if self.__typename_to_primarykey[pcctype.__name__] not in objjson:
            objjson[self.__typename_to_primarykey[pcctype.__name__]] = str(uuid.uuid4())
        return objjson

    def __make_pcc(self, pcctype, relevant_objs, params):
        universe = []
        param_list = []
        robjs = self.__convert_to_objects(relevant_objs)
        pobjs = self.__convert_to_objects(params)
        for tp in pcctype.__ENTANGLED_TYPES__:
            universe.append(robjs[tp])
        if hasattr(pcctype, "__parameter_types__"):
            for tp in pcctype.__parameter_types__:
                param_list.append(pobjs[tp])

        try:
            pcc_objects = pcc.create(pcctype, *universe, params = param_list)
        except TypeError, e:
            logging.warn("Exception in __make_pcc: " + e.message)
            return []
        return [self.__set_id_if_none(pcctype.Class(), create_jsondict(obj)) for obj in pcc_objects]

    def __construct_pccs(self, pcctype, pccs):
        paramtypes = list(pcctype.__parameter_types__) if hasattr(pcctype, "__parameter_types__") else []
        dependent_types = list(pcctype.__ENTANGLED_TYPES__)
        dependent_pccs = [tp for tp in (dependent_types + paramtypes) if not tp.__PCC_BASE_TYPE__]
        to_be_resolved = [tp for tp in dependent_pccs if tp not in pccs]
        for tp in to_be_resolved:
            self.__construct_pccs(tp, pccs)
        params = dict([(tp,
               self.__base_store.get_by_type(tp).values()
                 if tp.__PCC_BASE_TYPE__ else
               pccs[tp]) for tp in paramtypes])

        relevant_objs = dict([(tp,
               self.__base_store.get_by_type(tp).values()
                 if tp.__PCC_BASE_TYPE__ else
               pccs[tp]) for tp in dependent_types])

        pccs[pcctype] = self.__make_pcc(pcctype, relevant_objs, params)

    def __calculate_pcc(self, pcctype, params):
        pccs = {}
        self.__construct_pccs(pcctype, pccs)
        pccsmap = {}
        pccsmap[pcctype.__realname__] = dict([
                    (obj[self.__typename_to_primarykey[pcctype.__realname__]], obj)
                    for obj in pccs[pcctype]
                ]) if pcctype in pccs else {}

        return pccsmap

    def get_app_list(self):
        return self.__apps

    def clear(self, tp=None):
        if tp:
            for objid in self.__base_store.get_by_type(tp).keys():
                self.__base_store.delete(tp, objid)
        else:
            for tp in DATAMODEL_TYPES:
                for objid in self.__base_store.get_by_type(tp).keys():
                    self.__base_store.delete(tp, objid)


    def get(self, tp, id=None):
        if id:
            return self.__base_store.get(tp, id)
        else:
            return self.__base_store.get_by_type(tp)

    def get_ids(self, tp):
        return self.__base_store.get_ids(tp)

    def pause(self):
        self.__framelock.acquire()
        for app in self.__copylock:
            self.__copylock[app].acquire()

    def unpause(self):
        for app in self.__copylock:
            self.__copylock[app].release()
        self.__framelock.release()

    def get_lock(self, app):
        return self.__copylock[app]

    def get_update(self, tp, app, params = None, tracked_only = False):
        # get dynamic pccs with/without params
        # can
        tpname = tp.__realname__
        new, mod, deleted, touched  = {tpname: {}}, {tpname: {}}, {tpname: []}, set()
        # pccs are always recalculated from scratch. Easier
        if not tp.__PCC_BASE_TYPE__:
            if tp.__realname__ in self.__app_to_dynamicpcc[app]:
                mod, new, deleted, touched = mod, self.__calculate_pcc(
                    tp,
                    params), deleted, set([tpname])
        else:
        # take the base changes from the dictionary. Should have been updated with all changes.
            alltps = [tp]
            if tp in self.__base2derived:
                alltps.extend(self.__base2derived[tp])
            f_mod_t, f_new_t, f_deleted_t = {}, {}, []
            for dtp in alltps:
                dtpname = dtp.__realname__
                new_t, mod_t, deleted_t = self.__cache.get_all_updates(app, dtpname)
                f_mod_t.update(mod_t)
                f_new_t.update(new_t)
                f_deleted_t.extend(deleted_t)
            mod = {tpname: f_mod_t} if f_mod_t else {}
            new = {tpname: f_new_t} if f_new_t else {}
            deleted = {tpname: f_deleted_t} if f_deleted_t else {}
            touched = set([t.__realname__ for t in alltps])
        return new, mod if not tracked_only else {}, deleted, touched

    def clear_buffer(self, app, tp, tracked_only = False):
        (self.__cache.reset_tracking_cache_for_type(app, tp)
            if tracked_only else
        self.__cache.reset_cache_for_type(app, tp))


    def put_update(self, app, tp, new, mod, deleted):
        if tp.__PCC_BASE_TYPE__:
            return self.__put_update(app, tp, new, mod, deleted)
        types = tp.__ENTANGLED_TYPES__
        # Join types would be updated from each individual part
        if len(types) == 1:
            # dependent types other than projection
            # are not allowed for new and delete
            # the join object cannot have changes,
            # Each sub object in join tracks itself.
            base_tp = types[0]
            isprojection = hasattr(tp, "__pcc_projection__") and tp.__pcc_projection__ == True
            return self.put_update(app, types[0], new if isprojection else {}, mod, set())

    def __put_update(self, this_app, tp, new, mod, deleted):
        other_apps = set()
        if tp.__realname__ in self.__base_store.get_base_types():
            if tp.__realname__ in self.__type_to_app:
                other_apps = set(self.__type_to_app[tp.__realname__]).difference(set([this_app]))
        for id in new:
            self.__base_store.put(tp, id, new[id])
            if id not in self.__gc[this_app][tp.__realname__]:
                self.__gc[this_app][tp.__realname__].add(id)
        for app in other_apps:
            with self.__copylock[app]:
                self.__cache.add_new(app, tp.__realname__, new)

        other_apps = set()
        if tp.__realname__ in self.__base_store.get_base_types():
            if tp.__realname__ in self.__type_to_app:
                other_apps = set(self.__type_to_app[tp.__realname__]).difference(set([this_app]))
        for id in mod:
            self.__base_store.update(tp, id, mod[id])
        for app in other_apps:
            with self.__copylock[app]:
                self.__cache.add_updated(app, tp.__realname__, mod)

        other_apps = set()
        if tp.__realname__ in self.__base_store.get_base_types():
            if tp.__realname__ in self.__type_to_app:
                other_apps = set(self.__type_to_app[tp.__realname__]).difference(set([this_app]))

        producer_apps = [app for app in self.__gc if tp.__realname__ in self.__gc[app]]
        for id in deleted:
            for app in producer_apps:
                if id in self.__gc[app][tp.__realname__]:
                    self.__gc[app][tp.__realname__].remove(id)
            self.__base_store.delete(tp, id)

        for app in other_apps:
            with self.__copylock[app]:
                self.__cache.add_deleted(app, tp.__realname__, deleted)

    def register_app(self, app, typemap, name2class, name2baseclasses):
        self.__framelock.acquire()
        try:
            self.__apps.add(app)
            # Setup structure for garbage collection
            self.__gc[app] = {}
            producer, deleter, tracker, getter, gettersetter, setter = (
                set(typemap.setdefault("producing", set())),
                set(typemap.setdefault("deleting", set())),
                set(typemap.setdefault("tracking", set())),
                set(typemap.setdefault("getting", set())),
                set(typemap.setdefault("gettingsetting", set())),
                set(typemap.setdefault("setting", set()))
              )
            types_allowed = set(tracker).union(
                                              set(getter)).union(
                                              set(gettersetter)).union(
                                              set(setter))
            types_to_get = set()
            self.__base2derived
            for strtp in types_allowed:
                types_to_get.add(strtp)
                actual_cls = name2class[strtp]
                if actual_cls in self.__base2derived:
                    types_to_get.update(
                        set([tp.__realname__ for tp in self.__base2derived[actual_cls]]))
                types_extra = types_to_get.difference(types_allowed)
            self.__cache.register_app(app, types_allowed, types_extra)

            if app in self.__copylock:
                try:
                    with self.__copylock[app]:
                        # Clean-up old registration
                        for strtp in self.__type_to_app.keys():
                            if app in self.__type_to_app[strtp]:
                                del self.__type_to_app[strtp][app]

                        del self.__copylock[app]
                except:
                    pass

            self.__copylock[app] = RLock()
            self.__app_to_dynamicpcc[app] = set()

            with self.__copylock[app]:
                self.__cache.reset_cache_for_all_types(app)
                mod, new, deleted = ({}, {}, [])
                base_types = set()
                for str_tp in types_to_get:
                    tp = name2class[str_tp]
                    logging.debug("register " + str_tp + " by " + str(app) + " " + str(tp))
                    mod = {}
                    new = {}
                    deleted = []
                    if tp.__PCC_BASE_TYPE__:
                        base_types.add(tp)
                        new = self.__base_store.get_by_type(tp)
                        self.__cache.add(app, tp.__realname__, new, mod, deleted)
                    else:
                        bases = name2baseclasses[tp.__realname__]
                        for base in bases:
                            base_types.add(base)
                        self.__app_to_dynamicpcc[app].add(tp.__realname__)
                    self.__type_to_app.setdefault(tp.__realname__, set()).add(app)

                # Add producer and deleter types to base_store, but not to basechanges
                for str_tp in producer.union(set(deleter)):
                    tp = name2class[str_tp]
                    if str_tp in producer:
                        self.__gc[app][tp.__realname__] = set()
                    if tp.__PCC_BASE_TYPE__:
                        base_types.add(tp)
                    else:
                        bases = name2baseclasses[tp.__realname__]
                        for base in bases:
                            base_types.add(base)
                final_base_types = [b for b in base_types if b.__PCC_BASE_TYPE__]
                self.__base_store.add_types(final_base_types)
        except:
            raise
        finally:
            self.__framelock.release()

    def gc(self, this_app, name2class):
        mylock = self.__copylock[this_app]
        self.pause()
        try:
            self.logger.warn("Application %s disconnected. Removing owned objects.",
                         this_app)

            for strtp in self.__type_to_app.keys():
                if this_app in self.__type_to_app[strtp]:
                    self.__type_to_app[strtp].remove(this_app)

            other_apps = set()

            # delete all owned objects, and inform other simulations of deleted objects
            for strtp in self.__gc[this_app]:
                if strtp in self.__base_store.get_base_types() and strtp in self.__type_to_app:
                    other_apps = set(self.__type_to_app[strtp]).difference(set([this_app]))
                    for app in other_apps:
                        self.__cache.add_deleted(app, strtp, self.__gc[this_app][strtp])
                        for oid in self.__gc[this_app][strtp]:
                            self.__base_store.delete(name2class[strtp], oid, False)
            del self.__copylock[this_app]
            del self.__gc[this_app]
            self.__apps.remove(this_app)
            self.__cache.delete_app(this_app)
        except:
            raise
        finally:
            self.unpause()
            mylock.release()

