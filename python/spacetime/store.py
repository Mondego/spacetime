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
import json
import os
import sys
import uuid


# not active object.
# just stores the basic sets.
# requires set types that it has to store.
# must accept changes.
# must return object when asked for it.
class store(object):
    def __init__(self):
        # actual type objects
        self.__sets = set()

        # type -> {id : object} object is just json style recursive dictionary.
        # Onus on the client side to make objects
        self.__data = RecursiveDictionary()

    def add_types(self, types):
        # types will be list of actual type objects, not names. Load it somewhere.
        self.__sets.update(types)
        for tp in types:
            self.__data.setdefault(tp, RecursiveDictionary())

    def get_base_types(self):
        # returns actual types back.
        return self.__sets

    def get(self, tp, id):
        # assume it is there, else raise exception.
        return self.__data[tp][id]

    def get_ids(self, tp):
        return self.__data[tp].keys()

    def get_by_type(self, tp):
        return self.__data[tp]

    def get_as_dict(self):
        return self.__data

    def put(self, tp, id, object):
        # assume object is just dictionary, and not actual object.
        try:
            self.__data[tp][id] = object
        except:
            print "error finding id %s on type %s" % (id, tp.Class())
            raise

    def update(self, tp, id, object_changes):
        self.__data[tp][id].update(object_changes)

    def delete(self, tp, id):
        del self.__data[tp][id]


class st_dataframe(object):
    '''
    Dummy class for dataframe and pccs to work nicely
    '''
    def __init__(self, objs):
        self.items = objs

    def getcopies(self):
        return self.items

    def merge(self):
        pass

    def _change_type(self, baseobj, actual):
        class _container(object):
            pass
        newobj = _container()
        newobj.__dict__ = baseobj.__dict__
        newobj.__class__ = actual
        return newobj



# not active object.
# wrapper on store, that allows it to keep track of app, and subset merging etc.
# needs app.
# maintains list of changes to app.
# mod, new, delete are the three sets that have to be maintained for app.
class dataframe(object):
    def __init__(self, ):
        self.__base_store = store()
        self.__apps = set()

        # app -> mod, new, deleted
        # mod, new : tp -> id -> object changes/full object
        # deleted: list of ids deleted
        self.__app_to_basechanges = RecursiveDictionary()

        # app -> list of dynamic types tracked by app.
        self.__app_to_dynamicpcc = {}

        # app -> copylock
        self.__copylock = {}

        # Type -> List of apps that use it
        self.__type_to_app = {}

        self.__typename_to_primarykey = {}
        for tp in DATAMODEL_TYPES:
            if tp.__PCC_BASE_TYPE__ or tp.__name__ == "_Join":
                self.__typename_to_primarykey[tp.Class().__name__] = tp.__primarykey__._name
            else:
                self.__typename_to_primarykey[tp.Class().__name__] = tp.__ENTANGLED_TYPES__[0].__primarykey__._name

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
            if len(universe) == 1:
                universe = universe[0]
            pcc_bind = pcctype(universe = st_dataframe(universe), params = param_list)
            pcc_bind.create_snapshot()
        except TypeError, e:
            return []
        return [self.__set_id_if_none(pcctype.Class(), create_jsondict(obj)) for obj in pcc_bind.All()]

    def __construct_pccs(self, objs, pcctypelist, completed, pccs):
        incomplete = set(pcctypelist)
        params = {}
        while len(incomplete) != 0:
            for pcctype in pcctypelist:
                if pcctype in completed:
                    continue
                if completed.union(set(pcctype.__ENTANGLED_TYPES__)) == completed:
                    relevant_objs = dict(
                        [(tp,
                          objs[tp] if tp in objs else pccs[tp])
                         for tp in pcctype.__ENTANGLED_TYPES__]
                      )
                    if hasattr(pcctype, "__parameter_types__"):
                        for param_tp in pcctype.__parameter_types__:
                            if param_tp in relevant_objs:
                                continue
                            if param_tp.__PCC_BASE_TYPE__:
                                params.setdefault(param_tp, []).extend(self.__base_store.get_by_type(param_tp).values())
                            elif param_tp in pccs:
                                params.setdefault(param_tp, []).extend(pccs[param_tp].values())
                            else:
                                self.__construct_pccs(objs, [param_tp], completed, pccs)
                                params.setdefault(param_tp, []).extend(pccs[param_tp])
                    pccs[pcctype] = self.__make_pcc(pcctype, relevant_objs, params)
                    completed.add(pcctype)
                    incomplete.remove(pcctype)


    def __calculate_pcc(self, basechanges, pcctypelist, params):
        objs = {}
        for tp in basechanges:
            mod, new, deleted = basechanges[tp]
            all_ids = set([id for id in mod]).union(set([id for id in new])).difference(set(deleted))
            all_ids = all_ids.union(set(self.__base_store.get_ids(tp)))
            objs[tp] = [self.__base_store.get(tp, id) for id in all_ids]

        pccs = {}
        self.__construct_pccs(objs, pcctypelist, set(self.__base_store.get_base_types()), pccs)
        pccsmap = {}
        for tp in pccs:
            pccsmap[tp] = dict([(obj[self.__typename_to_primarykey[tp.Class().__name__]], obj) for obj in pccs[tp]])

        return pccsmap

    def __convert_type_str(self, mod, new, deleted):
        new_mod, new_new, new_deleted = {}, {}, {}
        for tp in mod:
            new_mod[tp.Class().__name__] = mod[tp]
        for tp in new:
            new_new[tp.Class().__name__] = new[tp]
        for tp in deleted:
            new_deleted[tp.Class().__name__] = deleted[tp]

        return new_mod, new_new, new_deleted

    def get_update(self, tp, app, params = None, tracked_only = False):
        # get dynamic pccs with/without params
        # can
        with self.__copylock[app]:
            # pccs are always recalculated from scratch. Easier
            if not tp.__PCC_BASE_TYPE__:
                if tp in self.__app_to_dynamicpcc[app]:
                    mod, new, deleted = {}, self.__calculate_pcc(
                        self.__app_to_basechanges[app],
                        [tp],
                        params), {}
            else:
        # take the base changes from the dictionary. Should have been updated with all changes.
                mod_t, new_t, deleted_t = self.__app_to_basechanges[app][tp]
                self.__app_to_basechanges[app][tp] = (mod_t.fromkeys(mod_t, {})
                                                      if not tracked_only else mod_t,
                                                      {},
                                                      set())
                mod = {tp: mod_t} if mod_t else {}
                new = {tp: new_t} if new_t else {}
                deleted = {tp: deleted_t} if deleted_t else {}

        return self.__convert_type_str(new, mod if not tracked_only else {}, deleted)

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

    def __put_update(self, app, tp, new, mod, deleted):
        other_apps = set()
        if tp in self.__base_store.get_base_types():
            other_apps = set(self.__type_to_app[tp])
        for id in new:
            self.__base_store.put(tp, id, new[id])
        for app in other_apps:
            with self.__copylock[app]:
                self.__app_to_basechanges[app][tp][1].update(new)


        if tp in self.__base_store.get_base_types():
            other_apps = set(self.__type_to_app[tp])
        for id in mod:
            self.__base_store.update(tp, id, mod[id])
        for app in other_apps:
            with self.__copylock[app]:
                self.__app_to_basechanges[app][tp][0].update(mod)


        if tp in self.__base_store.get_base_types():
            other_apps = set(self.__type_to_app[tp])
        for id in deleted:
            self.__base_store.delete(tp, id)
        for app in other_apps:
            with self.__copylock[app]:
                self.__app_to_basechanges[app][tp][2].difference_update(set(deleted))

    def register_app(self, app, typemap, name2class, name2baseclasses):
        self.__apps.add(app)
        producer, deleter, tracker, getter, gettersetter, setter = (
            set(typemap.setdefault("producing", set())),
            set(typemap.setdefault("deleting", set())),
            set(typemap.setdefault("tracking", set())),
            set(typemap.setdefault("getting", set())),
            set(typemap.setdefault("gettingsetting", set())),
            set(typemap.setdefault("setting", set()))
          )
        self.__copylock[app] = Lock()
        with self.__copylock[app]:
            self.__app_to_basechanges[app] = {}
            mod, new, deleted = ({}, {}, {})
            base_types = set()
            for str_tp in set(tracker).union(set(getter)).union(set(gettersetter)).union(set(setter)):
                tp = name2class[str_tp]
                mod = {}
                new = {}
                deleted = set()
                if tp.__PCC_BASE_TYPE__:
                    base_types.add(tp)
                    self.__app_to_basechanges[app][tp] = (mod, new, deleted)
                    self.__type_to_app.setdefault(tp, set()).add(app)
                else:
                    bases = name2baseclasses[tp.Class().__name__]
                    for base in bases:
                        base_types.add(base)
                        self.__app_to_basechanges[app][base] = (mod, new, deleted)
                        self.__type_to_app.setdefault(base, set()).add(app)
                    self.__app_to_dynamicpcc.setdefault(app, set()).add(tp)
                self.__type_to_app.setdefault(tp, set()).add(app)
            self.__base_store.add_types(base_types)
