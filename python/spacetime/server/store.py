'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import json
from time import sleep
from pcc.recursive_dictionary import RecursiveDictionary
from spacetime.common.wire_formats import FORMATS
from datamodel.all import DATAMODEL_TYPES
from pcc.dataframe.dataframe_threading import dataframe_wrapper as dataframe
from pcc.dataframe.application_queue import ApplicationQueue
from spacetime.common.modes import Modes
from spacetime.common.converter import create_jsondict, create_complex_obj


FETCHING_MODES = set([Modes.Getter, 
                      Modes.GetterSetter,
                      Modes.Taker])
TRACKING_MODES = set([Modes.Tracker])
PUSHING_MODES = set([Modes.Deleter,
                     Modes.GetterSetter,
                     Modes.Setter,
                     Modes.TakerSetter,
                     Modes.Producing])
ALL_MODES = set([Modes.Deleter,
                 Modes.GetterSetter,
                 Modes.Setter,
                 Modes.TakerSetter,
                 Modes.Producing,
                 Modes.Tracker,
                 Modes.Getter])

class dataframe_stores(object):
    def __init__(self, name2class):
        self.master_dataframe = dataframe()
        self.app_to_df = {}
        self.name2class = name2class
        self.pause_servers = False
        self.app_wire_format = {}


    def __pause(self):
        while self.pause_servers:
            sleep(0.1)
        
    def add_new_dataframe(self, name, df):
        self.__pause()    
        self.app_to_df[name] = df

    def delete_app(self, app):
        self.__pause()
        del self.app_to_df[app]

    def register_app(self, app, type_map, wire_format = "json"):
        self.__pause()
        types_to_get = set()
        for mode in FETCHING_MODES:
            types_to_get.update(set(type_map.setdefault(mode, set())))
        types_to_track = set()
        for mode in TRACKING_MODES:
            types_to_track.update(set(type_map.setdefault(mode, set())))
        types_to_track = types_to_track.difference(types_to_get)
        real_types_to_get = [self.name2class[tpstr] for tpstr in types_to_get]
        real_types_to_track = [self.name2class[tpstr] for tpstr in types_to_track]
        self.master_dataframe.add_types(real_types_to_get + real_types_to_track)
        
        df = ApplicationQueue(app, real_types_to_get + real_types_to_track, self.master_dataframe)
        self.add_new_dataframe(app, df)
        # Add all types to master.
        types_to_add_to_master = set()
        for mode in ALL_MODES:
            types_to_add_to_master.update(set(type_map.setdefault(mode, set())))
        self.master_dataframe.add_types([self.name2class[tpstr] for tpstr in types_to_add_to_master])
        self.app_wire_format[app] = wire_format


    def disconnect(self, app):
        self.__pause()
        if app in self.app_to_df:
            self.delete_app(app)

    def reload_dms(self, datamodel_types):
        self.__pause()
        pass

    def update(self, app, changes):
        #print json.dumps(changes, sort_keys = True, separators = (',', ': '), indent = 4) 
        self.__pause()
        dfc_type, content_type = FORMATS[self.app_wire_format[app]]
        dfc = dfc_type()
        dfc.ParseFromString(changes)
        if app in self.app_to_df:
            self.master_dataframe.apply_changes(dfc, except_app = app)

    def getupdates(self, app):
        self.__pause()
        dfc_type, content_type = FORMATS[self.app_wire_format[app]]
        final_updates = dfc_type()
        if app in self.app_to_df:
            final_updates = dfc_type(self.app_to_df[app].get_record())
            self.app_to_df[app].clear_record()
        return final_updates.SerializeToString(), content_type

    def get_app_list(self):
        return self.app_to_df.keys()

    def clear(self, tp = None):
        if not tp:
            self.__init__(self.name2class)
        else:
            if tp in self.master_dataframe.object_map:
                del self.master_dataframe.object_map[tp]
            if tp in self.master_dataframe.current_state:
                del self.master_dataframe.current_state[tp]

    def pause(self):
        self.pause_servers = True

    def unpause(self):
        self.pause_servers = False

    def gc(self, sim):
        # For now not clearing contents
        self.delete_app(sim)
        

    def get(self, tp):
        return [create_jsondict(o) for o in self.master_dataframe.get(tp)]

    def put(self, tp, objs):
        real_objs = [create_complex_obj(tp, obj, self.master_dataframe.object_map) for obj in objs.values()]
        tpname = tp.__realname__
        gkey =  self.master_dataframe.member_to_group[tpname]
        if gkey == tpname:
            self.master_dataframe.extend(tp, real_objs)
        else:
            for obj in real_objs:
                oid = obj.__primarykey__
                if oid in self.master_dataframe.object_map[gkey]:
                    # do this only if the object is already there.
                    # cannot add an object if it is a subset (or any other pcc type) type if it doesnt exist.
                    for dim in obj.__dimensions__:
                        # setting attribute to the original object, so that changes cascade
                        setattr(self.master_dataframe.object_map[gkey][oid], dim._name, getattr(obj, dim._name))
        


