import asyncio
import cbor
import os
import sys
from uuid import uuid4
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Queue
from spacetime.managers.version_graph import Graph
import spacetime.utils.utils as utils
from spacetime.utils.enums import Event, AutoResolve
from spacetime.debugger.debugger_types import AppToState
import time
from rtypes import pcc_set, primarykey, dimension, merge
import uuid
from copy import deepcopy

class VersionManager(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, appname, types, dump_graph=None):
        self.types = types
        self.type_map = {tp.__r_meta__.name: tp for tp in types}
        self.logger = utils.get_logger("%s_VersionManager" % appname)
        self.dump_graphs = dump_graph

    @abstractmethod
    def receive_data(self, appname, versions, package, from_external=True):
        pass

    @abstractmethod
    def retrieve_data(self, appname, version):
        pass

    @abstractmethod
    def data_sent_confirmed(self, app, version):
        pass

    @abstractmethod
    def _read_dimension_at(self, version, dtype, oid, dimname):
        pass

    def operational_transform(
            self, start, new_path, conflict_path, from_external):
        # This will contain all changes in the merge that do not conflict with
        # new changes in place + merged resolutions.
        t_new_merge = dict()
        # This will contain all merged resolutions. + changes in place not with
        # this version change.
        t_conflict_merge = dict()
        for tpname in new_path:
            if tpname in conflict_path:
                # Merge tp changes.
                tp_merge, tp_conf_merge = self.ot_on_type(
                    tpname, start, new_path[tpname], conflict_path[tpname],
                    from_external)
                t_new_merge[tpname] = tp_merge
                t_conflict_merge[tpname] = tp_conf_merge
            else:
                # tp only in new, not in conflict.
                # Can add this to changes that divergent path would need.
                t_conflict_merge[tpname] = new_path[tpname]
        for tpname in conflict_path:
            if tpname not in new_path:
                # tp only in conflict, not in master branch.
                # Can add this to changes that master branch would need.
                t_new_merge[tpname] = conflict_path[tpname]
        return t_new_merge, t_conflict_merge

    def ot_on_type(
            self, dtpname, start, new_tp_change, conflict_tp_change,
            from_external):
        tp_merge = dict()
        tp_conf_merge = dict()
        for oid in new_tp_change:
            if oid in conflict_tp_change:
                # Merge oid change.
                obj_merge, obj_conf_merge = self.ot_on_obj(
                    dtpname, oid,
                    start, new_tp_change[oid], conflict_tp_change[oid],
                    from_external)
                tp_merge[oid] = obj_merge
                tp_conf_merge[oid] = obj_conf_merge
            else:
                # obj only in master path.
                tp_conf_merge[oid] = new_tp_change[oid]
        for oid in conflict_tp_change:
            if oid not in new_tp_change:
                # oid only in conflict, not in master.
                # Add it to master.
                tp_merge[oid] = conflict_tp_change[oid]
        return tp_merge, tp_conf_merge

    def ot_on_obj(
            self, dtpname, oid, start, new_obj_change, conf_obj_change,
            from_external):
        dtype = self.type_map[dtpname]
        obj_merge = dict()
        obj_conf_merge = dict()

        # dtpname event determines the base type's changes.
        event_new = new_obj_change["types"][dtpname]
        event_conf = conf_obj_change["types"][dtpname]
        if event_new is Event.New and event_conf is Event.New:
            # Both paths have created a new object.
            # Resolve that.
            if dtype.__r_meta__.merge is not None:
                new = self.make_temp_obj(
                        start, dtype, oid, with_change=new_obj_change)  # new
                conflicting = self.make_temp_obj(
                        start, dtype, oid,
                         with_change=conf_obj_change)  # conflicting
                yours = new if from_external else conflicting
                theirs = conflicting if from_external else new
                obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                    dtype, oid,
                    None,  # original,
                    yours,
                    theirs,
                    new_obj_change, conf_obj_change)
            else:
                # Choose the New as other apps might have started work
                # on this object.
                obj_merge = dict()
                obj_conf_merge = self.dim_diff(dtpname, conf_obj_change, new_obj_change)
        elif event_new is Event.New and event_conf is Event.Modification:
            # This has to be an error. How did an object get modified if
            # the object was not there at start.
            raise RuntimeError(
                "Divergent modification received when object was"
                " created in the main line.")
        elif event_new is Event.New and event_conf is Event.Delete:
            # This has to be an error. How did an object get deleted if
            # the object was not there at start.
            raise RuntimeError(
                "Divergent deletion received when "
                "object was created in the main line.")
        elif event_new is Event.Modification and event_conf is Event.New:
            # resolving between an modified object and
            #  an object that was sent as a new record.
            # Should be error again as this would not be possible.
            # If the object was modified from the branch line,
            # then it existed in the branch.
            # If the object existed, then the diverging app should
            # have had the obj too as it
            # forked from that point. So the object cannot be returned as new.
            raise RuntimeError(
                "Divergent deletion received when"
                " object was created in the main line.")
        elif (event_new is Event.Modification
                  and event_conf is Event.Modification):
            # resolve between two different modifications.
            if dtype.__r_meta__.merge is not None:
                new = self.make_temp_obj(
                        start, dtype, oid, with_change=new_obj_change)  # new
                conflicting = self.make_temp_obj(
                        start, dtype, oid,
                        with_change=conf_obj_change)  # conflicting
                yours = new if from_external else conflicting
                theirs = conflicting if from_external else new
                obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                    dtype, oid,
                    self.make_temp_obj(start, dtype, oid),  # original
                    yours,  # new
                    theirs,  # conflicting
                    new_obj_change, conf_obj_change)
            else:
                # LWW strategy
                obj_merge = self.dim_diff(dtpname, new_obj_change, conf_obj_change)
                obj_conf_merge = self.dim_not_present(
                    conf_obj_change, new_obj_change)

        elif event_new is Event.Modification and event_conf is Event.Delete:
            # resolve between an app modifyinbg it,
            # and another app deleting the object.
            if dtype.__r_meta__.merge is not None:
                new = self.make_temp_obj(
                        start, dtype, oid, with_change=new_obj_change)  # new
                conflicting = None,  # conflicting
                yours = new if from_external else conflicting
                theirs = conflicting if from_external else new
                obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                    dtype, oid,
                    self.make_temp_obj(start, dtype, oid),  # original
                    yours,  # new
                    theirs,  # conflicting
                    new_obj_change, conf_obj_change)
            else:
                # LWW strategy
                obj_merge = conf_obj_change
                obj_conf_merge = dict()

        elif event_new is Event.Delete and event_conf is Event.New:
            # This has to be an error again using the
            # logic explained in the above comments.
            raise RuntimeError(
                "Divergent deletion received when "
                "object was created in the main line.")
        elif event_new is Event.Delete and event_conf is Event.Modification:
            # resolve between an app modifyinbg it,
            # and another app deleting the object.
            if dtype.__r_meta__.merge is not None:
                new = None,  # new
                conflicting = self.make_temp_obj(
                        start, dtype, oid,
                         with_change=conf_obj_change)  # conflicting
                yours = new if from_external else conflicting
                theirs = conflicting if from_external else theirs
                obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                    dtype, oid,
                    self.make_temp_obj(start, dtype, oid),  # original
                    yours,  # new
                    theirs,  # conflicting
                    new_obj_change, conf_obj_change)
            else:
                # LWW strategy
                obj_merge = conf_obj_change
                obj_conf_merge = dict()
        elif event_new is Event.Delete and event_conf is Event.Delete:
            # Both apps are deleting it, just delete it.
            # and so do nothing as the changes are already there.
            pass
        return obj_merge, obj_conf_merge

    def resolve_with_custom_merge(
            self, dtype, oid, original, new, conflicting,
            new_obj_change, conf_obj_changes):
        obj = dtype.__r_meta__.merge(original, new, conflicting)  # conflicting
        dtpname = dtype.__r_meta__.name
        if obj:
            obj.__r_temp__.update(dtype.__r_table__.store_as_temp[oid])
            changes = {
                "dims": obj.__r_temp__, "types": dict()}
            changes["types"][dtpname] = (
                Event.Modification if original is not None else Event.New)

            del dtype.__r_table__.store_as_temp[oid]
            return (self.dim_diff(dtpname, new_obj_change, changes),
                    self.dim_diff(dtpname, conf_obj_changes, changes))
        else:
            # Object was deleted.
            return (
                {"types": {dtpname: Event.Delete}},
                {"types": {dtpname: Event.Delete}})

    def make_temp_obj(self, version, dtype, oid, with_change=dict()):
        obj = utils._container()
        obj.__class__ = dtype
        obj.__r_oid__ = oid
        obj.__r_temp__ = {
            dimname: (
                with_change["dims"][dimname]
                if "dims" in with_change and dimname in with_change["dims"] else
                self._read_dimension_at(version, dtype, oid, dimname))
            for dimname in dtype.__r_meta__.dimmap}

        dtype.__r_table__.store_as_temp[oid] = dict()
        return obj

    def dim_diff(self, dtpname, original, new):
        # return dims that are in new but not in/different in the original.
        change = {"dims": dict(), "types": dict()}
        if not (original and "dims" in original):
            return new
        for dim in new["dims"]:
            if (dim not in original["dims"]
                    or original["dims"][dim] != new["dims"][dim]):
                # The dim is not in original or the dim is there,
                # but the values are different.
                # copy it.
                change["dims"][dim] = new["dims"][dim]
        change["types"].update(original["types"])
        change["types"].update(new["types"])
        change["types"][dtpname] = Event.Modification
        return change

    def dim_not_present(self, original, new):
        # return dims that are in new but not in in the original.
        change = {"dims": dict(), "types": dict()}
        if not (original and "dims" in original):
            return new
        for dim in new["dims"]:
            if dim not in original["dims"]:
                # The dim is not in original or the dim is there,
                # but the values are different.
                # copy it.
                change["dims"][dim] = new["dims"][dim]
        change["types"].update(original["types"])
        change["types"].update(new["types"])
        return change

    def maintain(
            self, state_to_app, app_to_state,
            graph, appname, end_v, merge_func):
        # reset the state markers.
        state_to_app.setdefault(end_v, set()).add(appname)
        if appname in app_to_state:
            if app_to_state[appname] == end_v:
                self.logger.debug("Don't need to maintain.")
                return
            old_v = app_to_state[appname]
            state_to_app[old_v].remove(appname)
            if not state_to_app[old_v]:
                del state_to_app[old_v]
        app_to_state[appname] = end_v
        self.logger.debug(
            "Maintaining with {0}, {1}".format(
                app_to_state, state_to_app))
        # Clean up states.

        graph.maintain(state_to_app, merge_func)
        if self.dump_graphs:
            self.dump(self.dump_graphs, graph)

    def dump(self, folder, graph):
        g = ["DiGraph G{"]
        for node in graph.nodes:
            g.append("\t\"{0}\"[color={1}];".format(node[:4], "red" if graph.nodes[node].is_master else "blue"))
        for key in graph.edges:
            g.append("\t\"{0}\" -> \"{1}\";".format(key[0][:4], key[1][:4]))
        g.append("}")
        gstr = "\n".join(g)
        count = len(os.listdir(folder))
        open(os.path.join(folder, "heap{}.dot".format(count)), "w").write(gstr)

#
# @pcc_set
# class AppState(object):
#     oid = primarykey(str)
#     appname = dimension(str)
#     state_to_app = dimension(dict)
#
#     def __init__(self, appname):
#         self.oid = str(uuid.uuid4())
#         self.appname = appname
#         self.state_to_app = dict()

class FullStateVersionManager(VersionManager):
    @property
    def head(self):
        return self.version_graph.head.current



    def __init__(
            self, appname, types,
            dump_graph=None, instrument_record=None, resolver=None, 
            autoresolve=AutoResolve.FullResolve, mem_instrument=False, debug=None):
        self.appname = appname
        self.types = types
        self.type_map = {tp.__r_meta__.name: tp for tp in types}
        self.version_graph = Graph(debug)
        self.state_to_app = dict()
        # self.state_to_app = AppState(appname).state_to_app
        if debug:
            self.app_to_state = debug.read_one(AppToState, self.appname)
            assert self.app_to_state is not None
        else:
            self.app_to_state = dict()
        self.logger = utils.get_logger("%s_FullStateVersionManager" % appname)
        self.dump_graphs = dump_graph
        self.instrument_record = instrument_record
        self.resolver = resolver
        self.autoresolve = autoresolve
        self.mem_instrument = mem_instrument
        self.debug = debug
        if mem_instrument:
            self.mem_usage = list()
            self.record_mem_usage()

    def record_mem_usage(self):
        self.mem_usage.append(
            (time.time(), len(self.version_graph.edges),
             sum(sys.getsizeof(cbor.dumps(e.payload)) for e in self.version_graph.edges.values())))

    def set_app_marker(self, appname, end_v):
        self.state_to_app.setdefault(end_v, set()).add(appname)


    def receive_data(self, appname, versions, package, from_external=True):
        new_nodes =list()
        new_edges = list()
        modified_edges = list()
        start_v, end_v = versions
        if start_v == end_v:
            # The versions are the same, lets ignore.
            return True
        if self.autoresolve is AutoResolve.BranchExternalPush:
            self.version_graph.continue_chain(
                start_v, end_v, package, appname != self.appname)

        if start_v != self.version_graph.head.current:
            if self.autoresolve is AutoResolve.FullResolve:
                self.resolve_conflict(start_v, end_v, package, from_external)
            elif self.autoresolve is AutoResolve.BranchConflicts:
                self.version_graph.continue_chain(start_v, end_v, package)
        else:
            self.version_graph.continue_chain(start_v, end_v, package)
        if self.mem_instrument:
            self.record_mem_usage()
        if not self.debug:
            self.maintain(appname, end_v)
        if self.mem_instrument:
            self.record_mem_usage()
        return True


    def retrieve_data(self, appname, version):
        data, version_change = self.retrieve_data_nomaintain(version)
        self.set_app_marker(appname, version_change[1])
        return data, version_change

    def retrieve_data_nomaintain(self, version):
        merged = dict()
        if version not in self.version_graph.nodes:
            return merged, [version, version]
        for delta in self.version_graph[version:]:
            merged = utils.merge_state_delta(merged, delta)
        return merged, [version, self.version_graph.head.current]

    def data_sent_confirmed(self, app, version):
        if version[0] != version[1]:
            self.maintain(app, version[1])
            if self.mem_instrument:
                self.record_mem_usage()
        

    def resolve_conflict(self, start_v, end_v, package, from_external):
        new_v = self.version_graph.head.current
        change, _ = self.retrieve_data_nomaintain(start_v)
        t_new_merge, t_conflict_merge = self.operational_transform(
            start_v, change, package, from_external)
        merge_v = str(uuid4())
        self.version_graph.continue_chain(start_v, end_v, package)
        self.version_graph.continue_chain(new_v, merge_v, t_new_merge)
        self.version_graph.continue_chain(end_v, merge_v, t_conflict_merge)
        return

    def _read_dimension_at(self, version, dtype, oid, dimname):
        dtpname = dtype.__r_meta__.name
        for change in self.version_graph[version::-1]:
            if dtpname in change and oid in change[dtpname]:
                if ("dims" in change[dtpname][oid]
                        and dimname in change[dtpname][oid]["dims"]):
                    return change[dtpname][oid]["dims"][dimname]
                if ("types" in change[dtpname][oid]
                        and dtpname in change[dtpname][oid]["types"]
                        and change[dtpname][oid]["types"][dtpname] == Event.Delete):
                    # Object has been deleted before this version,
                    # and has not been added.
                    # Do not return a value.
                    break

    def maintain(self, appname, end_v):
        super().maintain(
            self.state_to_app, self.app_to_state,
            self.version_graph, appname, end_v, utils.merge_state_delta)
        if self.instrument_record:
            self.instrument_record.put(
                ("MEMORY", "{0}\t{1}\t{2}\n".format(time.time(), len(self.version_graph.nodes), len(self.version_graph.edges))))
