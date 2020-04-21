import logging
import os
import time
import cbor

from spacetime.utils.enums import Event
from rtypes.utils.enums import DiffType
#from copy import deepcopy

import networkx as nx
import matplotlib.pyplot as plt

class container(object):
    pass

def visualize_graph(version_graph):
    G = nx.DiGraph()
    root = version_graph.versions["ROOT"]
    the_queue = [root]
    visited = []
    # visited.append()
    G.add_node(root)
    while the_queue:
        parent = the_queue.pop(0)
        for i in parent.children:
            if (parent, i) not in visited:
                the_queue.append(i)
                visited.append((parent, i))
                try:
                    G.add_edge(parent, i[:7])
                except:
                    G.add_edge(parent, i)

    del visited

    # pos = graphviz_layout(G, prog='dot')
    plt.plot()
    nx.draw(G, with_labels=True)
    plt.show()

def get_logger(name, log_to_std):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if log_to_std:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    # if not os.path.exists("Logs"):
    #     os.makedirs("Logs")
    #fh = logging.FileHandler(os.path.join("Logs", name + ".log"))
    #fh.setLevel(logging.DEBUG)
    #fh.setFormatter(formatter)
    #logger.addHandler(fh)

    return logger

def deepcopy(the_dict):
    # This is faster than actual deepcopy.
    #return the_dict
    return cbor.loads(cbor.dumps(the_dict))

def merge_state_delta(old_change, newer_change, tp_to_dimmap, apply, delete_it=False):
    merged = dict()
    if old_change == dict():
        return deepcopy(newer_change)
    for dtpname in old_change:
        if dtpname not in newer_change:
            merged[dtpname] = (old_change[dtpname])
        else:
            merged[dtpname] = merge_objectlist_deltas(
                dtpname, old_change[dtpname], newer_change[dtpname],
                tp_to_dimmap, apply, delete_it=delete_it)
    for dtpname in newer_change:
        if dtpname not in old_change:
            merged[dtpname] = deepcopy(newer_change[dtpname])
    return merged

def merge_objectlist_deltas(
        dtpname, old_change, new_change, tp_to_dimmap, apply, delete_it=False):
    merged = dict()
    for oid in old_change:
        if oid not in new_change:
            merged[oid] = (old_change[oid])
        else:
            if delete_it and new_change[oid]["types"][dtpname] is Event.Delete:
                # Do not include this object in the merged changes if
                # delete is True
                continue
            if (old_change[oid]["types"][dtpname] is Event.New
                    and new_change[oid]["types"][dtpname] is Event.Delete):
                # Do not include this object as it was both created and deleted
                # in these merged changes.
                continue
            obj_data = merge_object_delta(
                dtpname, old_change[oid], new_change[oid],
                tp_to_dimmap[dtpname], apply)
            if not obj_data:
                obj_data = {"types": {dtpname: Event.Delete}}
            merged[oid] = obj_data
    for oid in new_change:
        if oid not in old_change:
            if delete_it and new_change[oid]["types"][dtpname] is Event.Delete:
                continue
            merged[oid] = deepcopy(new_change[oid])
    return merged

def merge_object_delta(dtpname, old_change, new_change, dimmap, apply):
    if not old_change:
        return deepcopy(new_change)
    if old_change["types"][dtpname] is Event.New and new_change["types"][dtpname] is Event.Delete:
        return None
    if new_change["types"][dtpname] is Event.Delete:
        return deepcopy(new_change)
    if (old_change["types"][dtpname] is Event.Delete
            and new_change["types"][dtpname] is Event.New):
        return deepcopy(new_change)
    if not (new_change["types"][dtpname] is Event.Modification 
            or (new_change["types"][dtpname] is Event.New 
                and old_change["types"][dtpname] is Event.New)):
        raise RuntimeError(
            "Not sure why the new change does not have modification.")
    if old_change["types"][dtpname] is Event.Delete:
        return (old_change)
    dim_change = (old_change["dims"])
    for dim in new_change["dims"]:
        if dim in dim_change and dimmap[dim].custom_diff:
            if new_change["dims"][dim]["type"] != DiffType.NEW:
                func = (
                    dimmap[dim].custom_diff.apply
                    if apply else
                    dimmap[dim].custom_diff.merge)
                value = func(
                    dim_change[dim]["value"], new_change["dims"][dim]["value"])
                dim_change[dim] = {
                    "value": value,
                    "type": DiffType.MOD
                }
                continue
        dim_change[dim] = new_change[dim]
    type_change = dict()
    for tpname, old_event in old_change["types"].items():
        new_event = (
            new_change["types"][tpname]
            if tpname in new_change["types"] else
            None)
        if new_event is None:
            # The new record doesnt change anything for the type.
            # use the old record.
            type_change[tpname] = old_event
        elif new_event is Event.Delete:
            type_change[tpname] = new_event
        elif old_event is Event.New:
            # Only if previous elif condition is satisfied.
            # Do not swap elif positions
            # or add the not condition of previos elif if required.
            type_change[tpname] = old_event
        else:
            type_change[tpname] = new_event
    for tpname, new_event in new_change["types"].items():
        if tpname not in old_change["types"]:
            type_change[tpname] = new_event

    return {"types": type_change, "dims": dim_change}

def make_obj(dtype, oid):
    obj = container()
    obj.__class__ = dtype
    obj.__r_oid__ = oid
    return obj

def get_deleted(data):
    try:
        return [
            (dtpname, oid)
            for dtpname in data for oid in data[dtpname]
            if data[dtpname][oid]["types"][dtpname] == Event.Delete]
    except Exception:
        raise

class instrument_func(object):
    instrument = False
    def __init__(self, name):
        self.name = name
        self.file = None

    def __call__(self, func):
        def replacement(obj, *args, **kwargs):
            start = time.perf_counter() * 1000
            start_p = time.process_time() * 1000
            try:
                ret_val = func(obj, *args, **kwargs)
            except Exception:
                raise
            finally:
                end = time.perf_counter() * 1000
                end_p = time.process_time() * 1000
                if obj.instrument_record is not None:
                    self.record_instrumentation(
                        obj, self.name, time.time(), end - start, end_p - start_p)
            return ret_val
        return replacement

    def record_instrumentation(self, obj, *args):
        obj.instrument_record.put("\t".join(map(str, args)) + "\n")

def dim_diff(dtpname, original, new, original_obj, new_obj, dimmap):
    # return dims that are in new but not in/different in the original.
    change = {"dims": dict(), "types": dict()}
    do_not_touch = set()
    for dim in new["dims"]:
        if dimmap[dim].custom_diff:
            new["dims"][dim] = {
                "type": DiffType.MOD if original_obj else DiffType.NEW,
                "value": dimmap[dim].custom_diff.diff_objs(
                    new_obj, original_obj)
            } if original_obj and new_obj else {
                
            }
            do_not_touch.add(dim)
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
