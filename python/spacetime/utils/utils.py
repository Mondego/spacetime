import logging
import os

from spacetime.utils.enums import Event
from copy import deepcopy

class _container(object):
    pass


def get_logger(name):
    if not os.path.exists("Logs"):
        os.makedirs("Logs")
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(os.path.join("Logs", name + ".log"))
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def merge_state_delta(old_change, newer_change, delete_it=False):
    merged = dict()
    if old_change == dict():
        return deepcopy(newer_change)
    for dtpname in old_change:
        if dtpname not in newer_change:
            merged[dtpname] = deepcopy(old_change[dtpname])
        else:
            merged[dtpname] = merge_objectlist_deltas(
                dtpname, old_change[dtpname], newer_change[dtpname],
                delete_it=delete_it)
    for dtpname in newer_change:
        if dtpname not in old_change:
            merged[dtpname] = deepcopy(newer_change[dtpname])
    return merged

def get_merge_objectlist_delta(dtpname):
    def curry_func(old_change, new_change, delete_it=False):
        return merge_objectlist_deltas(
            dtpname, old_change, new_change, delete_it=False)
    return curry_func

def get_merge_object_delta(dtpname):
    def curry_func(old_change, new_change):
        return merge_object_delta(
            dtpname, old_change, new_change)
    return curry_func

def merge_objectlist_deltas(dtpname, old_change, new_change, delete_it=False):
    merged = dict()
    for oid in old_change:
        if oid not in new_change:
            merged[oid] = deepcopy(old_change[oid])
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
            merged[oid] = merge_object_delta(
                dtpname, old_change[oid], new_change[oid])
    for oid in new_change:
        if oid not in old_change:
            if delete_it and new_change[oid]["types"][dtpname] is Event.Delete:
                continue
            merged[oid] = deepcopy(new_change[oid])
    return merged

def merge_object_delta(dtpname, old_change, new_change):
    if not old_change:
        return deepcopy(new_change)
    if old_change["types"][dtpname] is Event.New and new_change["types"][dtpname] is Event.Delete:
        return None
    if new_change["types"][dtpname] is Event.Delete:
        return deepcopy(new_change)
    if (old_change["types"][dtpname] is Event.Delete
            and new_change["types"][dtpname] is Event.New):
        return deepcopy(new_change)
    if new_change["types"][dtpname] is not Event.Modification:
        raise RuntimeError(
            "Not sure why the new change does not have modification.")

    dim_change = deepcopy(old_change["dims"])
    dim_change.update(new_change["dims"])
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
    obj = _container()
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
        print(data)
        raise