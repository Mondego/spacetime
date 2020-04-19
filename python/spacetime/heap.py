from threading import RLock

from rtypes.utils.enums import DiffType
from rtypes.utils.converter import unconvert

from spacetime.utils.diff import Diff
from spacetime.utils.enums import Event
import spacetime.utils.utils as utils
from spacetime.utils.rwlock import RWLockFair as RWLock


class Heap(object):
    def __init__(self, name, types, version_graph):
        self.types = types
        self.access_lock = RWLock(lock_factory=RLock)
        self.type_map = {
            tp.__r_meta__.name: tp
            for tp in types
        }
        self.name = name
        self.data = {
            tp.__r_meta__.name: dict()
            for tp in types
        }

        self.diff = Diff()
        self.version = "ROOT"
        self.version_graph = version_graph
        self.tp_to_dim = {
            tp.__r_meta__.name: tp.__r_meta__.dimmap
            for tp in types
        }

        self.tracked_objs = {
            tp.__r_meta__.name: dict()
            for tp in types
        }

    def _take_control(self, tpname, obj):
        obj.__r_df__ = self
        self.tracked_objs[tpname][obj.__r_oid__] = obj
        return obj

    def _release_control(self, dtype, oid):
        dtpname = dtype.__r_meta__.name
        if dtpname in self.tracked_objs and oid in self.tracked_objs[dtpname]:
            dtype.__r_table__.object_table[oid] = {
                dimname: self.read_dimension(dtype, oid, dimname)
                for dimname in dtype.__r_meta__.dimnames
            }
            obj = self.tracked_objs[dtpname][oid]
            obj.__r_df__ = None
            del self.tracked_objs[dtpname][oid]

    def _exists(self, dtype, oid):
        dtpname = dtype.__r_meta__.name
        return dtpname in self.data and oid in self.data[dtpname]

    def _receive_data(self, delta, version):
        with self.access_lock.gen_wlock():
            if delta:
                self._apply_state_delta(delta)
            self.version = version
            return True

    def _apply_state_delta(self, delta):
        for tpname, tp_delta in delta.items():
            for oid, obj_delta in tp_delta.items():
                if obj_delta["types"][tpname] == Event.Delete:
                    if tpname in self.data and oid in self.data[tpname]:
                        self._release_control(self.type_map[tpname], oid)
                        del self.data[tpname][oid]
                        if not self.data[tpname]:
                            del self.data[tpname]
                    continue
                dimdata = self.data.setdefault(
                    tpname, dict()).setdefault(oid, dict())
                dimmeta = self.tp_to_dim[tpname]
                for dim, value in obj_delta["dims"].items():
                    if dimmeta[dim].custom_diff:
                        if value["type"] == DiffType.NEW:
                            dimdata[dim] = dimmeta[dim].custom_diff.new_func(
                                dimdata.setdefault(dim, None), value["value"])
                        else:
                            dimdata[dim] = dimmeta[dim].custom_diff.apply_func(
                                dimdata[dim], value["value"])
                    else:
                        dimdata[dim] = unconvert(value, dimmeta[dim].dim_type)

    def checkout(self, wait=False, timeout=0):
        if wait:
            self.version_graph.wait_for_change({self.version}, timeout=timeout)
        self.version_graph.logger.info(f"CHECKOUT: from {self.version}")
        edges = list(self.version_graph.get_edges_to_head(
            self.name, self.version))
        # self.version_graph.logger.info(
        #     f"CHECKOUT: received:: "
        #     f"{[(edge.from_v.vid, edge.to_v.vid) for edge in edges]}")
        for edge in edges:
            self._receive_data(edge.delta, edge.to_v.vid)
        self.version_graph.confirm_fetch(self.name, self.version)

    def commit(self):
        self.version_graph.logger.info(
            f"Commit: {(self.version, self.diff.version)}")
        self.version_graph.put(
            self.name,
            self.diff.version,
            [(self.version, self.diff.version, self.diff, self.diff.version)])
        self.version = self.diff.version
        self.diff = Diff()

    def read_one(self, dtype, oid):
        dtpname = dtype.__r_meta__.name
        with self.access_lock.gen_rlock():
            if self._exists(dtype, oid):
                if oid in self.tracked_objs[dtpname]:
                    return self.tracked_objs[dtpname][oid]
                return self._take_control(dtpname, utils.make_obj(dtype, oid))
            return None

    def read_all(self, dtype):
        dtpname = dtype.__r_meta__.name
        with self.access_lock.gen_rlock():
            if dtpname not in self.data:
                return list()
            return [
                (self.tracked_objs[dtpname][oid]
                 if oid in self.tracked_objs[dtpname] else
                 self._take_control(dtpname, utils.make_obj(dtype, oid)))
                for oid in self.data[dtpname]
            ]

    def add(self, dtype, objs):
        dtpname = dtype.__r_meta__.name
        with self.access_lock.gen_wlock():
            for obj in objs:
                oid = obj.__r_oid__
                if dtpname in self.data and oid in self.data[dtpname]:
                    raise ValueError(
                        "Obj ({0}, {1}) already exists in dataframe.".format(
                            dtpname, oid))
                dim_map = dtype.__r_table__[obj.__r_oid__]
                self.data[dtpname][oid] = dim_map
            self.diff.add(dtype, objs, self.tp_to_dim[dtpname])
            for obj in objs:
                self._take_control(dtpname, obj)

    def delete(self, dtype, objs):
        with self.access_lock.gen_wlock():
            for obj in objs:
                oid = obj.__r_oid__
                dtpname = dtype.__r_meta__.name
                self._release_control(dtype, oid)
                assert obj.__r_df__ is None
                in_prev = self._exists(dtype, oid)
                self.diff.delete(dtype, oid, in_prev)
                del self.data[dtpname][oid]

    def read_dimension(self, dtype, oid, dimname):
        # if self.diff.has_new_value(dtype, oid, dimname):
        #     return self.diff.read_dimension(dtype, oid, dimname)
        dtpname = dtype.__r_meta__.name
        with self.access_lock.gen_rlock():
            has_value = (
                dtpname in self.data
                and oid in self.data[dtpname] 
                and dimname in self.data[dtpname][oid])
            if has_value:
                return self.data[dtpname][oid][dimname]
            return None

    def write_dimension(self, dtype, oid, dimname, value):
        with self.access_lock.gen_wlock():
            dtpname = dtype.__r_meta__.name
            self.diff.write_dimension(
                dtype, oid, dimname, value, self.tp_to_dim[dtpname])
            self.data[dtpname][oid][dimname] = value
            return self.diff.version

    def reset_primary_key(self, dtype, oid, dim, value):
        pass
