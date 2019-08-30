from multiprocessing import RLock
from spacetime.managers.diff import Diff
from spacetime.utils.enums import Event
import spacetime.utils.utils as utils
from rtypes.utils.enums import Rtype

class ManagedHeap(object):
    def __init__(self, types):
        self.types = types
        self.diff_access_lock = RLock()
        self.type_map = {
            tp.__r_meta__.name: tp
            for tp in types
        }
        self.data = {
            tp.__r_meta__.name: dict()
            for tp in types
        }
        self.diff = Diff()
        self.version = None
        self.version = "ROOT"

        self.tracked_objs = {
            tp.__r_meta__.name: dict()
            for tp in types
        }

        self.pending_commit = Diff()

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

    def _get_next_version(self):
        return [self.version, self.diff.version]

    def _extract_new_version(self, version):
        return version[1]

    def _add_dependent_types(self, dtype, objs):
        # Assumption is that these objects cannot be in conflict with existing.
        dtpmeta = dtype.__r_meta__
        dependent_list = list()
        dependent_list.extend(
            (dmeta, objs) for dmeta in dtpmeta.type_graph.dependents[dtpmeta])
        while dependent_list:
            dep_meta, base_objs = dependent_list.pop()
            oids = set(
                base_obj.__r_oid__
                for base_obj in base_objs
                if dep_meta.valid_obj(base_obj))
            self.diff.add_dep_objs(dtpmeta, dep_meta, oids)
            dependent_list.extend(dep_meta.type_graph.dependents[dep_meta])

    def _mod_dependent_obj(self, dtype, oid, dimname, value):
        dtpmeta = dtype.__r_meta__
        dependent_list = list()
        dependent_list.extend(dtpmeta.type_graph.dependents[dtpmeta])
        base_obj = self.read_one(dtype, oid)
        while dependent_list:
            dep_meta = dependent_list.pop()
            if dep_meta.valid_obj(base_obj):
                if (dep_meta.name in self.data 
                        and oid in self.data[dep_meta.name]):
                    self.diff.mod_dep_obj(
                        dtpmeta, dep_meta, oid, dimname, value)
                else:
                    self.diff.add_dep_objs(dtpmeta, dep_meta, [oid])
            elif (dep_meta.name in self.data
                  and oid in self.data[dep_meta.name]):
                # if object is currently present.
                self.diff.del_dep_obj(dtpmeta, dep_meta, [oid])
            dependent_list.extend(dep_meta.type_graph.dependents[dep_meta])

    def _del_dependent_obj(self, dtype, oid):
        dtpmeta = dtype.__r_meta__
        dependent_list = list()
        dependent_list.extend(dtpmeta.type_graph.dependents[dtpmeta])
        while dependent_list:
            dep_meta = dependent_list.pop()
            if dep_meta.name in self.data and oid in self.data[dep_meta.name]:
                del self.data[dep_meta.name][oid]
                self.diff.del_dep_obj(dtpmeta, dep_meta, [oid])
            dependent_list.extend(dep_meta.type_graph.dependents[dep_meta])

    def _del_all_dep_objs(self, dtype):
        dtpmeta = dtype.__r_meta__
        dependent_list = list()
        dependent_list.extend(dtpmeta.type_graph.dependents[dtpmeta])
        while dependent_list:
            dep_meta = dependent_list.pop()
            for oid in self.data[dtpmeta.name]:
                if (dep_meta.name in self.data
                        and oid in self.data[dep_meta.name]):
                    del self.data[dep_meta.name][oid]
                    self.diff.del_dep_obj(dtpmeta, dep_meta, [oid])
            dependent_list.extend(dep_meta.type_graph.dependents[dep_meta])

    def receive_data(self, data, version):
        if data:
            deleted_oids = utils.get_deleted(data)
            for tpname, oid in deleted_oids:
                self._release_control(self.type_map[tpname], oid)
            self.data = utils.merge_state_delta(
                self.data, data, delete_it=True)
        self.version = self._extract_new_version(version)
        return True

    def retreive_data(self):
        if self.diff:
            final_diff = Diff(
                utils.merge_state_delta(self.pending_commit, self.diff))
            final_diff.version = self.diff.version
            versions = self._get_next_version()
            self.pending_commit = final_diff
            self.diff = Diff()
            return final_diff, versions
        return dict(), None

    def data_sent_confirmed(self, versions):
        if versions is None:
            return
        self.pending_commit = Diff()
        self.version = versions[1]
        self.diff = Diff()

    def read_one(self, dtype, oid):
        dtpname = dtype.__r_meta__.name
        if self._exists(dtype, oid):
            if oid in self.tracked_objs[dtpname]:
                return self.tracked_objs[dtpname][oid]
            return self._take_control(dtpname, utils.make_obj(dtype, oid))
        return None

    def read_all(self, dtype):
        dtpname = dtype.__r_meta__.name
        if dtpname not in self.data:
            return list()
        return [
            (self.tracked_objs[dtpname][oid]
             if oid in self.tracked_objs[dtpname] else
             self._take_control(dtpname, utils.make_obj(dtype, oid)))
            for oid in self.data[dtpname]
        ]

    def add_one(self, dtype, obj):
        dtpmeta = dtype.__r_meta__
        dtpname = dtpmeta.name
        if dtpmeta.rtype is not Rtype.SET:
            raise TypeError("Cannot add new object that is not a pcc_set")

        oid = obj.__r_oid__
        if dtpname in self.data and oid in self.data[dtpname]:
            raise ValueError(
                "Obj ({0}, {1}) already exists in dataframe.".format(
                    dtpname, oid))
        dim_map = dtype.__r_table__[obj.__r_oid__]
        self.data[dtpname][oid] = {
            "dims": dim_map, "types": {dtpname: Event.New}}
        self.diff.add(dtype, [obj])
        self._take_control(dtpname, obj)
        self._add_dependent_types(dtype, [obj])

    def add_many(self, dtype, objs):
        dtpname = dtype.__r_meta__.name
        for obj in objs:
            oid = obj.__r_oid__
            if dtpname in self.data and oid in self.data[dtpname]:
                raise ValueError(
                    "Obj ({0}, {1}) already exists in dataframe.".format(
                        dtpname, oid))
            dim_map = dtype.__r_table__[obj.__r_oid__]
            self.data[dtpname][oid] = {
                "dims": dim_map, "types": {dtpname: Event.New}}
        self.diff.add(dtype, objs)
        for obj in objs:
            self._take_control(dtpname, obj)
        self._add_dependent_types(dtype, objs)

    def delete_one(self, dtype, obj):
        oid = obj.__r_oid__
        dtpname = dtype.__r_meta__.name
        self._release_control(dtype, oid)
        assert obj.__r_df__ is None
        in_prev = self._exists(dtype, oid)
        self.diff.delete(dtype, oid, in_prev)
        self._del_dependent_obj(dtype, oid)
        del self.data[dtpname][oid]

    def delete_all(self, dtype):
        objs = self.read_all(dtype)
        dtpname = dtype.__r_meta__.name
        for obj in objs:
            oid = obj.__r_oid__
            self._release_control(dtype, oid)
            assert obj.__r_df__ is None
            in_prev = self._exists(dtype, oid)
            self.diff.delete(dtype, oid, in_prev)
        self._del_all_dep_objs(dtype)
        for obj in objs:
            oid = obj.__r_oid__
            del self.data[dtpname][oid]

    def read_dimension(self, dtype, oid, dimname):
        # if self.diff.has_new_value(dtype, oid, dimname):
        #     return self.diff.read_dimension(dtype, oid, dimname)
        dtpname = dtype.__r_meta__.name
        if (dtpname in self.data
                and oid in self.data[dtpname] 
                and "dims" in self.data[dtpname][oid] 
                and dimname in self.data[dtpname][oid]["dims"]):
            return self.data[dtpname][oid]["dims"][dimname]
        return None

    def write_dimension(self, dtype, oid, dimname, value):
        self.diff.write_dimension(dtype, oid, dimname, value)
        dtpname = dtype.__r_meta__.name
        self.data[dtpname][oid]["dims"][dimname] = value
        self._mod_dependent_obj(dtype, oid, dimname, value)

    def reset_primary_key(self, dtype, oid, dim, value):
        pass
