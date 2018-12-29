from spacetime.managers.diff import Diff
from spacetime.utils.enums import Event
import spacetime.utils.utils as utils

class ManagedHeap(object):
    def __init__(self, types):
        self.types = types
        self.type_map = {
            tp.__r_meta__.name: tp
            for tp in types
        }
        self.data = dict()
        self.diff = Diff()
        self.version = "ROOT"
        self.membership = dict()
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
        return dtpname in self.membership and oid in self.membership[dtpname]

    def _set_membership(self, package):
        membership = dict()
        for _, tp_changes in package.items():
            for oid, obj_changes in tp_changes.items():
                for tpname, event in obj_changes["types"].items():
                    if tpname not in membership:
                        membership[tpname] = (
                            set() 
                            if tpname not in self.membership else
                            self.membership[tpname])
                    if event is Event.Delete:
                        # Oid has to be removed from the new version
                        if oid not in membership[tpname]:
                            raise RuntimeError(
                                "Got an delete without having the object.")
                        membership[tpname].remove(oid)
                    elif event is Event.New:
                        if oid in membership[tpname]:
                            raise RuntimeError(
                                "Got a new object but already have the object.")
                        membership[tpname].add(oid)
                    else:
                        if oid not in membership[tpname]:
                            raise RuntimeError(
                                "Got a modification for an "
                                "object that does not exist.")
        return membership


    def receive_data(self, data, version):
        if data:
            deleted_oids = utils.get_deleted(data)
            for tpname, oid in deleted_oids:
                self._release_control(self.type_map[tpname], oid)
            self.data = utils.merge_state_delta(
                self.data, data, delete_it=True)
            self.membership = self._set_membership(data)
        self.version = version
        return True

    def retreive_data(self):
        if len(self.diff) > 0:
            return self.diff, [self.version, self.diff.version]
        return dict(), [self.version, self.version]

    def data_sent_confirmed(self):
        self.data = utils.merge_state_delta(
            self.data, self.diff, delete_it=True)
        self.membership = self._set_membership(self.diff)
        self.version = self.diff.version
        self.diff = Diff()

    def read_one(self, dtype, oid):
        dtpname = dtype.__r_meta__.name
        if (self.diff.not_marked_for_delete(dtype, oid) 
                and (self.diff.exists(dtype, oid) 
                        or self._exists(dtype, oid))):
            if oid in self.tracked_objs[dtpname]:
                return self.tracked_objs[dtpname][oid]
            return self._take_control(dtpname, utils.make_obj(dtype, oid))
        return None

    def read_all(self, dtype):
        dtpname = dtype.__r_meta__.name
        staged_oids, fresh_deletes = self.diff.read_oids(dtype)
        sm_oids = (
            self.membership.setdefault(
                dtype.__r_meta__.name, set()) - fresh_deletes)
        return [
            (self.tracked_objs[dtpname][oid]
             if oid in self.tracked_objs[dtpname] else
             self._take_control(dtpname, utils.make_obj(dtype, oid)))
            for oid in staged_oids.union(sm_oids)
        ]

    def add_one(self, dtype, obj):
        dtpname = dtype.__r_meta__.name
        oid = obj.__r_oid__
        if dtpname in self.data and oid in self.data[dtpname]:
            raise ValueError(
                "Obj ({0}, {1}) already exists in dataframe.".format(
                    dtpname, oid))
        self.diff.add(dtype, [obj])
        self._take_control(dtpname, obj)

    def add_many(self, dtype, objs):
        dtpname = dtype.__r_meta__.name
        for obj in objs:
            oid = obj.__r_oid__
            if dtpname in self.data and oid in self.data[dtpname]:
                raise ValueError(
                    "Obj ({0}, {1}) already exists in dataframe.".format(
                        dtpname, oid))
            
        self.diff.add(dtype, objs)
        for obj in objs:
            self._take_control(dtpname, obj)

    def delete_one(self, dtype, obj):
        oid = obj.__r_oid__
        self._release_control(dtype, oid)
        assert (obj.__r_df__ is None)
        in_prev = self._exists(dtype, oid)
        self.diff.delete(dtype, oid, in_prev)
    
    def delete_all(self, dtype):
        objs = self.read_all(dtype)
        for obj in objs:
            oid = obj.__r_oid__
            self._release_control(dtype, oid)
            assert (obj.__r_df__ is None)
            in_prev = self._exists(dtype, oid)
            self.diff.delete(dtype, oid, in_prev)

    def read_dimension(self, dtype, oid, dimname):
        if self.diff.has_new_value(dtype, oid, dimname):
            return self.diff.read_dimension(dtype, oid, dimname)
        dtpname = dtype.__r_meta__.name
        if (dtpname in self.data 
                and oid in self.data[dtpname] 
                and "dims" in self.data[dtpname][oid] 
                and dimname in self.data[dtpname][oid]["dims"]):
            return self.data[dtpname][oid]["dims"][dimname]
        return None

    def write_dimension(self, dtype, oid, dimname, value):
        self.diff.write_dimension(dtype, oid, dimname, value)

    def reset_primary_key(self, dtype, oid, dim, value):
        pass
