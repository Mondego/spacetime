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

    def _take_control(self, objs):
        for obj in objs:
            obj.__r_df__ = self

    def _release_control(self, dtype, objs):
        for obj in objs:
            dtype.__r_table__.take_control(obj)
            obj.__r_df__ = None
    
    def _exists(self, dtype, oid):
        dtpname = dtype.__r_meta__.name
        return dtpname in self.membership and oid in self.membership[dtpname]

    def _set_membership(self, package):
        membership = dict()
        for dtpname, tp_changes in package.items():
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
            self.data = utils.merge_state_delta(self.data, data, delete_it=True)
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
        if (self.diff.not_marked_for_delete(dtype, oid) 
                and (self.diff.exists(dtype, oid) 
                        or self._exists(dtype, oid))):
            objs = utils.make_objs(dtype, [oid])
            self._take_control(objs)
            return objs[0]
        return None

    def read_all(self, dtype):
        staged_oids, fresh_deletes = self.diff.read_oids(dtype)
        sm_oids = (
            self.membership.setdefault(
                dtype.__r_meta__.name, set()) - fresh_deletes)
        objs = utils.make_objs(dtype, staged_oids.union(sm_oids))
        self._take_control(objs)
        return objs

    def add_one(self, dtype, obj):
        self.diff.add(dtype, [obj])
        self._take_control([obj])

    def add_many(self, dtype, objs):
        self.diff.add(dtype, objs)
        self._take_control(objs)

    def delete_one(self, dtype, obj):
        self._release_control(dtype, [obj])
        oid = obj.__r_oid__
        in_prev = self._exists(dtype, oid)
        self.diff.delete(dtype, oid, in_prev)
    
    def delete_all(self, dtype):
        objs = self.read_all(dtype)
        self._release_control(dtype, objs)
        for obj in objs:
            oid = obj.__r_oid__
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
