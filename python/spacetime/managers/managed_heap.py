from multiprocessing import RLock
from spacetime.managers.diff import Diff
from spacetime.utils.enums import Event, VersionBy
import spacetime.utils.utils as utils

class ManagedHeap(object):
    def __init__(self, types, version_by):
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
        if version_by == VersionBy.FULLSTATE:
            self.version = "ROOT"
        elif version_by == VersionBy.TYPE:
            self.version = {
                tp.__r_meta__.name: "ROOT"
                for tp in types
            }
        elif version_by == VersionBy.OBJECT_NOSTORE:
            self.version = {
                tp.__r_meta__.name: dict()
                for tp in types
            }
        else:
            raise NotImplementedError()

        self.tracked_objs = {
            tp.__r_meta__.name: dict()
            for tp in types
        }

        self.pending_commit = Diff()
        self.version_by = version_by

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
        if self.version_by == VersionBy.FULLSTATE:
            return [self.version, self.diff.version]
        elif self.version_by == VersionBy.TYPE:
            return {
                tpname: [self.version[tpname], self.diff.version]
                for tpname in self.version
                if tpname in self.diff
            }
        elif self.version_by == VersionBy.OBJECT_NOSTORE:
            version = dict()
            for tpname in self.diff:
                if not self.diff[tpname]:
                    continue
                version[tpname] = dict()
                for oid in self.diff[tpname]:
                    if tpname in self.version and oid in self.version[tpname]:
                        start = self.version[tpname][oid]
                    else:
                        start = "ROOT"
                    if self.diff[tpname][oid]["types"][tpname] == Event.Delete:
                        end = "END"
                    else:
                        end = self.diff.version
                    version[tpname][oid] = [start, end]
            return version
        else:
            raise NotImplementedError()

    def _extract_new_version(self, version):
        if self.version_by == VersionBy.FULLSTATE:
            return version[1]
        elif self.version_by == VersionBy.TYPE:
            extracted_v = {
                tpname: self.version[tpname]
                for tpname in self.version
                if tpname not in version
            }
            extracted_v.update({
                tpname: version[tpname][1]
                for tpname in version
            })
            return extracted_v
        elif self.version_by == VersionBy.OBJECT_NOSTORE:
            final_v = dict(self.version)
            for tpname in version:
                if not version[tpname]:
                    continue
                final_v.setdefault(tpname, dict())
                for oid in version[tpname]:
                    if version[tpname][oid][1] != "END":
                        final_v[tpname][oid] = version[tpname][oid][1]
                    elif oid in final_v[tpname]:
                        del final_v[tpname][oid]
            return final_v
        else:
            raise NotImplementedError()

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
        if len(self.diff) > 0:
            final_diff = Diff(utils.merge_state_delta(self.pending_commit, self.diff))
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
        if self.version_by == VersionBy.FULLSTATE:
            self.version = versions[1]
        elif self.version_by == VersionBy.TYPE:
            for tpname in versions:
                self.version[tpname] = versions[tpname][1]
        elif self.version_by == VersionBy.OBJECT_NOSTORE:
            for tpname in versions:
                for oid in versions[tpname]:
                    if versions[tpname][oid][1] == "END":
                        if oid in self.version[tpname]:
                            del self.version[tpname][oid]
                        continue
                    self.version[tpname][oid] = versions[tpname][oid][1]
        else:
            raise NotImplementedError()
        
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
        dtpname = dtype.__r_meta__.name
        oid = obj.__r_oid__
        if dtpname in self.data and oid in self.data[dtpname]:
            raise ValueError(
                "Obj ({0}, {1}) already exists in dataframe.".format(
                    dtpname, oid))
        dim_map = dtype.__r_table__[obj.__r_oid__]
        self.data[dtpname][oid] =  {"dims": dim_map, "types": {dtpname: Event.New}}
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
            dim_map = dtype.__r_table__[obj.__r_oid__]
            self.data[dtpname][oid] =  {"dims": dim_map, "types": {dtpname: Event.New}}
        self.diff.add(dtype, objs)
        
        for obj in objs:
            self._take_control(dtpname, obj)

    def delete_one(self, dtype, obj):
        oid = obj.__r_oid__
        dtpname = dtype.__r_meta__.name
        self._release_control(dtype, oid)
        assert (obj.__r_df__ is None)
        in_prev = self._exists(dtype, oid)
        self.diff.delete(dtype, oid, in_prev)
        del self.data[dtpname][oid]
    
    def delete_all(self, dtype):
        objs = self.read_all(dtype)
        dtpname = dtype.__r_meta__.name
        for obj in objs:
            oid = obj.__r_oid__
            self._release_control(dtype, oid)
            assert (obj.__r_df__ is None)
            in_prev = self._exists(dtype, oid)
            self.diff.delete(dtype, oid, in_prev)
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


    def reset_primary_key(self, dtype, oid, dim, value):
        pass
