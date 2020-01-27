from uuid import uuid4
from spacetime.utils.enums import Event

class Diff(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version = str(uuid4())

    def add(self, dtype, objs):
        dtpname = dtype.__r_meta__.name
        tpchange = self.setdefault(dtpname, dict())
        for obj in objs:
            oid, dim_map = extract(dtype, obj)
            tpchange[oid] = {
                "dims": dim_map,
                "types": {
                    dtpname: Event.New
                }
            }

    def write_dimension(self, dtype, oid, dim, value):
        dtpname = dtype.__r_meta__.name
        # No need for locks because once this operation is set
        # which is either the old list, and can be added.
        # or is new
        tpchange = self.setdefault(dtpname, dict())
        if oid in tpchange and tpchange[oid]["types"][dtpname] is Event.Delete:
            # If the object was marked for delete, modifications shouldnt work.
            return

        change = tpchange.setdefault(
            oid, {"dims": dict(), "types": dict()})
        change["dims"][dim] = value
        if dtpname not in change["types"]:
           change["types"][dtpname] = Event.Modification 
    
    def get_write_record(self, dtype, oid, dim):
        dtpname = dtype.__r_meta__.name
        tpchange = self.setdefault(dtpname, dict())
        if oid in tpchange and tpchange[oid]["types"][dtpname] is Event.Delete:
            # If the object was marked for delete, modifications shouldnt work.
            return
        change = tpchange.setdefault(
            oid, {"dims": dict(), "types": dict()})
        if dtpname not in change["types"]:
           change["types"][dtpname] = Event.Modification 
        return change["dims"][dim]
        

    def has_new_value(self, dtype, oid, dim):
        dtpname = dtype.__r_meta__.name
        return (
            dtpname in self 
            and oid in self[dtpname] 
            and dim in self[dtpname][oid]["dims"])

    def read_dimension(self, dtype, oid, dim):
        dtpname = dtype.__r_meta__.name
        return self[dtpname][oid]["dims"][dim]
            
    def exists(self, dtype, oid):
        dtpname = dtype.__r_meta__.name
        return (
            dtpname in self
            and oid in self[dtpname]
            and dtpname in self[dtpname][oid]["types"]
            and self[dtpname][oid]["types"][dtpname] is not Event.Delete)

    def read_oids(self, dtype):
        dtpname = dtype.__r_meta__.name
        oids = set()
        deletes = set()
        if dtpname in self:
            for oid in self[dtpname]:
                if dtpname in self[dtpname][oid]["types"]:
                   if self[dtpname][oid]["types"][dtpname] is not Event.Delete:
                       oids.add(oid)
                   else:
                       deletes.add(oid)
        return oids, deletes

    def not_marked_for_delete(self, dtype, oid):
        dtpname = dtype.__r_meta__.name
        return not (
            dtpname in self 
            and oid in self[dtpname] 
            and dtpname in self[dtpname][oid]["types"] 
            and self[dtpname][oid]["types"][dtpname] is Event.Delete)

    def delete(self, dtype, oid, in_prev):
        dtpname = dtype.__r_meta__.name
        is_staged = dtpname in self and oid in self[dtpname]
        if is_staged:
            del self[dtpname][oid]
        if in_prev:
            self.setdefault(dtpname, dict())[oid] = {
                "types": {dtpname: Event.Delete}}


def extract(dtype, obj):
    return obj.__r_oid__, dtype.__r_table__[obj.__r_oid__]