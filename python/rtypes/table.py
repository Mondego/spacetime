from uuid import uuid4
from rtypes.utils.converter import convert, unconvert


class RtypesTable(object):
    def __init__(self, cls):
        # Oid -> {dimname -> value}
        self.object_table = dict()
        # oid -> dataframe
        self.obj_type = cls
        self.store_as_temp = dict()

    def __getitem__(self, oid):
        return self.object_table[oid]

    def set(self, oid, dimname, dim_obj, value):
        if not isinstance(value, dim_obj.dim_type):
            # No ducktyping :D
            raise TypeError(
                "{0} is not of type {1}".format(
                    repr(value), dim_obj.dim_type.__name__))
        if oid is None:
            # setting up a random uuid as oid
            oid = str(uuid4())
            self.object_table[oid] = dict()

        converted = convert(dim_obj.dim_type, value)
        if oid in self.store_as_temp:
            self.store_as_temp[oid][dimname] = converted
        else:
            # Write to local state map.
            self.object_table[oid][dimname] = convert(dim_obj.dim_type, value)
        return oid

    def set_primarykey(self, oid, dimname, dim_obj, value):
        if not isinstance(value, dim_obj.dim_type):
            # No ducktyping :D
            raise TypeError(
                "{0} is not of type {1}".format(
                    repr(value), dim_obj.dim_type.__name__))

        if oid is None and value is None:
            raise RuntimeError("Primary key cannot be None.")
            
        if oid is not None:
            # Oid is being reset, but the object is not controlled by the dataframe.
            self.object_table[oid][dimname] = convert(dim_obj.dim_type, value)
            self.object_table[value] = self.object_table[oid]
            del self.object_table[oid]
        else:
            # oid is being assigned for the first time.
            self.object_table[value] = dict()
            self.object_table[value][dimname] = convert(dim_obj.dim_type, value)
        return value


    def get(self, oid, dimname, dim_obj):
        if oid in self.store_as_temp and dimname in self.store_as_temp[oid]:
            return unconvert(self.store_as_temp[oid][dimname], dim_obj.dim_type)
        if oid not in self.object_table and dimname not in self.object_table[oid]:
            # Value has not been assigned.
            raise AttributeError("{0} has not been assigned a value.".format(dimname))
        # return value from local table.
        
        return unconvert(self.object_table[oid][dimname], dim_obj.dim_type)

    def delete_obj(self, oid):
        if oid in self.object_table:
            del self.object_table[oid]

    def take_control(self, obj):
        oid = obj.__r_oid__
        self.object_table[oid] = dict()
        for dimname, dim_obj in self.obj_type.__r_meta__.dimmap.items():
            if hasattr(obj, dimname):
                self.object_table[oid][dimname] = convert(dim_obj.dim_type, getattr(obj, dimname))
