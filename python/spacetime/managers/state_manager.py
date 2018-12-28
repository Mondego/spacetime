from uuid import uuid4
import spacetime.utils.utils as utils
from spacetime.utils.enums import Event

class _container(object):
    pass

class Record(object):
    def __init__(self, vid, prev_id, delta):
        self.vid = vid
        self.prev_id = prev_id
        self.delta = delta
        self.next_id = None


class StateManager(object):
    @property
    def head(self):
        print ("Reading head as ", self._head)
        return self._head

    @head.setter
    def head(self, value):
        print ("Setting head as ", value)
        self._head = value
    @staticmethod
    def convert_obj_to_record(self, dtype, obj):
        pass

    def __init__(self, appname, type_map):
        self.forked = False
        self.forked_version = None
        self.logger = utils.get_logger("%s_StateManager" % appname)
        self.state = dict()
        
        self.app_to_state = dict()
        self.state_to_app = dict()

        self.version_members = dict()

        self.alt_path = dict()

        self.head = None
        self.tail = None

        self.type_map = type_map

    def get_latest_version(self):
        return self.head

    def fork(self, appname):
        version = self.get_latest_version()
        self.maintain(appname, version)
        return version
    
    def read_oids(self, version, dtype):
        dtpname = dtype.__r_meta__.name
        if version not in self.version_members:
            raise RuntimeError("Unknown version number. %s" % version)
        if dtpname not in self.version_members[version]:
            return set()
        return self.version_members[version][dtpname]

    def exists(self, version, dtype, oid):
        dtpname = dtype.__r_meta__.name
        if version not in self.version_members:
            raise RuntimeError("Unknown version number. %s" % version)
        members = self.version_members[version]
        return dtpname in members and oid in members[dtpname]

    def read_dimension(self, version, dtype, oid, dim):
        if version is None:
            return None
        if version not in self.state:
            raise RuntimeError("Searching for a version that does not exist. %s" % version)
        dtpname = dtype.__r_meta__.name
        delta = self.state[version].delta
        if (dtpname in delta 
                and oid in delta[dtpname]
                and dim in delta[dtpname][oid]["dims"]):
            return delta[dtpname][oid]["dims"][dim]
        else:
            return self.read_dimension(
                self.state[version].prev_id, dtype, oid, dim)

    def data_sent_confirmed(self, app, version):
        self.maintain(app, version[1])

    def get_last_known_version(self, parent):
        return self.app_to_state[parent] if parent in self.app_to_state else None

    def get_diff_from(self, start, end):
        merged = dict()
        current = start
        if current in self.alt_path:
            merged = self.alt_path[current].delta
            current = self.alt_path[current].next_id

        while current is not None:
            merged = self.merge_state_delta(merged, self.state[current].delta)
            if current is end:
                break
            current = self.state[current].next_id
        return merged

    def get_diff_since(self, version):
        head = self.head
        if version is None:
            return self.get_diff_from(self.tail, head), [version, head]
        if version in self.alt_path:
            return self.get_diff_from(version, head), [version, head]
        return (
            self.get_diff_from(self.state[version].next_id, head),
            [version, head])
        

    def get_diff_since_last_sync(self, appname):
        if appname in self.app_to_state:
            return self.get_diff_since(self.app_to_state[appname])
        else:
            return self.get_diff_since(None)

    def set_membership(self, prev_id, package):
        membership = dict()
        for dtpname, tp_changes in package.items():
            for oid, obj_changes in tp_changes.items():
                for tpname, event in obj_changes["types"].items():
                    if tpname not in membership:
                        membership[tpname] = (
                            set() 
                            if prev_id is None or tpname not in self.version_members[prev_id] else
                            self.version_members[prev_id][tpname])
                    if event is Event.Delete:
                        # Oid has to be removed from the new version
                        if oid not in membership[tpname]:
                            raise RuntimeError("Got an delete without having the object.")
                        membership[tpname].remove(oid)
                    elif event is Event.New:
                        if oid in membership[tpname]:
                            raise RuntimeError("Got a new object but already have the object.")
                        membership[tpname].add(oid)
                    else:
                        if oid not in membership[tpname]:
                            raise RuntimeError("Got a modification for an object that does not exist.")
        return membership

    def continue_chain(self, start_v, end_v, package):
        self.state[end_v] = Record(end_v, start_v, package)
        self.version_members[end_v] = self.set_membership(start_v, package)
        print ("Setting ", end_v, "membership", self.version_members[end_v])
        if self.head is not None:
            self.logger.debug("Adding changelist")
            # We are not adding the first version.
            self.state[self.head].next_id = end_v
        else:
            # Set the tail the first time.
            self.logger.debug("Adding first changelist {0}".format(end_v))
            self.tail = end_v
        self.head = end_v

    def apply(self, appname, version, package, no_maintain=False):
        start_v, end_v = version
        self.logger.debug("Applying package from {0}".format(appname))
        if start_v == end_v:
            # Nothing changed
            self.logger.debug("No change. Both versions are same")
            return True
        if self.head == start_v:
            # Continue the chain.
            self.continue_chain(start_v, end_v, package)
        else:
            self.logger.debug("Merge conflict. Resolve merge")
            self.merge(start_v, end_v, package)
        if not no_maintain:
            self.logger.debug(
                "Maintain DAG with {0}, {1}".format(appname, end_v))
            self.maintain(appname, end_v)
        return True

    def merge(self, start_v, end_v, package):
        new_v = self.tail
        conflict_v = end_v
        # package : start -> conflict
        # change: start -> new
        change, _ = self.get_diff_since(start_v)
        t_new_merge, t_conflict_merge = self.operational_transform(
            start_v, change, package)
        merge_v = str(uuid4())
        self.continue_chain(new_v, merge_v, t_new_merge)
        self.alt_path[conflict_v] = Record(merge_v, conflict_v, t_conflict_merge)
        self.alt_path[conflict_v].prev_id = start_v

    def operational_transform(self, start, new_path, conflict_path):
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
                    tpname, start, new_path[tpname], conflict_path[tpname])
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

    def ot_on_type(self, dtpname, start, new_tp_change, conflict_tp_change):
        tp_merge = dict()
        tp_conf_merge = dict()
        for oid in new_tp_change:
            if oid in conflict_tp_change:
                # Merge oid change.
                obj_merge, obj_conf_merge = self.ot_on_obj(
                    dtpname, start, new_tp_change[oid], conflict_tp_change[oid])
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

    def ot_on_obj(self, dtpname, start, new_obj_change, conf_obj_change):
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
                obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                    dtype, 
                    None,  # original
                    self.make_temp_obj(
                        start, dtype, oid, with_change=new_obj_change),  # new
                    self.make_temp_obj(
                        start, dtype, oid,
                         with_change=conf_obj_change),  # conflicting
                    new_obj_change, conf_obj_change)
            else:
                # Choose the New as other apps might have started work
                # on this object.
                obj_merge = dict()
                obj_conf_merge = self.dim_diff(conf_obj_change, new_obj_change)
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
                obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                    dtype, 
                    self.make_temp_obj(start, dtype, oid),  # original
                    self.make_temp_obj(
                        start, dtype, oid, with_change=new_obj_change),  # new
                    self.make_temp_obj(
                        start, dtype, oid,
                        with_change=conf_obj_change),  # conflicting
                    new_obj_change, conf_obj_change)
            else:
                # LWW strategy
                obj_merge = self.dim_diff(new_obj_change, conf_obj_change)
                obj_conf_merge = dict()

        elif event_new is Event.Modification and event_conf is Event.Delete:
            # resolve between an app modifyinbg it,
            # and another app deleting the object.
            if dtype.__r_meta__.merge is not None:
                obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                    dtype, 
                    self.make_temp_obj(start, dtype, oid),  # original
                    self.make_temp_obj(
                        start, dtype, oid, with_change=new_obj_change),  # new
                    None,  # conflicting
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
                obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                    dtype, 
                    self.make_temp_obj(start, dtype, oid),  # original
                    None,  # new
                    self.make_temp_obj(
                        start, dtype, oid,
                        with_change=conf_obj_change),  # conflicting
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
            self, dtype, original, new, conflicting,
            new_obj_change, conf_obj_changes):
        obj = dtype.__r_meta__.merge(original, new, conflicting)  # conflicting
        if obj:
            changes = dtype.__r_table__.store_as_temp[oid]
            del dtype.__r_table__.store_as_temp[oid]
            return (self.dim_diff(new_obj_change, changes),
                    self.dim_diff(conf_obj_change, changes))
        else:
            # Object was deleted.
            return {"types": {dtpname: Event.Delete}}, {"types": {dtpname: Event.Delete}}

    def make_temp_obj(self, version, dtype, oid, with_change=dict()):
        obj = _container()
        obj.__class__ = dtype
        obj.__r_oid__ = oid
        dtype.__r_table__.object_table[oid] = {
            dimname: (
                with_change["dims"][dimname]
                if "dims" in with_change and dimname in with_change["dims"] else
                self.read_dimension(version, dtype, oid, dimname))
            for dimname in dtype.__r_meta__.dimmap}
        
        dtype.__r_table__.store_as_temp[oid] = dict()
        return obj

    def dim_diff(self, original, new):
        # return dims that are in new but not in/different in the original.
        change = {"dims": dict(), "types": dict()}
        if not (original and "dims" in original):
            change["dims"] = new
        for dim in new:
            if dim not in original["dims"] or original["dims"][dim] == new[dim]:
                # The dim is not in original or the dim is there,
                # but the values are different.
                # copy it.
                change["dims"][dim] = new[dim]
        return change


    def maintain(self, appname, end_v):
        # reset the state markers.
        self.state_to_app.setdefault(end_v, set()).add(appname)
        if appname in self.app_to_state:
            if self.app_to_state[appname] == end_v:
                self.logger.debug("Don't need to maintain.")
                return
            old_v = self.app_to_state[appname]
            self.state_to_app[old_v].remove(appname)
            if not self.state_to_app[old_v]:
                del self.state_to_app[old_v]
        self.app_to_state[appname] = end_v
        self.logger.debug("Maintaining with {0}, {1}".format(self.app_to_state, self.state_to_app))
        # Clean up states.
        current = self.head

        while current is not None:
            change = self.state[current]
            if current in self.state_to_app and self.state_to_app[current]:
                # This is marked by some app.
                # Do not merge it with newer changed.
                current = change.prev_id
                continue
            if change.next_id is None:
                # THis is the last change
                # Do not merge it with newer changes.
                current = change.prev_id
                continue
            # Merge the change into the next newer change.
            merge_version = change.next_id
            merge_change = self.state[merge_version]
            merge_change.delta = self.merge_state_delta(
                change.delta, merge_change.delta)
            merge_change.prev_id = change.prev_id
            del self.state[current]
            del self.version_members[current]
            current = change.prev_id

    def merge_state_delta(self, old_change, newer_change):
        merged = dict()

        for dtpname in old_change:
            if dtpname not in newer_change:
                merged[dtpname] = old_change[dtpname]
            else:
                merged[dtpname] = self.merge_objectlist_deltas(
                    dtpname, old_change[dtpname], newer_change[dtpname])
        for dtpname in newer_change:
            if dtpname not in old_change:
                merged[dtpname] = newer_change[dtpname]
        return merged

    def merge_objectlist_deltas(self, dtpname, old_change, new_change):
        merged = dict()
        for oid in old_change:
            if oid not in new_change:
                merged[oid] = old_change[oid]
            else:
                merged[oid] = self.merge_object_delta(
                    dtpname, old_change[oid], new_change[oid])
        for oid in new_change:
            if oid not in old_change:
                merged[oid] = new_change[oid]
        return merged

    def merge_object_delta(self, dtpname, old_change, new_change):
        if new_change["types"][dtpname] is Event.Delete:
            return new_change
        if (old_change["types"][dtpname] is Event.Delete
                and new_change["types"][dtpname] is Event.New):
            return new_change
        if new_change["types"][dtpname] is not Event.Modification:
            raise RuntimeError(
                "Not sure why the new change does not have modification.")

        dim_change = dict(old_change["dims"])
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
