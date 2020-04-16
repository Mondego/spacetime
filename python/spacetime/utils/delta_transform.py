def delta_transformation(
        type_map, start, new_path, conflict_path, from_external):
    # This will contain all changes in the merge that do not conflict with
    # new changes in place + merged resolutions.
    t_new_merge = dict()
    # This will contain all merged resolutions. + changes in place not with
    # this version change.
    t_conflict_merge = dict()
    for tpname in new_path:
        if tpname in conflict_path:
            # Merge tp changes.
            tp_merge, tp_conf_merge = dt_on_type(
                tpname, typemap[tpname], start,
                new_path[tpname], conflict_path[tpname],
                from_external)
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

def dt_on_type(
        dtpname, start, new_tp_change, conflict_tp_change, from_external):
    tp_merge = dict()
    tp_conf_merge = dict()
    for oid in new_tp_change:
        if oid in conflict_tp_change:
            # Merge oid change.
            obj_merge, obj_conf_merge = dt_on_obj(
                dtpname, oid,
                start, new_tp_change[oid], conflict_tp_change[oid],
                from_external)
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

def dt_on_obj(
        dtpname, oid, start, new_obj_change, conf_obj_change, from_external):
    dtype = self.type_map[dtpname]
    obj_merge = dict()
    obj_conf_merge = dict()
    # dtpname event determines the base type's changes.
    event_new = new_obj_change["types"][dtpname]
    event_conf = conf_obj_change["types"][dtpname]
    if event_new is Event.New and event_conf is Event.New:
        # Both paths have created a new object.
        # Resolve that.
        if self.resolver and dtype in self.resolver:
            new = self.make_temp_obj(
                    start, dtype, oid, with_change=new_obj_change)  # new
            conflicting = self.make_temp_obj(
                    start, dtype, oid,
                        with_change=conf_obj_change)  # conflicting
            yours = new if from_external else conflicting
            theirs = conflicting if from_external else new
            obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                dtype, oid,
                None,  # original,
                yours,
                theirs,
                new_obj_change, conf_obj_change)
        else:
            # Choose the New as other apps might have started work
            # on this object.
            obj_merge = dict()
            obj_conf_merge = utils.dim_diff(dtpname, conf_obj_change, new_obj_change)
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
        if self.resolver and dtype in self.resolver:
            new = self.make_temp_obj(
                start, dtype, oid, with_change=new_obj_change)  # new
            conflicting = self.make_temp_obj(
                start, dtype, oid,
                with_change=conf_obj_change)  # conflicting
            yours = new if from_external else conflicting
            theirs = conflicting if from_external else new
            obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                dtype, oid,
                self.make_temp_obj(start, dtype, oid),  # original
                yours,  # new
                theirs,  # conflicting
                new_obj_change, conf_obj_change)
        else:
            # LWW strategy
            obj_merge = utils.dim_diff(
                dtpname, new_obj_change, conf_obj_change)
            obj_conf_merge = utils.dim_not_present(
                conf_obj_change, new_obj_change)

    elif event_new is Event.Modification and event_conf is Event.Delete:
        # resolve between an app modifyinbg it,
        # and another app deleting the object.
        if self.resolver and dtype in self.resolver:
            new = self.make_temp_obj(
                start, dtype, oid, with_change=new_obj_change)  # new
            conflicting = None  # conflicting
            yours = new if from_external else conflicting
            theirs = conflicting if from_external else new
            obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                dtype, oid,
                self.make_temp_obj(start, dtype, oid),  # original
                yours,  # new
                theirs,  # conflicting
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
        if self.resolver and dtype in self.resolver:
            new = None  # new
            conflicting = self.make_temp_obj(
                start, dtype, oid,
                with_change=conf_obj_change)  # conflicting
            yours = new if from_external else conflicting
            theirs = conflicting if from_external else theirs
            obj_merge, obj_conf_merge = self.resolve_with_custom_merge(
                dtype, oid,
                self.make_temp_obj(start, dtype, oid),  # original
                yours,  # new
                theirs,  # conflicting
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
        self, dtype, oid, original, new, conflicting,
        new_obj_change, conf_obj_changes):
    obj = self.resolver[dtype](original, new, conflicting)  # conflicting
    dtpname = dtype.__r_meta__.name
    if obj:
        obj.__r_temp__.update(dtype.__r_table__.store_as_temp[oid])
        changes = {
            "dims": obj.__r_temp__, "types": dict()}
        changes["types"][dtpname] = (
            Event.Modification if original is not None else Event.New)

        del dtype.__r_table__.store_as_temp[oid]
        return (utils.dim_diff(dtpname, new_obj_change, changes),
                utils.dim_diff(dtpname, conf_obj_changes, changes))
    else:
        # Object was deleted.
        return (
            {"types": {dtpname: Event.Delete}},
            {"types": {dtpname: Event.Delete}})

def make_temp_obj(self, version, dtype, oid, with_change=dict()):
    obj = utils.container()
    obj.__class__ = dtype
    obj.__r_oid__ = oid
    obj.__r_temp__ = {
        dimname: (
            with_change["dims"][dimname]
            if "dims" in with_change and dimname in with_change["dims"] else
            self._read_dimension_at(version, dtype, oid, dimname))
        for dimname in dtype.__r_meta__.dimmap}

    dtype.__r_table__.store_as_temp[oid] = dict()
    return obj

def dim_diff(dtpname, original, new):
    # return dims that are in new but not in/different in the original.
    change = {"dims": dict(), "types": dict()}
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

def dim_not_present(original, new):
    # return dims that are in new but not in in the original.
    change = {"dims": dict(), "types": dict()}
    if not (original and "dims" in original):
        return new
    for dim in new["dims"]:
        if dim not in original["dims"]:
            # The dim is not in original or the dim is there,
            # but the values are different.
            # copy it.
            change["dims"][dim] = new["dims"][dim]
    change["types"].update(original["types"])
    change["types"].update(new["types"])
    return change
