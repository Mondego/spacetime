import asyncio
import cbor
from uuid import uuid4
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Queue
from spacetime.managers.version_graph import Graph
import spacetime.utils.utils as utils
from spacetime.utils.enums import Event, VersionBy

class VersionManagerProcess(Process):
            
    def __init__(self, appname, types, version_by):
        self.version_manager = None
        if version_by == VersionBy.FULLSTATE:
            self.version_manager = FullStateVersionManager(appname, types)
        elif version_by == VersionBy.TYPE:
            self.version_manager = TypeVersionManager(appname, types)
        elif version_by == VersionBy.OBJECT_NOSTORE:
            self.version_manager = ObjectVersionManagerVersionSent(
                appname, types)
        else:
            raise NotImplementedError()
        super().__init__()
        self.daemon = True
        self.done = False
        self.read_queue = Queue()
        self.write_queue = Queue()
        
    def _retrieve_data(
            self, appname, version):
        return self.version_manager.retrieve_data(appname, version)

    async def _send_data_request(self, appname, version):
        reader, _ = await asyncio.open_connection()
        self.read_queue.put((reader, appname, version))
        data = cbor.loads(await reader.read())
        await reader.close()
        return data

    async def _process_reads(self):
        while True:
            req = self.read_queue.get()
            writer, appname, version = req
            writer.write(cbor.dumps(self._retrieve_data(appname, version)))
            writer.write_eof()
            await writer.drain()
            writer.close()

    async def _process_writes(self):
        while True:
            req = self.write_queue.get()
            if req[0] == "GET":
                appname, versions, package, from_external = req[1:]
                self.version_manager.receive_data(
                    appname, versions, package, from_external)
            elif req[0] == "CONFIRM":
                app, version = req[1:]
                self.version_manager.data_sent_confirmed(app, version)

    def run(self):
        loop = asyncio.get_event_loop()
        loop.create_task(self._process_reads())
        loop.create_task(self._process_writes())
        loop.run_forever()

    def receive_data(self, appname, versions, package, from_external=True):
        self.write_queue.put(("GET", appname, versions, package, from_external))

    def retrieve_data(self, appname, version):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(
            self._send_data_request(appname, version))

    def data_sent_confirmed(self, app, version):
        self.write_queue.put(("CONFIRM", app, version))


class VersionManager(object):
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def __init__(self, appname, types):
        self.types = types
        self.type_map = {tp.__r_meta__.name: tp for tp in types}
        self.logger = utils.get_logger("%s_VersionManager" % appname)

    @abstractmethod
    def receive_data(self, appname, versions, package, from_external=True):
        pass

    @abstractmethod
    def retrieve_data(self, appname, version):
        pass

    @abstractmethod
    def data_sent_confirmed(self, app, version):
        pass

    @abstractmethod
    def _read_dimension_at(self, version, dtype, oid, dimname):
        pass

    def operational_transform(
            self, start, new_path, conflict_path, from_external):
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
                    tpname, start, new_path[tpname], conflict_path[tpname],
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

    def ot_on_type(
            self, dtpname, start, new_tp_change, conflict_tp_change,
            from_external):
        tp_merge = dict()
        tp_conf_merge = dict()
        for oid in new_tp_change:
            if oid in conflict_tp_change:
                # Merge oid change.
                obj_merge, obj_conf_merge = self.ot_on_obj(
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

    def ot_on_obj(
            self, dtpname, oid, start, new_obj_change, conf_obj_change,
            from_external):
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
                obj_merge = self.dim_diff(new_obj_change, conf_obj_change)
                obj_conf_merge = self.dim_not_present(
                    conf_obj_change, new_obj_change)

        elif event_new is Event.Modification and event_conf is Event.Delete:
            # resolve between an app modifyinbg it,
            # and another app deleting the object.
            if dtype.__r_meta__.merge is not None:
                new = self.make_temp_obj(
                        start, dtype, oid, with_change=new_obj_change)  # new
                conflicting = None,  # conflicting
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
            if dtype.__r_meta__.merge is not None:
                new = None,  # new
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
        obj = dtype.__r_meta__.merge(original, new, conflicting)  # conflicting
        dtpname = dtype.__r_meta__.name
        if obj:
            obj.__r_temp__.update(dtype.__r_table__.store_as_temp[oid])
            changes = {
                "dims": obj.__r_temp__, "types": dict()}
            changes["types"][dtpname] = (
                Event.Modification if original is not None else Event.New)
            
            del dtype.__r_table__.store_as_temp[oid]
            return (self.dim_diff(new_obj_change, changes),
                    self.dim_diff(conf_obj_changes, changes))
        else:
            # Object was deleted.
            return (
                {"types": {dtpname: Event.Delete}},
                {"types": {dtpname: Event.Delete}})

    def make_temp_obj(self, version, dtype, oid, with_change=dict()):
        obj = utils._container()
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

    def dim_diff(self, original, new):
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
        return change

    def dim_not_present(self, original, new):
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

    def maintain(
            self, state_to_app, app_to_state,
            graph, appname, end_v, merge_func):
        # reset the state markers.
        state_to_app.setdefault(end_v, set()).add(appname)
        if appname in app_to_state:
            if app_to_state[appname] == end_v:
                self.logger.debug("Don't need to maintain.")
                return
            old_v = app_to_state[appname]
            state_to_app[old_v].remove(appname)
            if not state_to_app[old_v]:
                del state_to_app[old_v]
        app_to_state[appname] = end_v
        self.logger.debug(
            "Maintaining with {0}, {1}".format(
                app_to_state, state_to_app))
        # Clean up states.

        graph.maintain(state_to_app, merge_func)

class FullStateVersionManager(VersionManager):
    def __init__(self, appname, types):
        self.types = types
        self.type_map = {tp.__r_meta__.name: tp for tp in types}
        self.version_graph = Graph()
        self.state_to_app = dict()
        self.app_to_state = dict()
        self.logger = utils.get_logger("%s_FullStateVersionManager" % appname)

    def set_app_marker(self, appname, end_v):
        self.state_to_app.setdefault(end_v, set()).add(appname)

    def receive_data(self, appname, versions, package, from_external=True):
        start_v, end_v = versions
        if start_v == end_v:
            # The versions are the same, lets ignore.
            return True
        if start_v != self.version_graph.head.current:
            self.resolve_conflict(start_v, end_v, package, from_external)
        else:
            self.version_graph.continue_chain(start_v, end_v, package)
        self.maintain(appname, end_v)
        return True

    def retrieve_data(self, appname, version):
        data, version_change = self.retrieve_data_nomaintain(version)
        self.set_app_marker(appname, version_change[1])
        return data, version_change

    def retrieve_data_nomaintain(self, version):
        merged = dict()
        if version not in self.version_graph.nodes:
            return merged, [version, version]
        for delta in self.version_graph[version:]:
            merged = utils.merge_state_delta(merged, delta)
        return merged, [version, self.version_graph.head.current]

    def data_sent_confirmed(self, app, version):
        if version[0] != version[1]:
            self.maintain(app, version[1])

    def resolve_conflict(self, start_v, end_v, package, from_external):
        new_v = self.version_graph.head.current
        change, _ = self.retrieve_data_nomaintain(start_v)
        t_new_merge, t_conflict_merge = self.operational_transform(
            start_v, change, package, from_external)
        merge_v = str(uuid4())
        self.version_graph.continue_chain(start_v, end_v, package)
        self.version_graph.continue_chain(new_v, merge_v, t_new_merge)
        self.version_graph.continue_chain(end_v, merge_v, t_conflict_merge)

    def _read_dimension_at(self, version, dtype, oid, dimname):
        dtpname = dtype.__r_meta__.name
        for change in self.version_graph[version::-1]:
            if dtpname in change and oid in change[dtpname]:
                if ("dims" in change[dtpname][oid]
                        and dimname in change[dtpname][oid]["dims"]):
                    return change[dtpname][oid]["dims"][dimname]
                if ("types" in change[dtpname][oid]
                        and dtpname in change[dtpname][oid]["types"]
                        and change[dtpname][oid]["types"][dtpname] == Event.Delete):
                    # Object has been deleted before this version,
                    # and has not been added.
                    # Do not return a value.
                    break

    def maintain(self, appname, end_v):
        return super().maintain(
            self.state_to_app, self.app_to_state,
            self.version_graph, appname, end_v, utils.merge_state_delta)

class TypeVersionManager(VersionManager):
    def __init__(self, appname, types):
        self.types = types
        self.type_map = {tp.__r_meta__.name: tp for tp in types}
        self.version_graph = {tp.__r_meta__.name: Graph() for tp in types}
        self.state_to_app = {tp.__r_meta__.name: dict() for tp in types}
        self.app_to_state = {tp.__r_meta__.name: dict() for tp in types}
        self.logger = utils.get_logger("%s_TypeVersionManager" % appname)

    def receive_data(self, appname, versions, package, from_external=True):
        for tpname in versions:
            if tpname not in self.version_graph:
                continue
            start_v, end_v = versions[tpname]
            if start_v == end_v:
                # The versions are the same, lets ignore.
                return True
            if start_v != self.version_graph[tpname].head.current:
                self.resolve_conflict(
                    tpname, start_v, end_v, package[tpname], from_external)
            else:
                self.version_graph[tpname].continue_chain(
                    start_v, end_v, package[tpname])
            self.maintain(appname, tpname, end_v)
        return True

    def retrieve_data(self, appname, version):
        final_data, final_versions = dict(), dict()
        for tpname in version:
            if tpname not in self.version_graph:
                continue
            data, version_change = self.retrieve_data_nomaintain(
                tpname, version[tpname])
            self.set_app_marker(appname, tpname, version_change[1])
            final_data[tpname] = data
            final_versions[tpname] = version_change
        return final_data, final_versions

    def set_app_marker(self, appname, tpname, end_v):
        self.state_to_app[tpname].setdefault(end_v, set()).add(appname)

    def retrieve_data_nomaintain(self, tpname, version):
        merged = dict()
        for delta in self.version_graph[tpname][version:]:
            merged = utils.merge_objectlist_deltas(tpname, merged, delta)
        return merged, [version, self.version_graph[tpname].head.current]

    def resolve_conflict(self, tpname, start_v, end_v, package, from_external):
        new_v = self.version_graph[tpname].head.current
        change, _ = self.retrieve_data_nomaintain(tpname, start_v)
        t_new_merge, t_conflict_merge = self.ot_on_type(
            tpname, start_v, change, package, from_external)
        merge_v = str(uuid4())
        self.version_graph[tpname].continue_chain(new_v, merge_v, t_new_merge)
        self.version_graph[tpname].continue_chain(
            end_v, merge_v, t_conflict_merge)

    def data_sent_confirmed(self, app, version):
        for tpname in version:
            if version[tpname][0] != version[tpname][1]:
                self.maintain(app, tpname, version[tpname][1])

    def _read_dimension_at(self, version, dtype, oid, dimname):
        dtpname = dtype.__r_meta__.name
        for change in self.version_graph[dtpname][version::-1]:
            if oid in change:
                if "dims" in change[oid] and dimname in change[oid]["dims"]:
                    return change[oid]["dims"][dimname]
                if ("types" in change[oid]
                        and dtpname in change[oid]["types"]
                        and change[oid]["types"][dtpname] == Event.Delete):
                    # Object has been deleted before this version,
                    # and has not been added.
                    # Do not return a value.
                    break

    def maintain(self, appname, tpname, end_v):
        return super().maintain(
            self.state_to_app[tpname], self.app_to_state[tpname],
            self.version_graph[tpname], appname, end_v,
            utils.get_merge_objectlist_delta(tpname))


class ObjectVersionManagerVersionSent(VersionManager):
    def __init__(self, appname, types):
        self.types = types
        self.type_map = {tp.__r_meta__.name: tp for tp in types}
        self.version_graph = {tp.__r_meta__.name: dict() for tp in types}
        self.state_to_app = {tp.__r_meta__.name: dict() for tp in types}
        self.app_to_state = {tp.__r_meta__.name: dict() for tp in types}
        self.logger = utils.get_logger(
            "%s_ObjectVersionManagerVersionSent" % appname)

    def receive_data(self, appname, versions, package, from_external=True):
        for tpname in versions:
            if tpname not in self.version_graph:
                continue
            for oid in versions[tpname]:
                start_v, end_v = versions[tpname][oid]
                if start_v == end_v:
                    # The versions are the same, lets ignore.
                    return True
                if oid not in self.version_graph[tpname] and start_v != "ROOT":
                    # Can recover if it is a delete, but do not want to hide
                    # the error.
                    raise RuntimeError(
                        "Got an increment without having the object.")
                graph = self.version_graph[tpname].setdefault(oid, Graph())
            
                if start_v != graph.head.current:
                    self.resolve_conflict(
                        tpname, oid, start_v, end_v, package[tpname][oid],
                        from_external)
                else:
                    graph.continue_chain(start_v, end_v, package[tpname][oid])
                
                self.maintain(appname, tpname, oid, end_v)
        return True

    def retrieve_data(self, appname, version):
        final_data, final_versions = dict(), dict()
        for tpname in version:
            if tpname not in self.version_graph:
                continue
            data = dict()
            new_versions = dict()
            version_oids = set(version[tpname].keys())
            current_oids = set(self.version_graph[tpname].keys())
            deleted_oids = version_oids - current_oids
            # Deal with deletes
            for oid in deleted_oids:
                data[oid] = {
                    "types": {
                        tpname: Event.Delete
                    }
                }
                new_versions[oid] = [version[tpname][oid], "END"]
            # Deal with new
            for oid in current_oids - version_oids:
                obj_data, version_change = self.retrieve_data_nomaintain(
                    tpname, oid, "ROOT")
                self.set_app_marker(appname, tpname, oid, version_change[1])
                data[oid] = obj_data
                new_versions[oid] = version_change
            # Deal with modified
            for oid in version_oids.intersection(current_oids):
                obj_data, version_change = self.retrieve_data_nomaintain(
                    tpname, oid, version[tpname][oid])
                if obj_data:
                    self.set_app_marker(appname, tpname, oid, version_change[1])
                    data[oid] = obj_data
                    new_versions[oid] = version_change
            if data:
                final_data[tpname] = data
                final_versions[tpname] = new_versions
        return final_data, final_versions

    def set_app_marker(self, appname, tpname, oid, end_v):
        self.state_to_app[tpname].setdefault(
            oid, dict()).setdefault(end_v, set()).add(appname)

    def retrieve_data_nomaintain(self, tpname, oid, version):
        merged = dict()
        for delta in self.version_graph[tpname][oid][version:]:
            merged = utils.merge_object_delta(tpname, merged, delta)
        return merged, [version, self.version_graph[tpname][oid].head.current]

    def resolve_conflict(
            self, tpname, oid, start_v, end_v, package, from_external):
        graph = self.version_graph[tpname][oid]
        new_v = graph.head.current
        change, _ = self.retrieve_data_nomaintain(tpname, oid, start_v)
        t_new_merge, t_conflict_merge = self.ot_on_obj(
            tpname, oid, start_v, change, package, from_external)
        merge_v = str(uuid4())
        graph.continue_chain(new_v, merge_v, t_new_merge)
        graph.continue_chain(
            end_v, merge_v, t_conflict_merge)

    def data_sent_confirmed(self, app, version):
        for tpname in version:
            for oid in version[tpname]:
                if version[tpname][oid][0] != version[tpname][oid][1]:
                    self.maintain(app, tpname, oid, version[tpname][oid][1])

    def _read_dimension_at(self, version, dtype, oid, dimname):
        dtpname = dtype.__r_meta__.name
        for change in self.version_graph[dtpname][oid][version::-1]:
            if "dims" in change and dimname in change["dims"]:
                return change["dims"][dimname]
            if ("types" in change
                    and dtpname in change["types"]
                    and change["types"][dtpname] == Event.Delete):
                # Object has been deleted before this version,
                # and has not been added.
                # Do not return a value.
                break

    def maintain(self, appname, tpname, oid, end_v):
        graph = self.version_graph[tpname][oid]
        super().maintain(
            self.state_to_app[tpname].setdefault(oid, dict()),
            self.app_to_state[tpname].setdefault(oid, dict()),
            graph, appname, end_v,
            utils.get_merge_object_delta(tpname))
        if graph.head.prev_master == "ROOT" and graph.head.current == "END":
            # The object is deleted, and everyone who had it, has received a
            # delete request. So delete the object.
            del self.version_graph[tpname][oid]
            del self.state_to_app[tpname][oid]
            del self.app_to_state[tpname][oid]

