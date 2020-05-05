import time
from uuid import uuid4
from collections import deque, OrderedDict
from threading import Condition

from spacetime.utils.enums import Event
from spacetime.utils import utils
from spacetime.utils.rwlock import RWLockFair as RWLock
from spacetime.utils.utils import instrument_vg_put
from rtypes.utils.enums import DiffType
from rtypes.utils.converter import unconvert, convert

DUMP_GRAPH = False


def rlock(func):
    def read_locked_func(self, *args, **kwargs):
        try:
            with self.rwlock.gen_rlock():
                return func(self, *args, **kwargs)
        except AssertionError:
            print ("Assertion")
            raise
        # except Exception:
        #     self.logger.error(
        #         f"READ: {self.nodename}, {self.alias}, {self.node_to_version}, {list(self.edges.keys())}")
        #     raise
    return read_locked_func


def wlock(func):
    def write_locked_func(self, *args, **kwargs):
        try:
            with self.rwlock.gen_wlock():
                edges_s = str(self.edges)
                return func(self, *args, **kwargs)
        except AssertionError:
            self.logger.error(
                 f"ASSERT ON WRITE: NODENAME: {self.nodename},\n ALIAS: {self.alias},\n NODETOVERSION: {self.node_to_version},\n VERSIONTONODE: {self.version_to_node},\n EDGES: {list(self.edges.keys())},\n ARGS: {args}\n OLD EDGES: {edges_s}")
            raise
        # except Exception:
        #     self.logger.error(
        #         f"WRITE: {self.nodename}, {self.alias}, {self.node_to_version}, {list(self.edges.keys())}, {args}")
        #     raise
    return write_locked_func


class Version(object):
    def __repr__(self):
        return self.vid

    def __str__(self):
        return str(self.vid)

    def __eq__(self, version):
        return (
            self.vid == version.vid
            and ({c.vid for c in self.children}
                 == {c.vid for c in version.children})
            and ({p.vid for p in self.parents}
                 == {p.vid for p in version.parents}))

    def __hash__(self):
        return hash(self.vid)

    def __init__(self, vid, children=None, parents=None, creation_time=None):
        self.vid = vid
        # if children or parents are set, it is purely in test cases
        # to ensure the mechanics work.
        # children and parents are not to be set in constructor
        # outside of test cases.
        self.children = children if children else set()
        self.parents = parents if parents else set()
        self.creation_time = (
            creation_time if creation_time else time.perf_counter())

    def add_child(self, child):
        self.children.add(child)
        child.parents.add(self)

    def remove_child(self, child):
        self.children.remove(child)
        child.parents.remove(self)

class Edge(object):
    def __repr__(self):
        return f"EID:{self.eid} FROM:{self.from_v} TO:{self.to_v}"

    def __eq__(self, edge):
        return (
            self.eid == edge.eid
            and self.from_v == edge.from_v
            and self.to_v == edge.to_v
            and self.delta == edge.delta)

    def __hash__(self):
        return hash((self.eid, self.from_v))

    def __iter__(self):
        return iter((self.from_v, self.to_v, self.delta, self.eid))

    def __init__(self, from_v, to_v, delta, eid):
        self.eid = eid
        self.from_v = from_v
        self.to_v = to_v
        self.delta = delta


class EidGroup():
    @property
    def delta(self):
        if not self._delta:
            self._delta = self.calculate_delta()
        return self._delta

    def __init__(self, start, eids, nodes, logger):
        self.start = start
        self.eids = eids
        self.nodes = nodes
        self.end = start
        self.tracked_sets = list()
        self.version_to_group = dict()
        self._delta = None
        self.new_eid = str(uuid4())
        self.logger = logger

    def add_edge(self, edge):
        if edge.from_v not in self.version_to_group:
            # This is now a new group.
            new_group = [
                {edge}, edge.from_v, edge.to_v, {edge.from_v, edge.to_v}]
            self.tracked_sets.append(new_group)
            self.version_to_group[edge.from_v] = new_group
            self.version_to_group[edge.to_v] = new_group
        else:
            group = self.version_to_group[edge.from_v]
            group[0].add(edge)
            group[2] = edge.to_v
            group[3].add(edge.to_v)
            self.version_to_group[edge.to_v] = group

    def calculate_delta(self):
        edges, start, end, _ = self.tracked_sets[0]
        edge_map = {
            (e.from_v, e.to_v): e
            for e in edges
        }

        final_path = list()
        potential_paths = [[start]]
        while not final_path:
            path = potential_paths.pop()
            last = path[-1]
            for child in last.children:
                if (last, child) not in edge_map:
                    continue
                if child == end:
                    final_path = path + [child]
                    break
                potential_paths.append(path + [child])

        updated_delta = dict()
        for i in range(len(final_path) - 1):
            edge = edge_map[(final_path[i], final_path[i+1])]
            updated_delta = self._merge_delta(updated_delta, edge.delta)

        return updated_delta

    def _merge_delta(self, prev_delta, next_delta):
        merge_delta = dict()
        for tpname, tp_delta in prev_delta.items():
            merge_delta[tpname] = (
                self._merge_tp_delta(tpname, tp_delta, next_delta[tpname])
                if tpname in next_delta else
                tp_delta)
        merge_delta.update({
            tpname: tp_delta
            for tpname, tp_delta in next_delta.items()
            if tpname not in prev_delta})
        return merge_delta

    def _merge_tp_delta(self, tpname, prev_delta, next_delta):
        merge_delta = dict()
        for oid, obj_delta in prev_delta.items():
            if oid in next_delta:
                if (obj_delta["types"][tpname] == Event.New
                        and next_delta[oid]["types"][tpname] == Event.Delete):
                    continue
                merge_delta[oid] = self._merge_obj_delta(
                    tpname, obj_delta, next_delta[oid])
            else:
                merge_delta[oid] = obj_delta
        merge_delta.update({
            oid: obj_delta
            for oid, obj_delta in next_delta.items()
            if oid not in prev_delta})
        return merge_delta

    def _merge_obj_delta(self, tpname, prev_obj, next_obj):
        if next_obj["types"][tpname] == Event.Delete:
            return next_obj
        prev_obj["dims"].update(next_obj["dims"])
        return prev_obj


class VersionGraph(object):
    def __init__(self, nodename, types, resolver=None,
                 log_to_std=False, log_to_file=False):
        self.logger = utils.get_logger(
            f"version_graph_{nodename}", log_to_std, log_to_file)
        self.nodename = nodename
        self.types = types
        self.type_map = {tp.__r_meta__.name: tp for tp in types}
        self.tpnames = {
            tp.__r_meta__.name: tp.__r_meta__.name_chain
            for tp in self.types
        }

        self.tp_to_dim = {
            tp.__r_meta__.name: tp.__r_meta__.dimmap
            for tp in types
        }

        # Main cast
        self.logger.info(f"Creating version {'ROOT'}")
        self.tail = self.head = Version("ROOT")
        # vid -> version obj
        self.versions = {"ROOT": self.head}
        # (from_vobj, to_vobj) -> edge obj
        self.edges = dict()

        # vid -> vobj
        self.alias = dict()
        self.reverse_alias = dict()
        self.node_alias_map = dict()

        # Supporting roles
        self.resolver = resolver

        self.forward_edge_map = dict()
        self.graph_change_event = Condition()
        self.rwlock = RWLock()

        # GC stuff
        self.node_to_eid = dict()
        self.eids = set()
        self.version_to_node = dict()
        self.node_to_version = dict()

        self.node_to_lock = dict()
        self.node_to_confirmed = dict()
        self.version_to_required = dict()
        self.confirm_to_gc = False

    def _add_single_edge(self, from_v, to_v, delta, eid):
        self.logger.info(f"Adding new edge ({from_v.vid}, {to_v.vid})")
        self.edges[(from_v, to_v)] = Edge(from_v, to_v, delta, eid)
        self.forward_edge_map[(from_v, eid)] = to_v
        from_v.add_child(to_v)

    def _add_edges(self, remotename, edges, version_ts):
        newly_added = set()
        to_be_processed = list(edges)
        prev_unprocessed_count = len(to_be_processed)
        while to_be_processed:
            unprocessed_count = 0
            next_tbp = list()
            for from_vid, to_vid, delta, eid in to_be_processed:
                from_v, to_v = self._resolve_versions(
                    remotename, from_vid, to_vid, eid, version_ts)
                if from_v.vid not in self.versions:
                    next_tbp.append((from_vid, to_vid, delta, eid))
                    unprocessed_count += 1
                    continue

                if to_v.vid not in self.versions:
                    self.versions[to_v.vid] = to_v
                    newly_added.add(to_v)

                if (from_v, to_v) not in self.edges:
                    if (from_v, eid) not in self.forward_edge_map:
                        self._add_single_edge(from_v, to_v, delta, eid)
                    else:
                        self._equate_versions(
                            remotename,
                            self.forward_edge_map[(from_v, eid)], to_v)
                        if to_v in newly_added:
                            newly_added.remove(to_v)
            to_be_processed = next_tbp
            if unprocessed_count == prev_unprocessed_count:
                raise RuntimeError(
                    self.nodename, "Cannot add some edges",
                    next_tbp, self.alias)
            prev_unprocessed_count = unprocessed_count
        return newly_added

    def _equate_versions(self, remotename, original, alternate):
        for parent in alternate.parents:
            edge = self.edges[(parent, alternate)]
            if (parent, original) not in self.edges:
                self._add_single_edge(parent, original, edge.delta, edge.eid)
        for child in alternate.children:
            edge = self.edges[(alternate, child)]
            if (original, child) not in self.edges:
                self._add_single_edge(original, child, edge.delta, edge.eid)
        self.alias[alternate.vid] = original.vid
        self.node_alias_map.setdefault(
            original.vid, dict())[remotename] = alternate.vid
        self.reverse_alias.setdefault(original.vid, set()).add(alternate.vid)
        self._delete_version(alternate, "from equate versions")

    def _resolve_versions(self, remotename, from_vid, to_vid, eid, version_ts):
        from_vid = self.choose_alias(from_vid)

        if from_vid in self.versions:
            from_v = self.versions[from_vid]
        else:
            self.logger.info(f"Creating version {from_vid}")
            from_v = Version(from_vid, creation_time=version_ts)

        to_vid = self.choose_alias(to_vid)

        if to_vid in self.versions:
            return from_v, self.versions[to_vid]

        if (from_v, eid) in self.forward_edge_map:
            existing_to_v = self.forward_edge_map[(from_v, eid)]
            self.logger.info(f"Setting alias {to_vid} <-> {existing_to_v.vid}")
            self.alias[to_vid] = existing_to_v.vid
            self.node_alias_map.setdefault(
            existing_to_v.vid, dict())[remotename] = to_vid
            self.reverse_alias.setdefault(existing_to_v.vid, set()).add(to_vid)
            return from_v, existing_to_v
        self.logger.info(f"Creating version {to_vid}")
        return from_v, Version(to_vid, creation_time=version_ts)

    def _build_bridge_node(self, newly_added, version_ts):
        to_see = deque([self.tail])
        while to_see:
            version = to_see.popleft()
            for parent in version.parents:
                if parent.children == {version}:
                    continue
                if any(sib.children.intersection(version.children)
                       for sib in parent.children
                       if (sib != version
                               and version in newly_added
                               and sib not in newly_added)):
                    continue
                try:
                    sib = next(
                        sib for sib in parent.children
                        if (sib != version
                            and version in newly_added
                            and sib not in newly_added))
                except StopIteration:
                    # There are no candidates of any type.
                    continue
                newly_added.add(
                    self._merge(parent, sib, version, version_ts))
                # Once we add one bridge node, the rest of the nodes
                # will stitch together.
                return newly_added
            to_see.extend(version.children)
        return newly_added

    def _complete_graph(self, newly_added, version_ts):
        head = self.head
        newly_added = self._build_bridge_node(newly_added, version_ts)
        while newly_added:
            merge_versions = list()
            for version in newly_added:
                for parent in version.parents:
                    if parent.children == {version}:
                        continue
                    if any(sib.children.intersection(version.children)
                           for sib in parent.children if sib != version):
                        continue
                    sib = next(
                        sib for sib in parent.children if sib != version)
                    merge_versions.append(
                        self._merge(parent, sib, version, version_ts))
                if not version.children:
                    head = version
            newly_added = merge_versions

        return head

    def _merge(self, parent, sibling, version, version_ts):
        new_v = Version(str(uuid4()), creation_time=version_ts)
        self.logger.info(
            f"Creating merge version of {sibling.vid}, "
            f"{version.vid} as {new_v.vid}")
        self.versions[new_v.vid] = new_v
        current_merge, conf_merge = self._delta_transform(
            parent, self.edges[(parent, version)].delta,
            self.edges[(parent, sibling)].delta, False)
        self._add_single_edge(
            sibling, new_v,
            conf_merge,
            self.edges[(parent, version)].eid)
        self._add_single_edge(
            version, new_v,
            current_merge,
            self.edges[(parent, sibling)].eid)
        return new_v

    def _delta_transform(
            self, origin_v, current_delta, conf_delta, from_external):
        current_merge = dict()
        conf_merge = dict()
        for tpname in current_delta:
            if tpname in conf_delta:
                # There may be conflicts in these.
                your_tp_merge, their_tp_merge = self._dt_on_type(
                    tpname, origin_v, current_delta[tpname], conf_delta[tpname],
                    from_external)
                if your_tp_merge:
                    current_merge[tpname] = your_tp_merge
                if their_tp_merge:
                    conf_merge[tpname] = their_tp_merge
            else:
                # Include non conflicting changes
                # in yours to their recovery path.
                conf_merge[tpname] = current_delta[tpname]
        # Include non conflicting changes
        # in theirs to your recovery path.
        current_merge.update({
            tpname: conf_delta[tpname]
            for tpname in conf_delta
            if tpname not in current_delta
        })
        return current_merge, conf_merge

    def _dt_on_type(
            self, tpname, origin_v, current_delta, conf_delta, from_external):
        current_merge = dict()
        conf_merge = dict()
        for oid in current_delta:
            if oid in conf_delta:
                # Object is in conflict!
                your_obj_merge, their_obj_merge = self._dt_on_obj(
                    tpname, oid, origin_v, current_delta[oid], conf_delta[oid],
                    from_external)
                if your_obj_merge:
                    current_merge[oid] = your_obj_merge
                if their_obj_merge:
                    conf_merge[oid] = their_obj_merge
            else:
                conf_merge[oid] = current_delta[oid]
        current_merge.update({
            oid: conf_delta[oid]
            for oid in conf_delta
            if oid not in current_delta
        })
        return current_merge, conf_merge

    def _dt_on_obj(
            self, dtpname, oid, start, new_obj_change,
            conf_obj_change, from_external):
        dtype = self.type_map[dtpname]
        # dtpname event determines the base type's changes.
        event_new = new_obj_change["types"][dtpname]
        event_conf = conf_obj_change["types"][dtpname]
        if ((event_new is Event.New
             and event_conf in {Event.Modification, Event.Delete})
                or (event_new in {Event.Modification, Event.Delete}
                    and event_conf is Event.New)):
            # This has to be an error.
            raise RuntimeError(
                "Impossible combination for merge.")
        if event_new is Event.Delete and event_conf is Event.Delete:
            return dict(), dict()
        if self.resolver and dtype in self.resolver:
            original = (
                None
                if event_new is Event.New else
                self._make_temp_obj(start, dtype, oid))
            new = (
                self._make_temp_obj(
                    start, dtype, oid, with_change=new_obj_change)
                if event_new is not Event.Delete else
                None)
            conflicting = (
                self._make_temp_obj(
                    start, dtype, oid, with_change=conf_obj_change)
                if event_conf is not Event.Delete else
                None)
            yours = new if from_external else conflicting
            theirs = conflicting if from_external else new
            return self._resolve_with_custom_merge(
                dtype, oid, original, yours, theirs,
                new_obj_change, conf_obj_change)
        if event_new == event_conf:
            return (
                (dict()
                 if event_new == Event.New else
                 utils.dim_diff(
                     dtpname, new_obj_change, conf_obj_change,
                     None, None, self.tp_to_dim[dtpname])),
                utils.dim_diff(
                    dtpname, conf_obj_change, new_obj_change,
                    None, None, self.tp_to_dim[dtpname]))
        return conf_obj_change, dict()

    def _resolve_with_custom_merge(
            self, dtype, oid, original, new, conflicting,
            new_obj_change, conf_obj_changes):
        obj = self.resolver[dtype](original, new, conflicting)  # conflicting
        dtpname = dtype.__r_meta__.name
        dimmap = self.tp_to_dim[dtpname]
        if obj:
            obj.__r_temp__.update(dtype.__r_table__.store_as_temp[oid])
            changes = {
                "dims": {
                    dim: (
                        obj
                        if dimmap[dim].custom_diff else
                        convert(dimmap[dim].dim_type, value))
                    for dim, value in obj.__r_temp__.items()},
                "types": dict()}
            changes["types"][dtpname] = (
                Event.Modification if original is not None else Event.New)
            del dtype.__r_table__.store_as_temp[oid]
            return (
                utils.dim_diff(
                    dtpname, new_obj_change, changes, original, new, dimmap),
                utils.dim_diff(
                    dtpname, conf_obj_changes, changes,
                    original, conflicting, dimmap))
        # Object was deleted.
        return (
            {"types": {dtpname: Event.Delete}},
            {"types": {dtpname: Event.Delete}})

    def _make_temp_obj(self, version, dtype, oid, with_change=None):
        if with_change is None:
            with_change = dict()
        obj = utils.container()
        obj.__class__ = dtype
        obj.__r_oid__ = oid
        dtpname = dtype.__r_meta__.name
        dimmap = self.tp_to_dim[dtpname]
        obj.__r_temp__ = {
            dimname: (
                (unconvert(with_change["dims"][dimname],
                           dimmap[dimname].dim_type)
                 if not dimmap[dimname].custom_diff else
                 dimmap[dimname].custom_diff.apply(
                     self._read_dimension_at(version, dtype, oid, dimname),
                     with_change["dims"][dimname]))
                if "dims" in with_change and dimname in with_change["dims"] else
                self._read_dimension_at(version, dtype, oid, dimname))
            for dimname in dtype.__r_meta__.dimmap}

        dtype.__r_table__.store_as_temp[oid] = dict()
        return obj

    def _read_dimension_at(self, version, dtype, oid, dimname):
        dtpname = dtype.__r_meta__.name
        dimmap = self.tp_to_dim[dtype.__r_meta__.name]
        if dimmap[dimname].custom_diff:
            deltas = deque()
            for edge in self.get_edges_to_root(version):
                if dtpname in edge.delta and oid in edge.delta[dtpname]:
                    if (edge.delta[dtpname][oid]["types"][dtpname]
                            == Event.Delete):
                        return None
                    if dimname in edge.delta[dtpname][oid]["dims"]:
                        deltas.appendleft(
                            edge.delta[dtpname][oid]["dims"][dimname])
                        if (edge.delta[dtpname][oid]["dims"][dimname]["type"]
                                == DiffType.NEW):
                            break
            if not deltas:
                return None
            obj = dimmap[dimname].custom_diff.new(deltas.popleft())
            for delta in deltas:
                dimmap[dimname].custom_diff.apply(obj, delta)
            return obj
        for edge in self.get_edges_to_root(version):
            if dtpname in edge.delta and oid in edge.delta[dtpname]:
                if (edge.delta[dtpname][oid]["types"][dtpname]
                        == Event.Delete):
                    return None
                if dimname in edge.delta[dtpname][oid]["dims"]:
                    return unconvert(
                        edge.delta[dtpname][oid]["dims"][dimname],
                        dimmap[dimname].dim_type)
        return None

    def node_alias_match(self, nodename, vid):
        if vid in self.node_alias_map and nodename in self.node_alias_map[vid]:
            return self.node_alias_map[vid][nodename]
        return vid

    @rlock
    def get(self, nodename, versions=None):
        if not versions:
            read_nodename = "R-{0}-{1}".format(nodename, self.nodename)
            write_nodename = "W-{0}-{1}".format(nodename, self.nodename)
            versions = {
                (self.node_to_version[read_nodename].vid
                 if read_nodename in self.node_to_version else
                 self.tail.vid),
                (self.node_to_version[write_nodename].vid
                 if write_nodename in self.node_to_version else
                 self.tail.vid)}
        before_req = {
            self.versions[self.choose_alias(from_vid)] for from_vid in versions}
        if self.head in before_req:
            return list(), self.head.vid

        to_see = deque([self.head])
        edges = list()
        while to_see:
            version = to_see.popleft()
            for parent in version.parents:
                edge = self.edges[(parent, version)]
                edges.append(
                    (self.node_alias_match(nodename, parent.vid),
                     self.node_alias_match(nodename, version.vid),
                     edge.delta, edge.eid))
                if (parent not in before_req
                        and not parent.children.intersection(before_req)):
                    to_see.append(parent)
                else:
                    before_req.add(parent)
        # By reversing it, the adding of nodes is more efficient.
        edges.reverse()
        # self.logger.info(
        #     f"Get request: (RESPONSE) "
        #     f"{', '.join(f'{f[:4]}->{t[:4]}' for f,t,_,_ in edges)}")
        transaction_id = str(uuid4())
        self.confirm_fetch(
            "O-{0}-{1}".format(nodename, transaction_id), self.head.vid)
        self.confirm_fetch(
            "R-{0}-{1}".format(nodename, self.nodename), self.head.vid)
        self.logger.info(
            f"Get request: (FROM) {nodename} {versions} "
            f"(TO) {self.head.vid}, {self.node_to_version}")
        return edges, self.head.vid, self.get_refs(nodename), transaction_id

    @wlock
    # @instrument_vg_put
    def put(self, req_node, remote_refs, edges):
        # self.logger.info(
        #     f"Put request: "
        #     f"{', '.join(f'{f[:4]}->{t[:4]}' for f,t,_,_ in edges)}")
        version_ts = time.perf_counter()
        vids = set()
        for s, e, _, _ in edges:
            vids.add(self.choose_alias(s))
            vids.add(self.choose_alias(e))
        if self.confirm_to_gc:
            self.garbage_collect(ignore=vids)
            self.confirm_to_gc = False
        head = self._complete_graph(
            self._add_edges(req_node, edges, version_ts), version_ts)
        self._update_refs(remote_refs)
        self.head = head
        writename = "W-{0}-{1}".format(req_node, self.nodename)
        if req_node != self.nodename and writename in remote_refs:
            remotehead = self.choose_alias(remote_refs[writename])
            self.version_to_required.setdefault(remotehead, set()).add(req_node)
            if req_node not in self.node_to_confirmed:
                self.node_to_confirmed[req_node] = list()
        assert len([v for v in self.versions.values() if len(v.children) == 0]) == 1
        assert len([v for v in self.versions.values() if len(v.parents) == 0]) == 1
        if DUMP_GRAPH:
            utils.dump_graph(
                self, f"DUMP/{self.nodename}-{version_ts}-0GETBEFORE.png")
        self.garbage_collect()
        if DUMP_GRAPH:
            utils.dump_graph(
                self, f"DUMP/{self.nodename}-{version_ts}-1GETAFTER.png")
        assert len([v for v in self.versions.values() if len(v.children) == 0]) == 1
        assert len([v for v in self.versions.values() if len(v.parents) == 0]) == 1
        self.logger.info(
            f"Put request: {len(edges)}, {remote_refs}, "
            f"{self.head}, {self.node_to_version}")
       #  pre_gc_end = time.perf_counter()
       #  pre_gc_proc_end = time.process_time()

       #  gc_start = time.perf_counter()
       #  gc_proc_start = time.process_time()
       #  self.garbage_collect()
       #  gc_end = time.perf_counter()
       #  gc_proc_end = time.process_time()
       #  self.benchmark_q.put({"nodename": self.nodename,
       #                        "put_start": pre_gc_start,
       #                        "pre_gc": pre_gc_end-pre_gc_start,
       #                        "pre_gc_proc": pre_gc_proc_end-pre_gc_proc_start,
       #                        "gc": gc_end-gc_start,
       #                        "gc_proc": gc_proc_end-gc_proc_start,
       #                        "put_end": gc_end,
       #                        "versions_len": len(self.versions)})
        return self.head

    @wlock
    def pre_gc(self):
        self.garbage_collect()

    def put_as_heap(self, heapname, version, diff):
        # self.logger.info(
        #     f"Put request: "
        #     f"{', '.join(f'{f[:4]}->{t[:4]}' for f,t,_,_ in edges)}")
        refs = {heapname: diff.version}
        return self.put(
            heapname, refs, [(version, diff.version, diff, diff.version)])

    def get_refs(self, remotename):
        read_remotename = "R-{0}-{1}".format(remotename, self.nodename)
        write_remotename = "W-{0}-{1}".format(remotename, self.nodename)
        refs = {
            nodename: version.vid
            for nodename, version in self.node_to_version.items()
            if (not nodename.startswith("O-")
                and nodename not in {
                    remotename, read_remotename, write_remotename})}

        refs["W-{0}-{1}".format(self.nodename, remotename)] = self.head.vid
        return refs

    def _build_causal_chain(self):
        to_see = deque([self.tail])
        eid_to_parent = OrderedDict()
        incoming_eids = {self.tail: set()}
        all_eids_in_version = {self.tail: set()}
        all_eids = set()

        while to_see:
            version = to_see.popleft()
            eids_to_version = set()
            parent_eids = set()
            for parent in version.parents:
                edge_id = self.edges[(parent, version)].eid
                eids_to_version.add(edge_id)
                all_eids.add(edge_id)
                try:
                    parent_eids.update(all_eids_in_version[parent])
                except:
                    print (all_eids_in_version, self.edges, len(self.versions))
                    raise
                if edge_id not in eid_to_parent:
                    eid_to_parent[edge_id] = incoming_eids[parent]
            incoming_eids[version] = eids_to_version
            all_eids_in_version[version] = parent_eids.union(eids_to_version)
            to_see.extend(version.children)
        return eid_to_parent, all_eids_in_version, all_eids

    def _build_node_requirement_map(self, all_eids_in_version, all_eids):
        eid_missing_to_nodes = dict()
        for node in self.node_to_version:
            for eid in all_eids:
                if eid not in all_eids_in_version[self.node_to_version[node]]:
                    eid_missing_to_nodes.setdefault(eid, list()).append(node)
        return eid_missing_to_nodes

    def _create_dependency_groups(self, eid_to_parent, eid_missing_to_nodes):
        groups = set()
        eid_to_group = dict()
        none_groups = set()
        for eid, dependent_eids in eid_to_parent.items():
            nodes_that_require = eid_missing_to_nodes.setdefault(eid, set())
            added = False
            if not dependent_eids:
                potential_group = set()
                for group in none_groups:
                    if group.nodes == nodes_that_require:
                        potential_group.add(group)
                if len(potential_group) == 1:
                    group = potential_group.pop()
                    group.eids.add(eid)
                    eid_to_group[eid] = group
                    added = True
            for dep_eid in dependent_eids:
                dep_group = eid_to_group[dep_eid]
                if dep_group.nodes == nodes_that_require:
                    dep_group.eids.add(eid)
                    dep_group.end = eid
                    eid_to_group[eid] = dep_group
                    added = True
            if not added:
                new_group = EidGroup(eid, {eid}, nodes_that_require, self.logger)
                if not dependent_eids:
                    none_groups.add(new_group)
                eid_to_group[eid] = new_group
                groups.add(new_group)
        return groups, eid_to_group

    def _add_edges_to_groups(self, eid_to_group):
        to_see = deque([self.tail])
        while to_see:
            version = to_see.popleft()
            for child in version.children:
                edge = self.edges[(version, child)]
                group = eid_to_group[edge.eid]
                group.add_edge(edge)
            to_see.extend(version.children)

    def _clean_unneeded_branches(self, ignore=None):
        if ignore is None:
            ignore = set()
        to_see = deque([self.tail])
        seen = set()
        while to_see:
            version = to_see.popleft()
            if version in seen:
                continue
            seen.add(version)
            while (len(version.parents) == 1
                   and len(version.children) == 1
                   and version not in self.version_to_node
                   and any(
                       (sibling in self.version_to_node
                        or len(sibling.children) > 1)
                       for sibling in next(p for p in version.parents).children
                       if sibling != version)):
                parent = next(p for p in version.parents)
                if version.vid not in ignore:
                    self._delete_version(version, "from uneeded")
                version = parent
            to_see.extend(version.children)

    def _delete_version(self, version, from_loc):
        if version.vid not in self.versions:
            return
        self.logger.info(f"Deleting version {version.vid}, {from_loc}")
        parents = set(version.parents)
        for parent in parents:
            if (parent, version) in self.edges:
                del self.forward_edge_map[
                    (parent, self.edges[(parent, version)].eid)]
                self.logger.info(f"Deleting edge ({parent.vid}, {version.vid})")
                del self.edges[(parent, version)]
                parent.remove_child(version)

        children = set(version.children)
        for child in children:
            if (version, child) in self.edges:
                del self.forward_edge_map[
                    (version, self.edges[(version, child)].eid)]
                self.logger.info(f"Deleting edge ({version.vid}, {child.vid})")
                del self.edges[(version, child)]
                version.remove_child(child)

        if version.vid in self.version_to_required:
            for node in self.version_to_required[version.vid]:
                self.node_to_confirmed[node].append(version.vid)
                if version.vid in self.reverse_alias:
                    self.node_to_confirmed[node].extend(
                        self.reverse_alias[version.vid])
            del self.version_to_required[version.vid]

        if version.vid in self.reverse_alias:
            for vid in self.reverse_alias[version.vid]:
                del self.alias[vid]
            del self.reverse_alias[version.vid]

        if version.vid in self.node_alias_map:
            del self.node_alias_map[version.vid]

        if version in self.version_to_node:
            del self.version_to_node[version]

        del self.versions[version.vid]

    def garbage_collect(self, ignore=None):
        # A map of eid -> set of eids that it depends on.
        # The first occurence of eid is used to determine the set
        # as this is the definitive set.
        if ignore is None:
            ignore = set()

        self._clean_unneeded_branches(ignore=ignore)

        eid_to_parent, all_eids_in_version, all_eids = (
            self._build_causal_chain())

        eid_missing_to_nodes = self._build_node_requirement_map(
            all_eids_in_version, all_eids)

        groups, eid_to_group = self._create_dependency_groups(
            eid_to_parent, eid_missing_to_nodes)

        self._add_edges_to_groups(eid_to_group)

        edges_to_add = list()
        versions_to_delete = set()
        for group in groups:
            for _, start, end, versions in group.tracked_sets:
                if not group.nodes and start != self.tail:
                    self.logger.info(
                        f"UUpdating versions to delete with {versions - {end}}")
                    versions_to_delete.update(versions - {end})
                    continue
                if (start, end) not in self.edges:
                    edges_to_add.append(
                        Edge(start, end, group.delta, group.new_eid))
                self.logger.info(
                    f"LUpdating versions to delete with "
                    f"{versions - {start, end}}, {versions}")
                versions_to_delete.update(versions - {start, end})

        for from_v, to_v, delta, eid in edges_to_add:
            if from_v in versions_to_delete or to_v in versions_to_delete:
                continue
            self._add_single_edge(from_v, to_v, delta, eid)

        for version in versions_to_delete:
            if version.vid not in ignore:
                self._delete_version(version, "from end of GC")

    def choose_alias(self, version):
        alias_vid = (
            self.alias[version]
            if version in self.alias else
            version)
        return alias_vid

    @rlock
    def get_edges_to_head(self, node, version):
        version = self.choose_alias(version)
        if version not in self.versions:
            raise RuntimeError(f"Version {version} not found.")
        version_obj = self.versions[version]
        while True:
            try:
                next_version_obj = next(v for v in version_obj.children)
                if (version_obj, next_version_obj) not in self.edges:
                    raise RuntimeError(
                        f"Edge({version_obj.vid},{next_version_obj.vid}) "
                        f"expected but not found.")
                yield self.edges[(version_obj, next_version_obj)]
                version_obj = next_version_obj
            except StopIteration:
                break
        self.logger.info(f"Completed CHECKOUT till {version_obj}.")
        self.confirm_fetch(node, version_obj.vid)

    def get_edges_to_root(self, version_obj):
        version = self.choose_alias(version_obj.vid)
        if version not in self.versions:
            raise RuntimeError(f"Version {version} not found.")
        version_obj = self.versions[version]
        while True:
            try:
                prev_version_obj = next(v for v in version_obj.parents)
                if (prev_version_obj, version_obj) not in self.edges:
                    raise RuntimeError(
                        f"Edge({prev_version_obj.vid},{version_obj.vid}) "
                        f"expected but not found.")
                yield self.edges[(prev_version_obj, version_obj)]
                version_obj = prev_version_obj
            except StopIteration:
                break

    def confirm_fetch(self, node, vid):
        vid = self.choose_alias(vid)
        if node in self.node_to_version:
            if self._clean_refs_if_later(node, self.versions[vid]):
                self._add_ref(node, self.versions[vid])
        else:
            self._add_ref(node, self.versions[vid])

    @rlock
    def _delete_old_reference(self, nodename, tid):
        old_nodename = "O-{0}-{1}".format(nodename, tid)
        if old_nodename in self.node_to_version:
            version = self.node_to_version[old_nodename]
            if (version in self.version_to_node
                    and old_nodename in self.version_to_node[version]):
                self.version_to_node[version].remove(old_nodename)
                # Do not do this. It will automatically be cleaned up by
                # GC when the version is deleted.
                # Keeping it requires us to promote the rlock to a wlock
                # and this function is called even on reads. So NO!
                # if not self.version_to_node[version]:
                #     del self.version_to_node[version]
            del self.node_to_version[old_nodename]
            self.logger.info(f"Deleting reference to {old_nodename}")
        self.confirm_to_gc = True

    @rlock
    def get_confirmed(self, nodename):
        confirmed = list()
        if nodename in self.node_to_confirmed:
            confirmed = self.node_to_confirmed[nodename]
            self.node_to_confirmed[nodename] = list()
        return confirmed
    # def _partial_update_refs(self, node, version):
    #     partial_nodename = "P-{0}-{1}".format(self.nodename, node)
    #     self.version_to_node.setdefault(version, set()).add(partial_nodename)
    #     self.node_to_version[partial_nodename] = version

    def _update_refs(self, remote_refs):
        for rnode, vid in remote_refs.items():
            self.confirm_fetch(rnode, vid)

    def _clean_refs_if_later(self, rnode, version):
        prev_version = self.node_to_version[rnode]
        if (prev_version == version
                or prev_version.creation_time >= version.creation_time):
            return False
        # This means that the reference is new.
        # Need to update it.
        if (prev_version in self.version_to_node
                and rnode in self.version_to_node[prev_version]):
            self.version_to_node[prev_version].remove(rnode)
            if not self.version_to_node[prev_version]:
                del self.version_to_node[prev_version]
        return True

    def _add_ref(self, node, version):
        self.node_to_version[node] = version
        self.version_to_node.setdefault(version, set()).add(node)

    def wait_for_change(self, versions, timeout):
        success = True
        with self.graph_change_event:
            success = self.graph_change_event.wait_for(
                lambda: any(
                    self.versions[version].children
                    for version in versions
                    if version in self.versions),
                timeout if timeout > 0 else None)
        if not success:
            raise TimeoutError(
                "No new version received in time {0}".format(timeout))
