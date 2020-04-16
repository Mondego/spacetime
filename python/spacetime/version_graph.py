from uuid import uuid4
from collections import deque, OrderedDict
from threading import Condition

from spacetime.remote import Remote
from spacetime.utils.enums import Event
from spacetime.utils import utils
from spacetime.utils.rwlock import RWLockFair as RWLock
from rtypes.utils.enums import DiffType
from rtypes.utils.converter import unconvert, convert

def rlock(func):
    def read_locked_func(self, *args, **kwargs):
        with self.rwlock.gen_rlock():
            return func(self, *args, **kwargs)
    return read_locked_func

def wlock(func):
    def write_locked_func(self, *args, **kwargs):
        with self.rwlock.gen_wlock():
            return func(self, *args, **kwargs)
    return write_locked_func
    

class Version(object):
    def __repr__(self):
        return self.vid

    def __eq__(self, version):
        return (
            self.vid == version.vid
            and ({c.vid for c in self.children}
                 == {c.vid for c in version.children})
            and ({p.vid for p in self.parents}
                 == {p.vid for p in version.parents}))

    def __hash__(self):
        return hash(self.vid)

    def __init__(self, vid, children=None, parents=None):
        self.vid = vid
        self.children = children if children else set()
        self.parents = parents if parents else set()


class Edge(object):
    def __repr__(self):
        return " ".join((self.eid, self.from_v, self.to_v))

    def __eq__(self, edge):
        return (
            self.eid == edge.eid
            and self.from_v == edge.from_v
            and self.to_v == edge.to_v
            and self.delta == edge.delta)

    def __hash__(self):
        return hash((self.eid, self.from_v))

    def __init__(self, from_v, to_v, delta, eid):
        self.eid = eid
        self.from_v = from_v
        self.to_v = to_v
        self.delta = delta


class VersionGraph(object):
    def __init__(self, nodename, types, resolver=None, log_to_std=False):
        self.logger = utils.get_logger(f"version_graph_{nodename}", log_to_std)
        self.nodes = {
            nodename: ("ROOT", "ROOT")
        }

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
        self.tail = self.head = Version("ROOT")
        # vid -> version obj
        self.versions = {"ROOT": self.head}
        # (from_vobj, to_vobj) -> edge obj
        self.edges = dict()

        # vid -> vobj
        self.alias = dict()

        # Supporting roles
        self.resolver = resolver
       
        self.forward_edge_map = dict()
        self.graph_change_event = Condition()
        self.rwlock = RWLock()
        
        # GC stuff
        self.node_to_eid = dict()
        self.eids = set()
        self.state_to_node = dict()
        self.node_to_version = dict()

    def _add_single_edge(self, from_v, to_v, delta, eid):
        self.edges[(from_v, to_v)] = Edge(from_v, to_v, delta, eid)
        self.forward_edge_map[(from_v, eid)] = to_v
        from_v.children.add(to_v)
        to_v.parents.add(from_v)

    def _add_edges(self, edges):
        newly_added = set()
        to_be_processed = list(edges)
        prev_unprocessed_count = len(to_be_processed)
        while to_be_processed:
            unprocessed_count = 0
            next_tbp = list()
            for from_vid, to_vid, delta, eid in to_be_processed:
                from_v, to_v = self._resolve_versions(from_vid, to_vid, eid)
                if from_v.vid not in self.versions:
                    next_tbp.append((from_vid, to_vid, delta, eid))
                    unprocessed_count += 1
                    continue

                if to_v.vid not in self.versions:
                    self.versions[to_v.vid] = to_v
                    newly_added.add(to_v)

                if (from_v, to_v) not in self.edges:
                    self._add_single_edge(from_v, to_v, delta, eid)
            to_be_processed = next_tbp
            if unprocessed_count == prev_unprocessed_count:
                raise RuntimeError("Cannot add some edges", next_tbp)
            prev_unprocessed_count = unprocessed_count
        return newly_added

    def _resolve_versions(self, from_vid, to_vid, eid):
        from_vid = self.choose_alias(from_vid)

        from_v = (
            self.versions[from_vid]
            if from_vid in self.versions else
            Version(from_vid))

        to_vid = self.choose_alias(to_vid)

        if to_vid in self.versions:
            return from_v, self.versions[to_vid]

        if (from_v, eid) in self.forward_edge_map:
            existing_to_v = self.forward_edge_map[(from_v, eid)]
            self.alias[to_vid] = existing_to_v.vid
            return from_v, existing_to_v
        return from_v, Version(to_vid)

    def _complete_graph(self, newly_added):
        head = self.head
        while newly_added:
            merge_versions = list()
            for version in newly_added:
                for parent in version.parents:
                    if parent.children == {version}:
                        continue
                    if any(sib.children.intersection(version.children)
                           for sib in parent.children if sib != version):
                        continue
                    sib = next(sib for sib in parent.children if sib != version)
                    merge_versions.append(self._merge(parent, sib, version))
                if not version.children:
                    head = version
            newly_added = merge_versions
        return head

    def _merge(self, parent, sibling, version):
        new_v = Version(str(uuid4()))
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
                current_merge[tpname] = your_tp_merge
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
                current_merge[oid] = your_obj_merge
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
        dimmap = self.tp_to_dim [dtype.__r_meta__.name]
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

    @rlock
    def get(self, nodename, versions):
        self.logger.info(f"Get request: (FROM) {versions}")
        req_versions = {
            self.versions[self.choose_alias(from_vid)] for from_vid in versions}
        if self.head in req_versions:
            return list(), self.head.vid

        to_see = deque([self.head])
        edges = list()

        while to_see:
            version = to_see.popleft()
            for parent in version.parents:
                edge = self.edges[(parent, version)]
                edges.append(
                    (parent.vid, version.vid, edge.delta, edge.eid))
                if parent not in req_versions:
                    to_see.append(parent)
        # By reversing it, the adding of nodes is more efficient.
        edges.reverse()
        # self.logger.info(
        #     f"Get request: (RESPONSE) "
        #     f"{', '.join(f'{f[:4]}->{t[:4]}' for f,t,_,_ in edges)}")
        self.update_refs_as_get(nodename, self.head)
        return edges, self.head.vid

    @wlock
    def put(self, nodename, edges):
        # self.logger.info(
        #     f"Put request: "
        #     f"{', '.join(f'{f[:4]}->{t[:4]}' for f,t,_,_ in edges)}")
        self.logger.info(f"Put request: {len(edges)}")
        head = self._complete_graph(self._add_edges(edges))
        self.update_refs_as_put(nodename, head)
        self.head = head
        # self.head = self.garbage_collect(head)
        return self.head


    def garbage_collect(self, head):
        to_see = deque([self.tail])
        # A map of eid -> set of eids that it depends on.
        # The first occurence of eid is used to determine the set
        # as this is the definitive set.

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
                parent_eids.update(all_eids_in_version[parent])
                if edge_id not in eid_to_parent:
                    eid_to_parent[edge_id] = incoming_eids[parent]
            incoming_eids[version] = eids_to_version
            all_eids_in_version[version] = parent_eids.union(eids_to_version)
            to_see.extend(version.children)
        
        eid_missing_to_nodes = dict()
        seen_by = dict()
        for node in self.node_to_version:
            seen = all_eids_in_version[self.node_to_version[node]["READ"]].union(
                all_eids_in_version[self.node_to_version[node]["WRITE"]])
            seen_by[node] = seen
            for eid in (all_eids - seen):
                eid_missing_to_nodes.setdefault(eid, list()).append(node)
        
        # groups = set()
        # eid_to_group = dict()
        # eids_to_check = set()
        # for eid, prev_eids in eid_parent:
            






        '''
        edges_to_add = set()
        versions_to_delete = set()
        to_see = deque([self.tail])
        while to_see:
            version = to_see.popleft()
            incoming_needed = {tuple(eid_missing_to_nodes.setdefault(self.edges[(parent, version)].eid, tuple())) for parent in version.parents}
            outgoing_needed = {tuple(eid_missing_to_nodes.setdefault(self.edges[(version, children)].eid, tuple())) for children in version.children}
            if len(incoming_needed)+len(outgoing_needed) != len(incoming_needed.union(outgoing_needed)):
                versions_to_delete = version
        '''                
             
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
        self.update_refs_as_get(node, version_obj)

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

    def confirm_fetch(self, node, head):
        self.update_refs_as_confirm_get(node, self.versions[head])

    def update_refs_as_get(self, node, version):
        self.state_to_node.setdefault(version, set()).add(node)

    def update_refs_as_put(self, node, head):
        prev_write = self.versions["ROOT"]
        if node in self.node_to_state:
            prev_write = self.node_to_version[node]["WRITE"]
        else:
            self.node_to_state[node] = {
                "READ": self.versions["ROOT"], "WRITE": self.versions["ROOT"]}
        self.node_to_state[node]["WRITE"] = head
        self.state_to_node.setdefault(head, set())
        if (prev_write in self.state_to_node
                and prev_write in self.state_to_node[prev_write]):
            self.state_to_node[prev_write].remove(prev_write)

    def update_refs_as_confirm_get(self, node, version):
        prev_read = self.versions["ROOT"]
        if node in self.node_to_state:
            prev_read = self.node_to_version[node]["READ"]
        else:
            self.node_to_state[node] = {
                "READ": self.versions["ROOT"], "WRITE": self.versions["ROOT"]}

        self.node_to_state[node]["READ"] = version
        if (prev_read != self.node_to_state[node]["WRITE"]
                and prev_read in self.state_to_node
                and node in self.state_to_node[prev_read]):
            self.state_to_node[prev_read].remove(node)

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
                "No new version received in time {0}".format(
                    timeout))
