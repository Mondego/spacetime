from spacetime.utils.rwlock import RWLockWrite as RWLock


class Node(object):
    def __eq__(self, other):
        return other.current == self.current

    def __hash__(self):
        return hash(self.current)

    def __init__(self, current, is_master):
        self.current = current
        self.prev_master = None
        self.next_master = None
        self.all_prev = set()
        self.all_next = set()
        self.is_master = is_master
        self.write_lock = RWLock()

    def set_next(self, version):
        if self.next_master is None:
            self.next_master = version
        self.all_next.add(version)

    def set_prev(self, version):
        if self.prev_master is None:
            self.prev_master = version
        self.all_prev.add(version)


class Edge(object):
    def __init__(self, from_node, to_node, payload):
        self.from_node = from_node
        self.to_node = to_node
        self.payload = payload

class Graph(object):
    def __init__(self):
        self.tail = Node("ROOT", True)
        self.head = self.tail
        self.nodes = {"ROOT": self.tail}
        self.edges = dict()

    def __getitem__(self, key):
        if not isinstance(key, slice):
            raise RuntimeError("Cannot deal with non slice operators.")
        step_reverse = ((key.step is not None) and (key.step < 0))

        if key.start:
            start = self.nodes[key.start]
        elif step_reverse:
            start = self.head
        else:
            start = self.tail

        if key.stop:
            end, endlock = self._get_node(key.stop)
        elif step_reverse:
            end, endlock = self._get_tail()
        else:
            end, endlock = self._get_head()

        if start == end:
            endlock.release()
            return
        # Start and end are concurrent safe as it cannot be GCed yet.
        current = start
        current_lock = current.write_lock.gen_rlock().acquire()
        while current != end:
            if step_reverse:
                edge, prev_master, prev_lock,  = self._get_prev_edge(current, end)
                current_lock.release()
                current = prev_master
                current_lock = prev_lock
            else:
                edge, next_master, next_lock = self._get_next_edge(current, end)
                current_lock.release()
                if not step_reverse and next_master == end:
                    assert next_lock is None, (current.current, current.next_master, self.head.current, end.current, key.start)

                current_lock = next_lock
                current = next_master
            yield current.current, edge.payload
        if not step_reverse:
            assert current_lock is None, (current.current, self.head.current, end.current, key.start)
        endlock.release()

    def _get_node(self, version):
        while True:
            try:
                lock = self.nodes[version].write_lock.gen_rlock().acquire()
            except KeyError:
                continue
            try:
                return self.nodes[version], lock
            except KeyError:
                # Dammit! Our bad luck we cannot use this edge.
                # Good news is that it has been maintained and hence is better now!.
                # So try again.
                lock.release()
                continue

    def _get_tail(self):
        return self.tail, self.tail.write_lock.gen_rlock().acquire()

    def _get_head(self):
        while True:
            try:
                version = self.head.current
                lock = self.nodes[version].write_lock.gen_rlock().acquire()
            except KeyError:
                continue
            try:
                return self.nodes[version], lock
            except KeyError:
                # Dammit! Our bad luck we cannot use this edge.
                # Good news is that it has been maintained and hence is better now!.
                # So try again.
                lock.release()
                continue

    def _get_prev_edge(self, current, end):
        while True:
            try:
                prev_master = current.prev_master
                if prev_master == end.current:
                    # No need to acquire the lock, its already been acquired.
                    return self.edges[(prev_master, current.current)], self.nodes[prev_master], None
                
                lock = self.nodes[
                    prev_master].write_lock.gen_rlock().acquire()
            except KeyError:
                continue
            try:
                return self.edges[(prev_master, current.current)], self.nodes[prev_master], lock
            except KeyError:
                # Dammit! Our bad luck we cannot use this edge.
                # Good news is that it has been maintained and hence is better now!.
                # So try again.
                lock.release()
                continue

    def _get_next_edge(self, current, end):
        while True:
            try:
                next_master = current.next_master
                if next_master == end.current:
                    # No need to acquire the lock, its already been acquired.
                    return self.edges[(current.current, next_master)], self.nodes[next_master], None
                lock = self.nodes[next_master].write_lock.gen_rlock().acquire()
            except KeyError:
                continue
            try:
                return self.edges[(current.current, next_master)], self.nodes[next_master], lock
            except KeyError:
                # Dammit! Our bad luck we cannot use this edge.
                # Good news is that it has been maintained and hence is better now!.
                # So try again.
                lock.release()
                continue



    def continue_chain(
            self, from_version, to_version, package, force_branch=False):
        # The only thing that read would need is the head and that is changed
        # last. So this is concurrent safe.
        # Existing nodes and edges are not deleted here.
        from_version_node = self.nodes.setdefault(
            from_version, Node(from_version, from_version == self.head.current))

        to_version_node = self.nodes.setdefault(
            to_version, Node(
                to_version,
                (not force_branch) and from_version == self.head.current))
                
        to_version_node.set_prev(from_version)

        edge = Edge(from_version, to_version, package)
        self.edges[(from_version, to_version)] = edge

        from_version_node.set_next(to_version)
        if from_version_node == self.head:
            self.head = to_version_node
        return

    def delete_item(self, version):
        pass

    def maintain_edges(self):
        edges_to_del = set()
        for edge in self.edges:
            start, end = self.nodes[edge[0]], self.nodes[edge[1]]
            if not start.is_master and not end.is_master:
                # Can delete the edge that took start to the master line.
                # The other application clearly ignored it.
                # Lets delete so that nodes can be merged.
                for node_v in start.all_next:
                    if self.nodes[node_v].is_master and (start.current, node_v) in self.edges:
                        edges_to_del.add((start.current, node_v))
                        start.next_master = end.current
        for start, end in edges_to_del:
            with self.nodes[end].write_lock.gen_wlock():
                del self.edges[(start, end)]
                self.nodes[start].all_next.remove(end)
                self.nodes[end].all_prev.remove(start)


    def merge_node(self, node, merger_function):
        '''Node has to be deleted.'''

        # new_payload is the merged diff that bypasses this node.
        old_change = self.edges[(node.prev_master, node.current)].payload
        new_change = self.edges[(node.current, node.next_master)].payload
        new_payload = merger_function(old_change, new_change)
        self.nodes[node.prev_master].all_next.remove(node.current)
        self.nodes[node.next_master].all_prev.remove(node.current)
        
        if not (self.nodes[node.prev_master].is_master
                and self.nodes[node.next_master].is_master and not node.is_master):
            # This is not a branch.
            
        
            if (node.prev_master, node.next_master) not in self.edges:
                # We add this edge.
                self.edges[(node.prev_master, node.next_master)] = Edge(
                    node.prev_master, node.next_master, new_payload)
            else:
                # Figure out how to avoid this computation.
                assert self.edges[(node.prev_master,
                                    node.next_master)].payload == new_payload, (
                        self.edges[(node.prev_master, node.next_master)].payload,
                        new_payload)
            
            if self.nodes[node.prev_master].next_master == node.current:
                self.nodes[node.prev_master].next_master = node.next_master
            self.nodes[node.prev_master].all_next.add(node.next_master)
            if self.nodes[node.next_master].prev_master == node.current:
                self.nodes[node.next_master].prev_master = node.prev_master
            self.nodes[node.next_master].all_prev.add(node.prev_master)
        # Else it is a branch.
        # Either way, now that all the edges have been updated clearly.
        # we can delete the edges as any reads will now not use the nodes and 
        # edges that are being deleted. .
        with node.write_lock.gen_wlock():
            del self.edges[(node.prev_master, node.current)]
            del self.nodes[node.current]
            del self.edges[(node.current, node.next_master)]

    def maintain_nodes(self, state_to_ref, merger_function, master):
        mark_for_merge = set()
        mark_for_delete = set()
        for version, node in self.nodes.items():
            if node == self.head or node == self.tail:
                continue
            if version in state_to_ref and state_to_ref[version]:
                # Node is still marked.
                continue
            if len(node.all_next) > 1 or len(node.all_prev) > 1:
                # Node is marked in a branch.
                continue
            if node.is_master == master:
                mark_for_merge.add(node)

        for node in mark_for_merge:
            self.merge_node(node, merger_function)

    def maintain(self, state_to_ref, merger_function):
        # Delete edges that are useless.
        # Concurrency safe as these edges will never be accessed.
        self.maintain_edges()
        # Merge nodes that are chaining without anyone looking at them
        # First divergent.
        self.maintain_nodes(state_to_ref, merger_function, False)
        # The master line.
        self.maintain_nodes(state_to_ref, merger_function, True)
        return
