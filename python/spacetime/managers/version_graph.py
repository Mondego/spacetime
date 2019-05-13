from multiprocessing import RLock
from copy import deepcopy

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
        if isinstance(key, slice):
            step_reverse = ((key.step is not None) and (key.step < 0))
            if key.start:
                start = self.nodes[key.start]
            elif step_reverse:
                start = self.head
            else:
                start = self.tail

            if key.stop:
                end = self.nodes[key.stop]
            elif step_reverse:
                end = self.tail
            else:
                end = self.head
            current = start
            while current != end:
                if step_reverse:
                    edge = self.edges[(current.prev_master, current.current)]
                    current = self.nodes[current.prev_master]
                else:
                    edge = self.edges[(current.current, current.next_master)]
                    current = self.nodes[current.next_master]
                yield edge.payload
            return
        else:
            raise RuntimeError("Cannot deal with non slice operators.")

    def continue_chain(
            self, from_version, to_version, package, force_branch=False):
        from_version_node = self.nodes.setdefault(
            from_version, Node(from_version, from_version == self.head.current))

        to_version_node = self.nodes.setdefault(
            to_version, Node(
                to_version, (not force_branch) and from_version == self.head.current))
        to_version_node.set_prev(from_version)

        edge = Edge(from_version, to_version, package)
        self.edges[(from_version, to_version)] = edge

        from_version_node.set_next(to_version)
        if from_version == self.head.current:
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
            del self.edges[(start, end)]
            self.nodes[start].all_next.remove(end)
            self.nodes[end].all_prev.remove(start)


    def merge_node(self, node, merger_function):
        del self.nodes[node.current]
        old_change = self.edges[(node.prev_master, node.current)].payload
        new_change = self.edges[(node.current, node.next_master)].payload
        new_payload = merger_function(old_change, new_change)

        del self.edges[(node.prev_master, node.current)]
        del self.edges[(node.current, node.next_master)]
        self.nodes[node.prev_master].all_next.remove(node.current)
        self.nodes[node.next_master].all_prev.remove(node.current)
        if (self.nodes[node.prev_master].is_master
                and self.nodes[node.next_master].is_master and not node.is_master):
            # This is a branch, that can be deleted.
            return
        if (node.prev_master, node.next_master) not in self.edges:
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
        c = deepcopy(self.nodes), deepcopy(self.edges)
        self.maintain_edges()
        # Merge nodes that are chaining without anyone looking at them
        # First divergent.
        self.maintain_nodes(state_to_ref, merger_function, False)
        # The master line.
        self.maintain_nodes(state_to_ref, merger_function, True)
        if any(n.prev_master is None and n.current != "ROOT" for n in self.nodes.values()):
            self.nodes, self.edges = c
            self.maintain_edges()
            # Merge nodes that are chaining without anyone looking at them
            # First divergent.
            self.maintain_nodes(state_to_ref, merger_function, False)
            # The master line.
            self.maintain_nodes(state_to_ref, merger_function, True)
        return
