from multiprocessing import RLock


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

    def continue_chain(self, from_version, to_version, package):
        from_version_node = self.nodes.setdefault(
            from_version, Node(from_version, from_version == self.head.current))
        
        to_version_node = self.nodes.setdefault(
            to_version, Node(to_version, from_version == self.head.current))
        to_version_node.set_prev(from_version)

        edge = Edge(from_version, to_version, package)
        self.edges[(from_version, to_version)] = edge

        from_version_node.set_next(to_version)
        if from_version == self.head.current:
            self.head = to_version_node
        
    def delete_item(self, version):
        pass

    def maintain(self, state_to_ref, merger_function):
        mark_for_delete = set()
        # Pass 1 for all objects that are not in master line.
        for version, node in self.nodes.items():
            if len(node.all_next) > 1 or len(node.all_prev) > 1:
                # Anything that has a fork or a join on it, cannot be deleted
                # until all forks/joins are deleted.
                continue
            if not node.is_master:
                if version in state_to_ref and state_to_ref[version]:
                    # Node is still marked.
                    continue
                # Node can be deleted without shortcutting.
                mark_for_delete.add(node)
        # Clean up non master line.
        for node in mark_for_delete:
            del self.nodes[node.current]
            if (node.prev_master, node.current) in self.edges:
                del self.edges[(node.prev_master, node.current)]
            if (node.current, node.next_master) in self.edges:
                del self.edges[(node.current, node.next_master)]
            if node.prev_master:
                self.nodes[node.prev_master].all_next.remove(node.current)
            if node.next_master:
                self.nodes[node.next_master].all_prev.remove(node.current)
        # Pass 2 for all objects that are in the master line and can be merged.
        mark_for_merge = set()
        for version, node in self.nodes.items():
            if node == self.head or node == self.tail:
                continue
            if len(node.all_next) > 1 or len(node.all_prev) > 1:
                # Anything that has a fork or a join on it, cannot be deleted
                # until all forks/joins are deleted.
                continue
            if node.is_master:
                if version in state_to_ref and state_to_ref[version]:
                    # Node is still marked.
                    continue
                # Node can be merged with shortcutting.
                mark_for_merge.add(node)
        # Merge up master line.
        for node in mark_for_merge:
            del self.nodes[node.current]
            old_change = (
                self.edges[(node.prev_master, node.current)].payload
                if (node.prev_master, node.current) in self.edges else
                dict())
            new_change = (
                self.edges[(node.current, node.next_master)].payload
                if (node.current, node.next_master) in self.edges else
                dict())
            new_payload = merger_function(old_change, new_change)
            if (node.prev_master, node.current) in self.edges:
                del self.edges[(node.prev_master, node.current)]
            if (node.current, node.next_master) in self.edges:
                del self.edges[(node.current, node.next_master)]
            self.nodes[node.prev_master].all_next.remove(node.current)
            self.nodes[node.next_master].all_prev.remove(node.current)

            self.edges[(node.prev_master, node.next_master)] = Edge(
                node.prev_master, node.next_master, new_payload)
            self.nodes[node.prev_master].next_master = node.next_master
            self.nodes[node.prev_master].all_next.add(node.next_master)
            self.nodes[node.next_master].prev_master = node.prev_master
            self.nodes[node.next_master].all_prev.add(node.prev_master)
