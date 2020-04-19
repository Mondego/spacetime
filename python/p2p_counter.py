from multiprocessing import Process, Queue, Empty
from queue import Empty
import time
from threading import Thread

class Heap(object):
    def __init__(self, version, version_graph):
        self.version = version
        self.diff = dict()
        self.version_graph = version_graph

    def write_to_diff(self, key, value):
        self.diff[key] = value
    
    def read_value(self, key):
        if key in self.diff:
            return self.diff[key]

        return self.version_graph.read_value(key)

    def write(self, version):
        self.version = version
        self.diff = dict()

    def read(self):
        new_v = str(uuid.uuid4())
        ret = self.diff, [self.version, new_v]
        self.version = new_v
        self.diff = dict()
        return ret


class Dataframe(Thread):
    def __init__(self, in_q, out_q, remote):
        self.in_q = in_q
        self.out_q = out_q

        self.version_graph = VersionGraph()
        self.heap = Heap("ROOT", self.version_graph)
        self.remote = remote
        super().__init__(daemon=True)
        self.start()

    def checkout(self):
        self.heap.write(
            self.version_graph.flat_read(self.heap.version))

    def add_counter(self, i):
        self.heap.write_to_diff("counter", self.heap.read_value("counter") + i)

    def commit(self):
        self.version_graph.write(self.heap.read())

    def push(self):
        resp = self.version_graph.read(self.remote)
        self.out_q.put(resp)
        self.version_graph.update_refs(self.remote, resp)
    
    def run(self):
        while not self._stop:
            try:
                push_req = self.in_q.get(timeout=1)
                self.version_graph.write(self.remote, push_req)
            except Empty:
                time.sleep(0.001)

    def stop(self):
        self._stop = True


class Version(object):
    def __init__(self, vid):
        self.vid = vid
        self.children = set()
        self.parents = set()


class Edge(object):
    def __init__(self, eid, start, end, delta):
        self.eid = eid
        self.start = start
        self.end = end
        self.delta = delta

class VersionGraph(object):
    def __init__(self):
        self.node_to_version = dict()
        self.version_to_node = dict()
        self.versions = {
            "ROOT": Version("ROOT")
        }

        self.edges = dict()

    def write(self, source, edges):
        prev_count = len(edges) + 1
        created = set()
        while edges:
            edge = edges.pop()
            self.resolve(edge)
            if edge.start not in self.versions:
                edges.append(edge)
                continue
            if edge.end not in self.versions:
                self.versions[edge.end] = Version(edge.end)
                created.add(self.versions[edge.end])
            if (edge.start, edge.end) in self.edges:
                # Assert that they are equivalent edges.
                pass
            self.add_edge(edge)
            self.versions[edge.start].add_child(self.versions[edge.end])
            
        self.merge(created)

    def merge(self, created):
        newly_created = set()
        for new_version in created:
            for parent in new_version.parents:
                if any(len(
                        child.children.intersection(new_version.children) == 1)
                       for child in parent.children if child != new_version):
                    continue
                newly_created.add(self.three_way_merge(
                    parent, new_version, (parent.children - {new_version}).pop()))
        if newly_created:
            self.merge(newly_created)

    def three_way_merge(self, parent, new_version, conf_version):
        self.new_state = self.custom_func(self.)


    def update_refs(self, source, resp):
        pass

    def read(self, source):
        pass

    def flat_read(self, from_version):
        pass


class Counter(Process):
    def __init__(self, in_q, out_q, remote, index, max_c, num_c):
        self.index = index
        self.max_c = max_c
        self.num_c = num_c

        self.dataframe = Dataframe(in_q, out_q, remote)
        super().__init__(daemon=True)
    
    def run(self):
        for i in range(self.index, self.max_c, self.num_c)
            self.dataframe.checkout()
            self.dataframe.add_counter(i)
            self.dataframe.commit()

        self.dataframe.stop()
        self.dataframe.join()
        print(index, self.dataframe.counter)
        


def main():
    p1q = Queue()
    p2q = Queue()
    p1 = Counter(p1q, p2q, "Counter2", 0, 10, 2)
    p2 = Counter(p2q, p1q, "Counter1", 1, 10, 2)
    p1.start()
    p2.start()
    p1.join()
    p2.join()