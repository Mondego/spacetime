import unittest
from spacetime.version_graph import VersionGraph, Edge, Version
from spacetime.heap import Heap
from rtypes import dimension, primarykey, pcc_set
from spacetime.utils.enums import Event
from rtypes.utils.enums import Datatype


@pcc_set
class BasicTestType():
    oid = primarykey(int)
    dim1 = dimension(str)

    def __init__(self, oid, dim1):
        self.oid = oid
        self.dim1 = dim1


@pcc_set
class BasicTestType2():
    oid = primarykey(int)
    dim1 = dimension(str)
    dim2 = dimension(str)

    def __init__(self, oid, dim1, dim2):
        self.oid = oid
        self.dim1 = dim1
        self.dim2 = dim2


class TestHeap(unittest.TestCase):
    def check_graph(self, version_graph, test_edges):
        versions = dict()
        edges = dict()
        forward_edges = dict()
        for from_vid, to_vid, delta, eid in test_edges:
            from_v = versions.setdefault(from_vid, Version(from_vid))
            to_v = versions.setdefault(to_vid, Version(to_vid))
            from_v.add_child(to_v)
            edges[(from_v, to_v)] = Edge(from_v, to_v, delta, eid)
            forward_edges[(from_v, eid)] = to_v

        self.assertDictEqual(version_graph.versions, versions)
        self.assertDictEqual(version_graph.edges, edges)
        self.assertDictEqual(version_graph.forward_edge_map, forward_edges)

    def test_basic_heap(self):
        version_graph = VersionGraph("TEST", {BasicTestType}, dict())
        heap1 = Heap("H1", {BasicTestType}, version_graph)
        heap2 = Heap("H2", {BasicTestType}, version_graph)
        heap1.add(BasicTestType, [BasicTestType(0, "test0")])
        self.assertDictEqual(
            heap1.data,
            {
                BasicTestType.__r_meta__.name: {
                    0: {
                        "oid": 0,
                        "dim1": "test0"
                    }
                }
            })
        diff1 = {
            BasicTestType.__r_meta__.name: {
                0: {
                    "dims": {
                        "oid": {"type": Datatype.INTEGER, "value": 0},
                        "dim1": {"type": Datatype.STRING, "value": "test0"}
                    }, "types": {
                        BasicTestType.__r_meta__.name: Event.New
                    }
                }
            }
        }

        self.assertDictEqual(heap1.diff, diff1)
        end_v = heap1.diff.version
        heap1.commit()
        self.check_graph(version_graph, [("ROOT", end_v, diff1, end_v)])
        self.assertEqual(heap2.version, "ROOT")
        self.assertDictEqual(heap2.data, {BasicTestType.__r_meta__.name: {}})
        heap2.checkout()
        self.assertDictEqual(
            heap2.data,
            {
                BasicTestType.__r_meta__.name: {
                    0: {
                        "oid": 0,
                        "dim1": "test0"
                    }
                }
            })
        self.assertDictEqual(
            heap1.data,
            {
                BasicTestType.__r_meta__.name: {
                    0: {
                        "oid": 0,
                        "dim1": "test0"
                    }
                }
            })
        self.assertDictEqual(heap1.diff, dict())
        self.assertDictEqual(heap2.diff, dict())

    def test_heap_with_basic_merge(self):
        version_graph = VersionGraph("TEST", {BasicTestType}, dict())
        heap1 = Heap("H1", {BasicTestType}, version_graph)
        heap2 = Heap("H2", {BasicTestType}, version_graph)
        heap1.add(BasicTestType, [BasicTestType(0, "test0")])
        heap2.add(BasicTestType, [BasicTestType(1, "test1")])
        self.assertDictEqual(
            heap1.data,
            {
                BasicTestType.__r_meta__.name: {
                    0: {
                        "oid": 0,
                        "dim1": "test0"
                    }
                }
            })
        self.assertDictEqual(
            heap2.data,
            {
                BasicTestType.__r_meta__.name: {
                    1: {
                        "oid": 1,
                        "dim1": "test1"
                    }
                }
            })
        diff1 = {
            BasicTestType.__r_meta__.name: {
                0: {
                    "dims": {
                        "oid": {"type": Datatype.INTEGER, "value": 0},
                        "dim1": {"type": Datatype.STRING, "value": "test0"}
                    }, "types": {
                        BasicTestType.__r_meta__.name: Event.New
                    }
                }
            }
        }
        diff2 = {
            BasicTestType.__r_meta__.name: {
                1: {
                    "dims": {
                        "oid": {"type": Datatype.INTEGER, "value": 1},
                        "dim1": {"type": Datatype.STRING, "value": "test1"}
                    }, "types": {
                        BasicTestType.__r_meta__.name: Event.New
                    }
                }
            }
        }

        self.assertDictEqual(heap1.diff, diff1)
        self.assertDictEqual(heap2.diff, diff2)
        end_v1 = heap1.diff.version
        end_v2 = heap2.diff.version
        heap1.commit()
        self.check_graph(version_graph, [("ROOT", end_v1, diff1, end_v1)])
        heap2.commit()
        self.assertEqual(
            len([v for v in version_graph.versions.values() if not v.children]),
            1)
        end_v = next(
            v.vid for v in version_graph.versions.values() if not v.children)
        self.check_graph(
            version_graph,
            [("ROOT", end_v1, diff1, end_v1),
             ("ROOT", end_v2, diff2, end_v2),
             (end_v1, end_v, diff2, end_v2),
             (end_v2, end_v, diff1, end_v1)])

        self.assertDictEqual(heap1.diff, dict())
        self.assertDictEqual(heap2.diff, dict())
