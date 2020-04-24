import unittest
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from spacetime.version_graph import VersionGraph, Edge, Version
# from networkx.drawing.nx_agraph import write_dot, graphviz_layout
from spacetime.utils.utils import visualize_graph

class TestVersionManaer(unittest.TestCase):




    def assertEdgesEqual(self, vgedges, edges):
        self.assertEqual(len(vgedges), len(edges))
        for key in edges:
            v1_from_v, v1_to_v, v1_delta, v1_eid = edges[key]
            v2_from_v, v2_to_v, v2_delta, v2_eid = vgedges[key]
            self.assertEqual(v1_from_v, v2_from_v)
            self.assertEqual(v1_to_v, v2_to_v)

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
        self.assertEdgesEqual(version_graph.edges, edges)
        # self.assertDictEqual(version_graph.forward_edge_map, forward_edges)

    def test_equality(self):
        root = Version("ROOT")
        self.assertEqual(root, Version("ROOT"))
        v0 = Version("0", parents={root})
        root.children.add(v0)
        self.assertEqual(v0, Version("0", parents={root}))
        self.assertEqual(root, Version("ROOT", children={v0}))
        v1 = Version("1", parents={root})
        root.children.add(v1)
        self.assertEqual(v1, Version("1", parents={root}))
        self.assertEqual(root, Version("ROOT", children={v0, v1}))
        v2 = Version("2", parents={v0, v1})
        v0.children.add(v2)
        v1.children.add(v2)
        self.assertEqual(v0, Version("0", parents={root}, children={v2}))
        self.assertEqual(v1, Version("1", parents={root}, children={v2}))
        self.assertEqual(v2, Version("2", parents={v0, v1}))

    def test_basic_put(self):
        version_graph = VersionGraph("VG", set(), dict())
        version_graph.put(
            {"VG": "0"}, [("ROOT", "0", {"test_key": "test_value"}, "Root-0")])
        self.check_graph(
            version_graph,
            [("ROOT", "0", {"test_key": "test_value"}, "Root-0")])

    # @pytest.mark.skip(reason="Graph/nodes haven't been setup right")
    # def test_selection256_put(self):
    #     version_graph = VersionGraph("VG", set(), dict())
    #     version_graph.put(
    #         "VG1", "B", [("ROOT", "B", {"test_key": "test_value"}, "Root-B")])
    #     version_graph.put(
    #         "VG2", "C", [("ROOT", "C", {"testkey": "testvalue"}, "Root-C")])
    #     self.assertEqual(
    #         len([v for v in version_graph.versions.values() if not v.children]),
    #         1)  # ensure only 1 head
    #     version_D = next(
    #         v.vid for v in version_graph.versions.values() if not v.children)
    #     self.check_graph(
    #         version_graph,
    #         [("ROOT", "B", {"test_key": "test_value"}, "Root-B"),
    #          ("ROOT", "C", {"testkey": "testvalue"}, "Root-C"),
    #          ("C", version_D, {"test_key": "test_value"}, "Root-B"),
    #          ("B", version_D, {"testkey": "testvalue"}, "Root-C")])
    #     # A -> (B,C) -> D done
    #
    #     version_graph.put(
    #         "VG1", "E", [("B", "E", {"test_key2": "test_value2"}, "B-E")]
    #     )
    #
    #     self.assertEqual(
    #         len([v for v in version_graph.versions.values() if not v.children]),
    #         1)  # ensure only 1 head
    #     version_G = next(
    #         v.vid for v in version_graph.versions.values() if not v.children)
    #
    #     version_graph.put(
    #         "VG2", "F", [("C", "F", {"testkey3": "testvalue3"}, "C-F")])
    #
    #     self.assertEqual(
    #         len([v for v in version_graph.versions.values() if not v.children]),
    #         1)  # ensure only 1 head
    #     version_I = next(
    #         v.vid for v in version_graph.versions.values() if not v.children)
    #     print(version_I)

    def test_basic_put_continue(self):
        version_graph = VersionGraph("VG", set(), dict())
        version_graph.put(
            {"VG": "0"}, [("ROOT", "0", {"test_key": "test_value"}, "Root-0")])
        version_graph.put(
            {"VG": "1"}, [("0", "1", {"testkey": "testvalue"}, "0-1")])
        self.check_graph(
            version_graph,
            [("ROOT", "1", {"test_key": "testvalue"}, "Root-1")])

    def test_merge_put_simple(self):
        version_graph = VersionGraph("VG", set(), dict())
        version_graph.put(
            {"VG0": "0"}, [("ROOT", "0", {"test_key": "test_value"}, "Root-0")])
        version_graph.put(
            {"VG1": "1"}, [("ROOT", "1", {"testkey": "testvalue"}, "Root-1")])
        self.assertEqual(
            len([v for v in version_graph.versions.values() if not v.children]),
            1)  # ensure only 1 head
        end_v = next(
            v.vid for v in version_graph.versions.values() if not v.children)
        # self.visualize_graph(version_graph)
        self.check_graph(
            version_graph,
            [("ROOT", "0", {"test_key": "test_value"}, "Root-0"),
             ("ROOT", "1", {"testkey": "testvalue"}, "Root-1"),
             ("1", end_v, {"test_key": "test_value"}, "Root-0"),
             ("0", end_v, {"testkey": "testvalue"}, "Root-1")])

    def test_merge_put_two_levels(self):
        version_graph = VersionGraph("VG", set(), dict())
        version_graph.put(
            {"VG0": "0"}, [("ROOT", "0", {"testkey0": "testvalue0"}, "Root-0")])
        version_graph.put(
            {"VG1": "1"}, [("0", "1", {"testkey1": "testvalue1"}, "0-1")])
        version_graph.put(
            {"VG2": "2"}, [("ROOT", "2", {"testkey2": "testvalue2"}, "Root-2")])
        self.assertEqual(
            len([v for v in version_graph.versions.values() if not v.children]),
            1)
        merge2_v = next(
            v.vid for v in version_graph.versions.values() if not v.children)
        self.assertEqual(
            len(version_graph.versions["0"].children.intersection(
                version_graph.versions["2"].children)), 1)
        merge1_v = version_graph.versions["0"].children.intersection(
            version_graph.versions["2"].children).pop().vid
        # visualize_graph(version_graph)
        self.check_graph(
            version_graph,
            [("ROOT", "0", {"testkey0": "testvalue0"}, "Root-0"),
             ("0", "1", {"testkey1": "testvalue1"}, "0-1"),
             ("ROOT", "2", {"testkey2": "testvalue2"}, "Root-2"),
             ("0", merge1_v, {"testkey2": "testvalue2"}, "Root-2"),
             ("2", merge1_v, {"testkey0": "testvalue0"}, "Root-0"),
             ("1", merge2_v, {"testkey2": "testvalue2"}, "Root-2"),
             (merge1_v, merge2_v, {"testkey1": "testvalue1"}, "0-1")])

    def test_get_simple(self):
        version_graph = VersionGraph("VG", set(), dict())
        version_graph.put({"VG0": "0"}, [("ROOT", "0", "data(R->0)", "Root-0")])
        version_graph.put({"VG1": "1"}, [("0", "1", "data(0->1)", "0-1")])
        edges, head1, refs = version_graph.get("VG", {"ROOT"})
        self.assertEqual("1", head1)
        self.assertSetEqual(
            {("ROOT", "0", "data(R->0)", "Root-0"),
             ("0", "1", "data(0->1)", "0-1")}, set(edges))
        edges, head2, refs = version_graph.get("VG", {"0"})
        self.assertEqual("1", head2)
        self.assertSetEqual(
            {("0", "1", "data(0->1)", "0-1")}, set(edges))

    def test_get_branched(self):
        data1 = {
            "tp1": "data(R->0)",
        }
        data2 = {
            "tp2": "data(R->1)"
        }
        version_graph = VersionGraph("VG", set(), dict())
        version_graph.put({"VG0": "0"}, [("ROOT", "0", data1, "Root-0")])
        version_graph.put({"VG1": "1"}, [("ROOT", "1", data2, "Root-1")])
        self.assertEqual(
            len([v for v in version_graph.versions.values() if not v.children]),
            1)
        end_v = next(
            v.vid for v in version_graph.versions.values() if not v.children)
        edges, head1, refs = version_graph.get("VG", {"ROOT"})
        self.assertEqual(end_v, head1)
        # self.visualize_graph((version_graph))
        self.assertDictEqual(
            {("ROOT", "0"): (data1, "Root-0"),
             ("ROOT", "1"): (data2, "Root-1"),
             ("0", end_v): (data2, "Root-1"),
             ("1", end_v): (data1, "Root-0")},
            {(f, t): (d, i) for f, t, d, i in edges})
        edges, head2, refs = version_graph.get("VG", {"0", "1"})
        self.assertEqual(end_v, head2)
        self.assertDictEqual(
            {("0", end_v): (data2, "Root-1"),
             ("1", end_v): (data1, "Root-0")},
            {(f, t): (d, i) for f, t, d, i in edges})

    def test_p2p_merge(self):
        data1 = {
            "tp1": "data(R->0)",
        }
        data2 = {
            "tp2": "data(R->1)"
        }
        data3 = {
            "tp3": "data(M(0,1)->2)"
        }
        vg1 = VersionGraph("vg1", set(), dict())
        vg2 = VersionGraph("vg2", set(), dict())
        vg1.put({"vg10": "0"}, [("ROOT", "0", data1, "Root-0")])
        vg1.put({"vg11": "1"}, [("ROOT", "1", data2, "Root-1")])

        vg2.put({"vg20": "1"}, [("ROOT", "1", data2, "Root-1")])
        vg2.put({"vg21": "0"}, [("ROOT", "0", data1, "Root-0")])

        self.assertEqual(
            len([v for v in vg1.versions.values() if not v.children]), 1)
        self.assertEqual(
            len([v for v in vg2.versions.values() if not v.children]), 1)
        m01_vg1 = next(
            v.vid for v in vg1.versions.values() if not v.children)
        m01_vg2 = next(
            v.vid for v in vg2.versions.values() if not v.children)
        self.assertNotEqual(m01_vg1, m01_vg2)

        vg1.put({"vg13": "2"}, [(m01_vg1, "2", data3, "M(0,1)-2")])
        self.assertEqual(
            len([v for v in vg1.versions.values() if not v.children]), 1)
        edges, head_vg1, refs = vg1.get("vg1", {"0", "1"})
        self.assertEqual(head_vg1, "2")
        self.assertDictEqual(
            {("0", m01_vg1): (data2, "Root-1"),
             ("1", m01_vg1): (data1, "Root-0"),
             (m01_vg1, "2"): (data3, "M(0,1)-2")},
            {(f, t): (d, i) for f, t, d, i in edges})
        vg2.put({"vg23": head_vg1}, edges)

        self.check_graph(
            vg2,
            [("ROOT", "0", data1, "Root-0"),
             ("ROOT", "1", data2, "Root-1"),
             ("0", m01_vg2, data2, "Root-1"),
             ("1", m01_vg2, data1, "Root-0"),
             (m01_vg2, "2", data3, "M(0,1)-2")])
        self.assertEqual(vg2.alias[m01_vg1], m01_vg2)

    def test_get_with_two_versions(self):
        version_graph = VersionGraph("VG", set(), dict())
        version_graph.put({"VG0": "0"}, [("ROOT", "0", "data(R->0)", "Root-0")])
        version_graph.put({"VG1": "1"}, [("0", "1", "data(0->1)", "0-1")])
        version_graph.put({"VG2": "2"}, [("1", "2", "data(1->2)", "1-2")])
        edges, head1, refs = version_graph.get("VG", {"ROOT", "1"})
        self.assertEqual("2", head1)
        self.assertSetEqual(
            {("1", "2", "data(1->2)", "1-2")}, set(edges))

    def test_obtained_error_case1(self):
        vg = VersionGraph("VG", set(), dict())
        vg.put(
            {"rp1": "4", "p2": "6", "p1": "7", "wp1": "7"},
            [("ROOT", "0", {}, "0"), ("0", "1", {}, "1"),
             ("1", "2", {}, "2"), ("1", "3", {}, "3"),
             ("2", "4", {}, "3"), ("3", "4", {}, "2"),
             ("3", "5", {}, "5"), ("4", "6", {}, "5"),
             ("5", "6", {}, "2"), ("5", "7", {}, "7"),
             ("6", "8", {}, "7"), ("7", "8", {}, "2")])
        self.assertSetEqual(
            set({(v1.vid, v2.vid) for v1, v2 in vg.edges.keys()}),
            {("ROOT", "3"), ("3", "4"), ("3", "5"), ("4", "6"),
             ("5", "6"), ("5", "7"), ("6", "8"), ("7", "8")})

    def test_obtained_error_case2(self):
        vg = VersionGraph("VG", set(), dict())
        vg.put(
            {"rp1": "4", "p2": "6", "p1": "7", "wp1": "7", "wp2": "0"},
            [("ROOT", "0", {}, "0"), ("0", "1", {}, "1"),
             ("1", "2", {}, "2"), ("1", "3", {}, "3"),
             ("2", "4", {}, "3"), ("3", "4", {}, "2"),
             ("3", "5", {}, "5"), ("4", "6", {}, "5"),
             ("5", "6", {}, "2"), ("5", "7", {}, "7"),
             ("6", "8", {}, "7"), ("7", "8", {}, "2")])
        self.assertSetEqual(
            set({(v1.vid, v2.vid) for v1, v2 in vg.edges.keys()}),
            {("ROOT", "0"), ("0", "3"), ("3", "4"), ("3", "5"), ("4", "6"),
             ("5", "6"), ("5", "7"), ("6", "8"), ("7", "8")})