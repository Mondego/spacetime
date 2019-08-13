from uuid import uuid4
import unittest

from spacetime.managers.version_graph import Graph
from spacetime.utils.utils import merge_state_delta

class TestGraph(unittest.TestCase):
    def test_graph_init(self):
        graph = Graph()
        ROOT = graph.tail
        self.assertEqual("ROOT", ROOT.current)
        self.assertEqual(None, ROOT.prev_master)
        self.assertEqual(None, ROOT.next_master)
        self.assertSetEqual(set(), ROOT.all_prev)
        self.assertSetEqual(set(), ROOT.all_next)

        self.assertDictEqual({"ROOT": ROOT}, graph.nodes)
        self.assertDictEqual(dict(), graph.edges)
        self.assertEqual(ROOT, graph.head)

    def test_graph_continue_chain(self):
        graph = Graph()
        ROOT = graph.tail
        version1 = "1"
        payload1 = {"test": 0}
        # Continue the graph
        graph.continue_chain("ROOT", version1, payload1)
        # Make sure root did not change, except to get a next master version.
        self.assertEqual("ROOT", ROOT.current)
        self.assertEqual(None, ROOT.prev_master)
        self.assertEqual(version1, ROOT.next_master)
        self.assertSetEqual(set(), ROOT.all_prev)
        self.assertSetEqual(set(version1), ROOT.all_next)

        # Check the new node that was added.
        first_node = graph.nodes[version1]
        self.assertEqual(version1, first_node.current)
        self.assertEqual("ROOT", first_node.prev_master)
        self.assertEqual(None, first_node.next_master)
        self.assertSetEqual(set(["ROOT"]), first_node.all_prev)
        self.assertSetEqual(set(), first_node.all_next)

        # Check that the node list is correct.
        self.assertDictEqual({"ROOT": ROOT, version1: first_node}, graph.nodes)

        # Check the new edge that was created.
        first_edge = graph.edges[("ROOT", version1)]
        self.assertEqual("ROOT", first_edge.from_node)
        self.assertEqual(version1, first_edge.to_node)
        self.assertEqual(payload1, first_edge.payload)

        # check that the edges list is correct.
        self.assertDictEqual(
            {("ROOT", version1): first_edge}, graph.edges)

        # Check that the rest of the graph is correct.
        self.assertEqual(first_node, graph.head)
        self.assertEqual(ROOT, graph.tail)

        # Add another to see the chain progress.
        version2 = "2"
        payload2 = {"test": 1}
        # Continue the graph
        graph.continue_chain(version1, version2, payload2)
        # Make sure root did not change
        self.assertEqual("ROOT", ROOT.current)
        self.assertEqual(None, ROOT.prev_master)
        self.assertEqual(version1, ROOT.next_master)
        self.assertSetEqual(set(), ROOT.all_prev)
        self.assertSetEqual(set(version1), ROOT.all_next)

        # Check to see that first_node changed.
        self.assertEqual(version1, first_node.current)
        self.assertEqual("ROOT", first_node.prev_master)
        self.assertEqual(version2, first_node.next_master)
        self.assertSetEqual(set(["ROOT"]), first_node.all_prev)
        self.assertSetEqual(set([version2]), first_node.all_next)

        # Check the new node that was added.
        second_node = graph.nodes[version2]
        self.assertEqual(version2, second_node.current)
        self.assertEqual(version1, second_node.prev_master)
        self.assertEqual(None, second_node.next_master)
        self.assertSetEqual(set([version1]), second_node.all_prev)
        self.assertSetEqual(set(), second_node.all_next)

        # Check that the node list is correct.
        self.assertDictEqual({
            "ROOT": ROOT,
            version1: first_node,
            version2: second_node}, graph.nodes)

        # Check that the first_edge is still correct.
        self.assertEqual("ROOT", first_edge.from_node)
        self.assertEqual(version1, first_edge.to_node)
        self.assertEqual(payload1, first_edge.payload)

        # Check the new edge that was created.
        second_edge = graph.edges[(version1, version2)]
        self.assertEqual(version1, second_edge.from_node)
        self.assertEqual(version2, second_edge.to_node)
        self.assertEqual(payload2, second_edge.payload)

        # check that the edges list is correct.
        self.assertDictEqual({
            ("ROOT", version1): first_edge,
            (version1, version2): second_edge}, graph.edges)

        # Check that the rest of the graph is correct.
        self.assertEqual(second_node, graph.head)
        self.assertEqual(ROOT, graph.tail)

        # Add a parallel path from ROOT to 3.
        version3 = "3"
        payload3 = {"TEST", 3}
        graph.continue_chain("ROOT", version3, payload3)

        # Make sure root did not change, except for all_next
        self.assertEqual("ROOT", ROOT.current)
        self.assertEqual(None, ROOT.prev_master)
        self.assertEqual(version1, ROOT.next_master)
        self.assertSetEqual(set(), ROOT.all_prev)
        self.assertSetEqual(set([version1, version3]), ROOT.all_next)

        # Make sure that first_node did not change.
        self.assertEqual(version1, first_node.current)
        self.assertEqual("ROOT", first_node.prev_master)
        self.assertEqual(version2, first_node.next_master)
        self.assertSetEqual(set(["ROOT"]), first_node.all_prev)
        self.assertSetEqual(set([version2]), first_node.all_next)

        # Make sure that second_node did not change.
        self.assertEqual(version2, second_node.current)
        self.assertEqual(version1, second_node.prev_master)
        self.assertEqual(None, second_node.next_master)
        self.assertSetEqual(set([version1]), second_node.all_prev)
        self.assertSetEqual(set(), second_node.all_next)

        # Check newly added node.
        third_node = graph.nodes[version3]
        self.assertEqual(version3, third_node.current)
        self.assertEqual("ROOT", third_node.prev_master)
        self.assertEqual(None, third_node.next_master)
        self.assertSetEqual(set(["ROOT"]), third_node.all_prev)
        self.assertSetEqual(set(), third_node.all_next)

        # Check that the node list is correct.
        self.assertDictEqual({
            "ROOT": ROOT,
            version1: first_node,
            version2: second_node,
            version3: third_node}, graph.nodes)

        # Check that the first_edge is still correct.
        self.assertEqual("ROOT", first_edge.from_node)
        self.assertEqual(version1, first_edge.to_node)
        self.assertEqual(payload1, first_edge.payload)

        # Check that the first_edge is still correct.
        self.assertEqual(version1, second_edge.from_node)
        self.assertEqual(version2, second_edge.to_node)
        self.assertEqual(payload2, second_edge.payload)

        # Check the new edge that was created.
        third_edge = graph.edges[("ROOT", version3)]
        self.assertEqual("ROOT", third_edge.from_node)
        self.assertEqual(version3, third_edge.to_node)
        self.assertEqual(payload3, third_edge.payload)

        # check that the edges list is correct.
        self.assertDictEqual({
            ("ROOT", version1): first_edge,
            (version1, version2): second_edge,
            ("ROOT", version3): third_edge}, graph.edges)

        # Check that the rest of the graph is correct.
        self.assertEqual(second_node, graph.head)
        self.assertEqual(ROOT, graph.tail)

        # Add a parallel path from 3 to 2.
        payload4 = {"TEST", 3}
        graph.continue_chain(version3, version2, payload4)

        # Make sure root did not change
        self.assertEqual("ROOT", ROOT.current)
        self.assertEqual(None, ROOT.prev_master)
        self.assertEqual(version1, ROOT.next_master)
        self.assertSetEqual(set(), ROOT.all_prev)
        self.assertSetEqual(set([version1, version3]), ROOT.all_next)

        # Make sure that first_node did not change.
        self.assertEqual(version1, first_node.current)
        self.assertEqual("ROOT", first_node.prev_master)
        self.assertEqual(version2, first_node.next_master)
        self.assertSetEqual(set(["ROOT"]), first_node.all_prev)
        self.assertSetEqual(set([version2]), first_node.all_next)

        # Make sure that second_node did not change, except for all_prev
        self.assertEqual(version2, second_node.current)
        self.assertEqual(version1, second_node.prev_master)
        self.assertEqual(None, second_node.next_master)
        self.assertSetEqual(set([version1, version3]), second_node.all_prev)
        self.assertSetEqual(set(), second_node.all_next)

        # Make sure that third_node did not change, except for all_next
        self.assertEqual(version3, third_node.current)
        self.assertEqual("ROOT", third_node.prev_master)
        self.assertEqual(version2, third_node.next_master)
        self.assertSetEqual(set(["ROOT"]), third_node.all_prev)
        self.assertSetEqual(set([version2]), third_node.all_next)

        # Check that the node list is correct.
        self.assertDictEqual({
            "ROOT": ROOT,
            version1: first_node,
            version2: second_node,
            version3: third_node}, graph.nodes)

        # Check that the first_edge is still correct.
        self.assertEqual("ROOT", first_edge.from_node)
        self.assertEqual(version1, first_edge.to_node)
        self.assertEqual(payload1, first_edge.payload)

        # Check that the second_edge is still correct.
        self.assertEqual(version1, second_edge.from_node)
        self.assertEqual(version2, second_edge.to_node)
        self.assertEqual(payload2, second_edge.payload)

        # Check that the third_edge is still correct.
        self.assertEqual("ROOT", third_edge.from_node)
        self.assertEqual(version3, third_edge.to_node)
        self.assertEqual(payload3, third_edge.payload)

        # Check the new edge that was created.
        fourth_edge = graph.edges[(version3, version2)]
        self.assertEqual(version3, fourth_edge.from_node)
        self.assertEqual(version2, fourth_edge.to_node)
        self.assertEqual(payload4, fourth_edge.payload)

        # check that the edges list is correct.
        self.assertDictEqual({
            ("ROOT", version1): first_edge,
            (version1, version2): second_edge,
            ("ROOT", version3): third_edge,
            (version3, version2): fourth_edge}, graph.edges)

        # Check that the rest of the graph is correct.
        self.assertEqual(second_node, graph.head)
        self.assertEqual(ROOT, graph.tail)

        
if __name__ == "__main__":
    unittest.main()
