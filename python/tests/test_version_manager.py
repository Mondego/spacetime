from uuid import uuid4
import unittest

from spacetime.managers.version_manager import FullStateVersionManager
from rtypes.types.pcc_set import pcc_set
from rtypes.attributes import dimension, primarykey
from rtypes.utils.enums import Datatype
from spacetime.utils.enums import Event


@pcc_set
class Car(object):
    oid = primarykey(int)
    xvel = dimension(int)
    yvel = dimension(int)
    xpos = dimension(int)
    ypos = dimension(int)

    def move(self):
        xpos += xvel
        ypos += yvel

    def details(self):
        return self.oid, self.xvel, self.yvel, self.xpos, self.ypos

    def __init__(self, oid):
        self.oid = oid
        self.xvel = 0
        self.yvel = 0
        self.xpos = 0
        self.ypos = 0

carname = Car.__r_meta__.name
package1 = {
    carname: {
        0: {
            "dims": {
                "oid": {"type": Datatype.INTEGER, "value": 0},
                "xvel": {"type": Datatype.INTEGER, "value": 0},
                "yvel": {"type": Datatype.INTEGER, "value": 0},
                "xpos": {"type": Datatype.INTEGER, "value": 0},
                "ypos": {"type": Datatype.INTEGER, "value": 0}
            },
            "types": {
                carname: Event.New
            }
        }
    }
}

package2 = {
    carname: {
        0: {
            "dims": {
                "xvel": {"type": Datatype.INTEGER, "value": 1}
            },
            "types": {
                carname: Event.Modification
            }
        }
    }
}

package3 = {
    carname: {
        0: {
            "dims": {
                "oid": {"type": Datatype.INTEGER, "value": 0},
                "xvel": {"type": Datatype.INTEGER, "value": 1},
                "yvel": {"type": Datatype.INTEGER, "value": 0},
                "xpos": {"type": Datatype.INTEGER, "value": 0},
                "ypos": {"type": Datatype.INTEGER, "value": 0}
            },
            "types": {
                carname: Event.New
            }
        }
    }
}

package4 = {
    carname: {
        0: {
            "dims": {
                "yvel": {"type": Datatype.INTEGER, "value": 1}
            },
            "types": {
                carname: Event.Modification
            }
        }
    }
}

package5 = {
    carname: {
        0: {
            "dims": {
                "oid": {"type": Datatype.INTEGER, "value": 0},
                "xvel": {"type": Datatype.INTEGER, "value": 1},
                "yvel": {"type": Datatype.INTEGER, "value": 1},
                "xpos": {"type": Datatype.INTEGER, "value": 0},
                "ypos": {"type": Datatype.INTEGER, "value": 0}
            },
            "types": {
                carname: Event.New
            }
        }
    }
}

class TestFullStateVersionManager(unittest.TestCase):
    def check_nodes(self, nodes, values):
        for curr, prev_m, next_m, all_prev, all_next, is_master in values:
            self.assertTrue(curr in nodes)
            node = nodes[curr]
            self.assertEqual(curr, node.current)
            self.assertEqual(prev_m, node.prev_master)
            self.assertEqual(next_m, node.next_master)
            self.assertSetEqual(all_prev, node.all_prev)
            self.assertSetEqual(all_next, node.all_next)
            self.assertEqual(is_master, node.is_master)

    def check_edges(self, edges, values):
        for from_v, to_v, package in values:
            self.assertTrue((from_v, to_v) in edges)
            edge = edges[(from_v, to_v)]
            self.assertEqual(from_v, edge.from_node)
            self.assertEqual(to_v, edge.to_node)
            self.assertEqual(package, edge.payload)

    def test_vm_init(self):
        vm = FullStateVersionManager("TEST", [Car])
        self.assertListEqual([Car], vm.types)
        self.assertDictEqual({Car.__r_meta__.name: Car}, vm.type_map)

    def test_vm_receive_data1(self):
        vm = FullStateVersionManager("TEST", [Car])
        vg = vm.version_graph

        vm.receive_data("TEST_APP", ["ROOT", "0"], package1)
        node1 = vg.head
        self.assertEqual("0", node1.current)
        self.assertEqual("ROOT", node1.prev_master)
        self.assertEqual(None, node1.next_master)
        edge1 = vg.edges[("ROOT", "0")]
        self.assertEqual("ROOT", edge1.from_node)
        self.assertEqual("0", edge1.to_node)
        self.assertEqual(package1, edge1.payload)

        vm.receive_data("TEST_APP", ["0", "1"], package2)
        node2 = vg.head
        self.assertEqual("1", node2.current)
        self.assertEqual("ROOT", node2.prev_master)
        self.assertEqual(None, node2.next_master)
        edge2 = vg.edges[("ROOT", "1")]
        self.assertEqual("ROOT", edge2.from_node)
        self.assertEqual("1", edge2.to_node)
        self.assertEqual(package3, edge2.payload)

    def test_vm_receive_data2(self):
        vm = FullStateVersionManager("TEST", [Car])
        vg = vm.version_graph

        vm.receive_data("TEST_APP1", ["ROOT", "0"], package1)
        node1 = vg.head
        self.assertEqual("0", node1.current)
        self.assertEqual("ROOT", node1.prev_master)
        self.assertEqual(None, node1.next_master)
        edge1 = vg.edges[("ROOT", "0")]
        self.assertEqual("ROOT", edge1.from_node)
        self.assertEqual("0", edge1.to_node)
        self.assertEqual(package1, edge1.payload)

        vm.receive_data("TEST_APP2", ["0", "1"], package2)
        node2 = vg.head
        self.assertEqual("1", node2.current)
        self.assertEqual("0", node2.prev_master)
        self.assertEqual(None, node2.next_master)
        edge2 = vg.edges[("0", "1")]
        self.assertEqual("0", edge2.from_node)
        self.assertEqual("1", edge2.to_node)
        self.assertEqual(package2, edge2.payload)

        vm.receive_data("TEST_APP1", ["0", "2"], package4)
        node3 = vg.head
        self.assertSetEqual(
            set(["ROOT", "0", "1", "2", node3.current]), set(vg.nodes.keys()))
        self.check_nodes(
            vg.nodes,
            [("ROOT", None, "0", set(), set(["0"]), True),
             ("0", "ROOT", "1", set(["ROOT"]), set(["1", "2"]), True),
             ("1", "0", node3.current,
              set(["0"]), set([node3.current]), True),
             ("2", "0", node3.current, set(["0"]), set([node3.current]), False),
             (node3.current, "1", None, set(["1", "2"]), set(), True)])

        self.assertSetEqual(
            set([
                ("ROOT", "0"),
                ("0", "1"),
                ("0", "2"),
                ("1", node3.current),
                ("2", node3.current)]),
            set(vg.edges.keys()))
        
        self.check_edges(
            vg.edges,
            [("ROOT", "0", package1),
             ("0", "1", package2),
             ("0", "2", package4),
             ("1", node3.current, package4),
             ("2", node3.current, package2)])
        
    def test_vm_retrieve_data1(self):
        vm = FullStateVersionManager("TEST", [Car])
        vg = vm.version_graph

        vm.receive_data("TEST_APP1", ["ROOT", "0"], package1)
        
        r_package1, r_versions1 = vm.retrieve_data("TEST_APP1", "ROOT")
        self.assertDictEqual(package1, r_package1)
        self.assertListEqual(["ROOT", "0"], r_versions1)

        vm.receive_data("TEST_APP2", ["0", "1"], package2)
        r_package2, r_versions2 = vm.retrieve_data("TEST_APP2", "ROOT")
        self.assertDictEqual(package3, r_package2)
        self.assertListEqual(["ROOT", "1"], r_versions2)

        r_package3, r_versions3 = vm.retrieve_data("TEST_APP2", "0")
        self.assertDictEqual(package2, r_package3)
        self.assertListEqual(["0", "1"], r_versions3)

        vm.receive_data("TEST_APP1", ["0", "2"], package4)
        r_package4, r_versions4 = vm.retrieve_data("TEST_APP1", "ROOT")
        self.assertDictEqual(package5, r_package4)
        self.assertListEqual(["ROOT", vg.head.current], r_versions4)

        r_package5, r_versions5 = vm.retrieve_data("TEST_APP1", "1")
        self.assertDictEqual(package4, r_package5)
        self.assertListEqual(["1", vg.head.current], r_versions5)

        r_package6, r_versions6 = vm.retrieve_data("TEST_APP1", "2")
        self.assertDictEqual(package2, r_package6)
        self.assertListEqual(["2", vg.head.current], r_versions6)

    def test_vm_retrieve_data2(self):
        vm = FullStateVersionManager("TEST", [Car])
        vg = vm.version_graph
        vm.receive_data("TEST_APP1", ["ROOT", "0"], package1)
        vm.receive_data("TEST_APP2", ["0", "1"], package2)
        vm.receive_data("TEST_APP1", ["0", "2"], package4)
        vm.data_sent_confirmed("TEST_APP1", ["0", vg.head.current])

        self.assertSetEqual(
            set(["ROOT", "1", vg.head.current]), set(vg.nodes.keys()))
        self.check_nodes(
            vg.nodes,
            [("ROOT", None, "1", set(), set(["1"]), True),
             ("1", "ROOT", vg.head.current,
              set(["ROOT"]), set([vg.head.current]), True),
             (vg.head.current, "1", None, set(["1"]), set(), True)])

        self.assertSetEqual(
            set([
                ("ROOT", "1"),
                ("1", vg.head.current)]),
            set(vg.edges.keys()))
        
        self.check_edges(
            vg.edges,
            [("ROOT", "1", package3),
             ("1", vg.head.current, package4)])

        vm.data_sent_confirmed("TEST_APP2", ["0", vg.head.current])

        self.assertSetEqual(
            set(["ROOT", vg.head.current]), set(vg.nodes.keys()))
        self.check_nodes(
            vg.nodes,
            [("ROOT", None, vg.head.current,
              set(), set([vg.head.current]), True),
             (vg.head.current, "ROOT", None, set(["ROOT"]), set(), True)])

        self.assertSetEqual(
            set([("ROOT", vg.head.current)]),
            set(vg.edges.keys()))
        
        self.check_edges(vg.edges, [("ROOT", vg.head.current, package5)])


if __name__ == "__main__":
    unittest.main()
