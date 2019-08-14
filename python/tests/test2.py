from multiprocessing import Process, Event as MPEvent, Queue
import unittest
import time

from rtypes import pcc_set, primarykey, dimension, merge
from rtypes.utils.enums import Datatype

from spacetime import Dataframe
from spacetime.utils.enums import Event

@pcc_set
class Car(object):
    oid = primarykey(int)
    xvel = dimension(int)
    yvel = dimension(int)
    xpos = dimension(int)
    ypos = dimension(int)

    def move(self):
        self.xpos += self.xvel
        self.ypos += self.yvel

    def details(self):
        return self.oid, self.xvel, self.yvel, self.xpos, self.ypos

    def __init__(self, oid):
        self.oid = oid
        self.xvel = 0
        self.yvel = 0
        self.xpos = 0
        self.ypos = 0


class TestRtypes(unittest.TestCase):
    def test_base_set(self):
        c = Car(0)
        self.assertEqual(c.details(), (0, 0, 0, 0, 0))
        self.assertTrue("oid" not in c.__dict__)
        self.assertTrue("xvel" not in c.__dict__)
        self.assertTrue("yvel" not in c.__dict__)
        self.assertTrue("xpos" not in c.__dict__)
        self.assertTrue("ypos" not in c.__dict__)
        self.assertTrue(hasattr(Car, "__r_table__"))

        self.assertEqual(c.__r_oid__, 0)
        self.assertFalse(hasattr(c, "__r_df__"))
        self.assertEqual(Car.__r_table__.obj_type, Car)
        self.assertDictEqual(
            Car.__r_table__.object_table, {0: {
                "oid": {"type": Datatype.INTEGER, "value": 0},
                "xvel": {"type": Datatype.INTEGER, "value": 0},
                "yvel": {"type": Datatype.INTEGER, "value": 0},
                "xpos": {"type": Datatype.INTEGER, "value": 0},
                "ypos": {"type": Datatype.INTEGER, "value": 0}
            }})
        c.xvel = 1
        self.assertDictEqual(
            Car.__r_table__.object_table, {0: {
                "oid": {"type": Datatype.INTEGER, "value": 0},
                "xvel": {"type": Datatype.INTEGER, "value": 1},
                "yvel": {"type": Datatype.INTEGER, "value": 0},
                "xpos": {"type": Datatype.INTEGER, "value": 0},
                "ypos": {"type": Datatype.INTEGER, "value": 0}
            }})
        c.oid = 1
        self.assertDictEqual(
            Car.__r_table__.object_table, {1: {
                "oid": {"type": Datatype.INTEGER, "value": 1},
                "xvel": {"type": Datatype.INTEGER, "value": 1},
                "yvel": {"type": Datatype.INTEGER, "value": 0},
                "xpos": {"type": Datatype.INTEGER, "value": 0},
                "ypos": {"type": Datatype.INTEGER, "value": 0}
            }})
        self.assertEqual(c.__r_oid__, 1)
        
class TestFullStateDataframeBasic(unittest.TestCase):
    def test_basic_delete2(self):
        df = Dataframe("TEST", [Car])
        c = Car(0)
        df.checkout()
        df.add_one(Car, c)
        c.xvel = 1
        self.assertFalse("xvel" in c.__dict__)
        self.assertFalse("yvel" in c.__dict__)
        self.assertFalse("xpos" in c.__dict__)
        self.assertFalse("ypos" in c.__dict__)
        self.assertFalse("oid" in c.__dict__)
        self.assertTrue(hasattr(c, "__r_df__"))
        self.assertEqual(df.local_heap, c.__r_df__)
        self.assertEqual(c.xvel, 1)
        self.assertEqual(c.yvel, 0)
        self.assertEqual(c.xpos, 0)
        self.assertEqual(c.ypos, 0)
        self.assertEqual(c.oid, 0)
        df.commit()
        df.push_call_back(
            "TEST2", [df.versioned_heap.version_graph.head.current, "NEXT"], {
                Car.__r_meta__.name: {
                    0: {
                        "types": {
                            Car.__r_meta__.name: Event.Delete
                        }
                    }
                }
            })
        df.checkout()
        self.assertListEqual(list(), df.read_all(Car))
        self.assertFalse("xvel" in c.__dict__)
        self.assertFalse("yvel" in c.__dict__)
        self.assertFalse("xpos" in c.__dict__)
        self.assertFalse("ypos" in c.__dict__)
        self.assertFalse("oid" in c.__dict__)
        self.assertTrue(hasattr(c, "__r_df__"))
        self.assertEqual(None, c.__r_df__)
        self.assertEqual(c.xvel, 1)
        self.assertEqual(c.yvel, 0)
        self.assertEqual(c.xpos, 0)
        self.assertEqual(c.ypos, 0)
        self.assertEqual(c.oid, 0)

    def test_push1(self):
        df1 = Dataframe("TEST1", [Car])
        df2 = Dataframe("TEST2", [Car], details=df1.details)
        appname1 = df1.appname
        appname2 = df2.appname
        c = Car(0)
        
        df2.checkout()
        df2.add_one(Car, c)
        c.xvel = 1
        df2.sync()
        version1 = df1.versioned_heap.version_graph.head.current
        version2 = df2.versioned_heap.version_graph.head.current
        self.assertEqual(version1, version2)
        self.assertDictEqual(
            df1.versioned_heap.state_to_app, {"ROOT": set([appname2]), version1: set([appname2])})
        self.assertDictEqual(
            df1.versioned_heap.app_to_state, {appname2: version1})

        self.assertDictEqual(
            df2.versioned_heap.state_to_app,
            {'ROOT': {appname2}, version1: {'SOCKETPARENT', appname2}})
        self.assertDictEqual(
            df2.versioned_heap.app_to_state,
            {appname2: version1, "SOCKETPARENT": version1})


class TestDataframe(unittest.TestCase):
    def test_basic(self):
        df = Dataframe("TEST", [Car])
        c = Car(0)
        df.checkout()
        df.add_one(Car, c)
        c.xvel = 1
        self.assertFalse("xvel" in c.__dict__)
        self.assertFalse("yvel" in c.__dict__)
        self.assertFalse("xpos" in c.__dict__)
        self.assertFalse("ypos" in c.__dict__)
        self.assertFalse("oid" in c.__dict__)
        self.assertTrue(hasattr(c, "__r_df__"))
        self.assertEqual(df.local_heap, c.__r_df__)
        self.assertEqual(c.xvel, 1)
        self.assertEqual(c.yvel, 0)
        self.assertEqual(c.xpos, 0)
        self.assertEqual(c.ypos, 0)
        self.assertEqual(c.oid, 0)

    def test_basic_delete1(self):
        df = Dataframe("TEST", [Car])
        c = Car(0)
        df.checkout()
        df.add_one(Car, c)
        c.xvel = 1
        self.assertFalse("xvel" in c.__dict__)
        self.assertFalse("yvel" in c.__dict__)
        self.assertFalse("xpos" in c.__dict__)
        self.assertFalse("ypos" in c.__dict__)
        self.assertFalse("oid" in c.__dict__)
        self.assertTrue(hasattr(c, "__r_df__"))
        self.assertEqual(df.local_heap, c.__r_df__)
        self.assertEqual(c.xvel, 1)
        self.assertEqual(c.yvel, 0)
        self.assertEqual(c.xpos, 0)
        self.assertEqual(c.ypos, 0)
        self.assertEqual(c.oid, 0)
        df.commit()
        df.delete_one(Car, c)
        df.commit()
        self.assertListEqual(list(), df.read_all(Car))
        self.assertFalse("xvel" in c.__dict__)
        self.assertFalse("yvel" in c.__dict__)
        self.assertFalse("xpos" in c.__dict__)
        self.assertFalse("ypos" in c.__dict__)
        self.assertFalse("oid" in c.__dict__)
        self.assertTrue(hasattr(c, "__r_df__"))
        self.assertEqual(None, c.__r_df__)
        self.assertEqual(c.xvel, 1)
        self.assertEqual(c.yvel, 0)
        self.assertEqual(c.xpos, 0)
        self.assertEqual(c.ypos, 0)
        self.assertEqual(c.oid, 0)

    def test_parallel_df_no_merge(self):
        server_to_client_q = Queue()
        client_to_server_q = Queue()
        server_ready = MPEvent()
        client_ready = MPEvent()
        serv_proc = Process(
            target=server_df1,
            args=(server_to_client_q, client_to_server_q, server_ready, client_ready))
        #serv_proc.daemon = True
        serv_proc.start()
        client_proc = Process(
            target=client_df1,
            args=(client_to_server_q, server_to_client_q, server_ready, client_ready))
        #client_proc.daemon = True
        client_proc.start()
        serv_proc.join()
        client_proc.join()

    def test_parallel_df_merge(self):
        server_to_client_q = Queue()
        client_to_server_q = Queue()
        server_ready = MPEvent()
        client_ready = MPEvent()
        serv_proc = Process(
            target=server_df2,
            args=(server_to_client_q, client_to_server_q, server_ready, client_ready))
        #serv_proc.daemon = True
        serv_proc.start()
        client_proc = Process(
            target=client_df2,
            args=(client_to_server_q, server_to_client_q, server_ready, client_ready))
        #client_proc.daemon = True
        client_proc.start()
        serv_proc.join()
        client_proc.join()

    def test_parallel_df_merge_with_func(self):
        server_to_client_q = Queue()
        client_to_server_q = Queue()
        server_ready = MPEvent()
        client_ready = MPEvent()
        serv_proc = Process(
            target=server_df3,
            args=(server_to_client_q, client_to_server_q, server_ready, client_ready))
        #serv_proc.daemon = True
        serv_proc.start()
        client_proc = Process(
            target=client_df3,
            args=(client_to_server_q, server_to_client_q, server_ready, client_ready))
        #client_proc.daemon = True
        client_proc.start()
        serv_proc.join()
        client_proc.join()
    
    def test_parallel_with_create_and_delete(self):
        server_to_client_q = Queue()
        client_to_server_q = Queue()
        server_ready = MPEvent()
        client_ready = MPEvent()
        serv_proc = Process(
            target=server_df5,
            args=(server_to_client_q, client_to_server_q, server_ready, client_ready))
        #serv_proc.daemon = True
        serv_proc.start()
        client_proc = Process(
            target=client_df5,
            args=(client_to_server_q, server_to_client_q, server_ready, client_ready))
        #client_proc.daemon = True
        client_proc.start()
        serv_proc.join()
        client_proc.join()

    ''' Does not work yet as types are curried by value, not by reference. How odd.
    def test_foreign_key_basic(self):
        server_to_client_q = Queue()
        client_to_server_q = Queue()
        server_ready = MPEvent()
        client_ready = MPEvent()
        serv_proc = Process(
            target=server_df6,
            args=(server_to_client_q, client_to_server_q, server_ready, client_ready))
        #serv_proc.daemon = True
        serv_proc.start()
        client_proc = Process(
            target=client_df6,
            args=(client_to_server_q, server_to_client_q, server_ready, client_ready))
        #client_proc.daemon = True
        client_proc.start()
        serv_proc.join()
        client_proc.join()
    '''
    def test_yours_theirs_semantics(self):
        server_to_client_q = Queue()
        client_to_server_q = Queue()
        server_ready = MPEvent()
        client_ready = MPEvent()
        serv_proc = Process(
            target=server_df7,
            args=(server_to_client_q, client_to_server_q, server_ready, client_ready))
        #serv_proc.daemon = True
        serv_proc.start()
        client_proc = Process(
            target=client_df7,
            args=(client_to_server_q, server_to_client_q, server_ready, client_ready))
        #client_proc.daemon = True
        client_proc.start()
        serv_proc.join()
        client_proc.join()

def server_df1(send_q, recv_q, server_ready, client_ready):
    df = Dataframe("SERVER_TEST1", [Car])
    send_q.put(df.details)
    client_name = recv_q.get()
    # The server goes first.

    df.checkout()
    #Add car to server
    c = Car(0)
    df.add_one(Car, c)
    # Modify the car value.
    c.xvel = 1
    # Push record into server.
    df.commit()
    # Setting point C1
    server_ready.set()
    # Client 
    # Pull and read the changes.
    # Waiting at point S1
    client_ready.wait()
    client_ready.clear()
    # modify the object.
    df.checkout()
    c.yvel = 1
    df.commit()
    # Setting point C2
    server_ready.set()
    # Waiting at point S2
    client_ready.wait()
    client_ready.clear()

    df.checkout()
    assert ("xvel" not in c.__dict__)
    assert ("yvel" not in c.__dict__)
    assert ("xpos" not in c.__dict__)
    assert ("ypos" not in c.__dict__)
    assert ("oid" not in c.__dict__)
    assert (c.xvel is 1)
    assert (c.yvel is 1)
    assert (c.xpos is 1)
    assert (c.ypos is 1)
    assert (c.oid is 0)
    c2 = df.read_one(Car, 1)
    assert ("xvel" not in c2.__dict__)
    assert ("yvel" not in c2.__dict__)
    assert ("xpos" not in c2.__dict__)
    assert ("ypos" not in c2.__dict__)
    assert ("oid" not in c2.__dict__)
    assert (c2.xvel is 0)
    assert (c2.yvel is 0)
    assert (c2.xpos is 0)
    assert (c2.ypos is 0)
    assert (c2.oid is 1)
    df.commit()

    # Going for delete.
    df.checkout()
    c3 = Car(2)
    df.add_one(Car, c3)
    c4 = df.read_one(Car, 2)
    assert (c3.xvel is c4.xvel)
    assert (c3.yvel is c4.yvel)
    assert (c3.xpos is c4.xpos)
    assert (c3.ypos is c4.ypos)
    assert (c3.oid is c4.oid)
    c2.yvel = 1
    c2.xvel = 1
    df.delete_one(Car, c2)
    assert (df.read_one(Car, 1) is None)
    assert (c2.__r_df__ is None)
    assert (c2.xvel == 1)
    assert (c2.yvel == 1)
    c2.xvel = 2
    c2.yvel = 2
    assert (c2.xvel == 2)
    assert (c2.yvel == 2)
    assert (Car.__r_table__.object_table[1] == {
                "oid": {"type": Datatype.INTEGER, "value": 1},
                "xvel": {"type": Datatype.INTEGER, "value": 2},
                "yvel": {"type": Datatype.INTEGER, "value": 2},
                "xpos": {"type": Datatype.INTEGER, "value": 0},
                "ypos": {"type": Datatype.INTEGER, "value": 0}
            })
    
    df.delete_one(Car, c3)
    assert (df.read_one(Car, 2) is None)
    df.commit()
    assert (set(df.local_heap.data[Car.__r_meta__.name].keys()) == set([0]))
    
    # Setting point C3
    server_ready.set()
    # Waiting for S3
    client_ready.wait()

def client_df1(send_q, recv_q, server_ready, client_ready):
    server_name = recv_q.get()
    df = Dataframe("CLIENT_TEST", [Car], details=server_name)
    send_q.put(df.details)
    # Waiting at point C1
    server_ready.wait()
    server_ready.clear()

    # Pull from the server.
    df.pull()
    df.checkout()
    cars = df.read_all(Car)
    assert (1 == len(cars))
    c = cars[0]
    assert ("xvel" not in c.__dict__)
    assert ("yvel" not in c.__dict__)
    assert ("xpos" not in c.__dict__)
    assert ("ypos" not in c.__dict__)
    assert ("oid" not in c.__dict__)
    assert (c.xvel is 1)
    assert (c.yvel is 0)
    assert (c.xpos is 0)
    assert (c.ypos is 0)
    assert (c.oid is 0)
    df.commit()
    # Setting point S1
    client_ready.set()
    # Waiting at point C2
    server_ready.wait()
    server_ready.clear()
    df.pull()
    df.checkout()
    cars = df.read_all(Car)
    assert (1 == len(cars))
    c = cars[0]
    assert ("xvel" not in c.__dict__)
    assert ("yvel" not in c.__dict__)
    assert ("xpos" not in c.__dict__)
    assert ("ypos" not in c.__dict__)
    assert ("oid" not in c.__dict__)
    assert (c.xvel is 1)
    assert (c.yvel is 1)
    assert (c.xpos is 0)
    assert (c.ypos is 0)
    assert (c.oid is 0)
    df.commit()
    df.checkout()
    c.xpos = 1
    c.ypos = 1
    c2 = Car(1)
    df.add_one(Car, c2)
    df.commit()
    df.push()
    # Setting point S2
    client_ready.set()
    # Waiting at point C3
    server_ready.wait()
    server_ready.clear()
    df.pull()
    df.checkout()
    assert (df.read_one(Car, 1) is None)
    assert (df.read_one(Car, 2) is None)
    # This does not work yet. Have to figure it out.
    # limitation of making it a framework rather than
    # a programming language itself.
    # Cannot invalidate old references without holding the
    # reference itself.
    #assert ("xvel" not in c2.__dict__)
    #assert ("yvel" not in c2.__dict__)
    #assert ("xpos" not in c2.__dict__)
    #assert ("ypos" not in c2.__dict__)
    #assert ("oid" not in c2.__dict__)
    #assert (c2.xvel is 0)
    #assert (c2.yvel is 0)
    #assert (c2.xpos is 0)
    #assert (c2.ypos is 0)
    #assert (c2.oid is 1)
    #assert (c2.__r_df__ is None)
    #assert (Car.__r_table__.object_table[1] == {
    #            "oid": {"type": Datatype.INTEGER, "value": 0},
    #            "xvel": {"type": Datatype.INTEGER, "value": 0},
    #            "yvel": {"type": Datatype.INTEGER, "value": 0},
    #            "xpos": {"type": Datatype.INTEGER, "value": 0},
    #            "ypos": {"type": Datatype.INTEGER, "value": 0}
    #        })
    # Setting point S3
    client_ready.set()
    
def server_df2(send_q, recv_q, server_ready, client_ready):
    df = Dataframe("SERVER_TEST2", [Car])
    send_q.put(df.details)
    client_name = recv_q.get()
    # The server goes first.
    #print ("Server at start:", df.versioned_heap.version_graph.nodes.keys())

    df.checkout()
    #Add car to server
    c1 = Car(0)
    c2 = Car(1)
    df.add_many(Car, [c1, c2])
    # Modify the car value.
    c1.xvel = 1
    # Push record into server.
    df.commit()
    #print ("Server after adding 2 cars:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point C1
    #print ("Setting C1")
    server_ready.set()
    # Waiting at point S1
    #print ("Waiting for S1")
    client_ready.wait()
    client_ready.clear()
    #print ("Server after waiting for client first time:", df.versioned_heap.version_graph.nodes.keys())
    #print (df.versioned_heap.state_to_app)
    df.checkout()
    c1.yvel = 1
    df.commit()
    #print (df.versioned_heap.state_to_app)
    #print ("Server after modifying once:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point C2
    #print ("Setting C2")
    server_ready.set()
    # Waiting at point S2
    #print ("Waiting for S2")
    client_ready.wait()
    client_ready.clear()
    #print ("Server after waiting for client second time.:", df.versioned_heap.version_graph.nodes.keys())

    # Check how the merge worked out.
    df.checkout()
    c1 = df.read_one(Car, 0)
    c2 = df.read_one(Car, 1)
    assert ("xvel" not in c1.__dict__)
    assert ("yvel" not in c1.__dict__)
    assert ("xpos" not in c1.__dict__)
    assert ("ypos" not in c1.__dict__)
    assert ("oid" not in c1.__dict__)
    assert (c1.xvel is 1)
    assert (c1.yvel is 1)
    assert (c1.xpos is 0)
    assert (c1.ypos is 0)
    assert (c1.oid is 0)
    assert ("xvel" not in c2.__dict__)
    assert ("yvel" not in c2.__dict__)
    assert ("xpos" not in c2.__dict__)
    assert ("ypos" not in c2.__dict__)
    assert ("oid" not in c2.__dict__)
    assert (c2.xvel is 0)
    assert (c2.yvel is 1), c2.yvel
    assert (c2.xpos is 0)
    assert (c2.ypos is 0)
    assert (c2.oid is 1)
    # Setting point C3
    #print ("Setting C3")
    server_ready.set()
    # Waiting at point S3
    #print ("Waiting for S3")
    client_ready.wait()
    client_ready.clear()


def client_df2(send_q, recv_q, server_ready, client_ready):
    server_name = recv_q.get()
    df = Dataframe("CLIENT_TEST", [Car], details=server_name)
    send_q.put(df.details)
    #print ("Client at start:", df.versioned_heap.version_graph.nodes.keys())
    # Waiting at point C1
    #print ("Waiting for C1")
    server_ready.wait()
    server_ready.clear()
    #print ("Client after waiting for server first time.:", df.versioned_heap.version_graph.nodes.keys())

    # Pull from the server.
    df.pull()
    #print ("Client after first pull:", df.versioned_heap.version_graph.nodes.keys())
    df.checkout()
    cars = df.read_all(Car)
    assert (2 == len(cars))
    c1, c2 = cars
    #print ("Setting S1")
    # Setting point S1
    client_ready.set()
    c2.yvel = 1
    df.commit()
    #print ("Client after first modification:", df.versioned_heap.version_graph.nodes.keys())
    # Waiting at point C2
    #print ("Waiting for C2")
    server_ready.wait()
    server_ready.clear()
    #print ("Client after waiting for server:", df.versioned_heap.version_graph.nodes.keys())
    df.push()
    #print ("Client after pushing:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point S2
    #print ("Setting S2")
    client_ready.set()
    # Waiting at point c3
    #print ("Waiting for C3")
    server_ready.wait()
    server_ready.clear()
    df.pull()
    #print ("Client after pulling second time:", df.versioned_heap.version_graph.nodes.keys())

    df.checkout()

    c1 = df.read_one(Car, 0)
    c2 = df.read_one(Car, 1)
    assert ("xvel" not in c1.__dict__)
    assert ("yvel" not in c1.__dict__)
    assert ("xpos" not in c1.__dict__)
    assert ("ypos" not in c1.__dict__)
    assert ("oid" not in c1.__dict__)
    assert (c1.xvel is 1)
    assert (c1.yvel is 1)
    assert (c1.xpos is 0)
    assert (c1.ypos is 0)
    assert (c1.oid is 0)
    assert ("xvel" not in c2.__dict__)
    assert ("yvel" not in c2.__dict__)
    assert ("xpos" not in c2.__dict__)
    assert ("ypos" not in c2.__dict__)
    assert ("oid" not in c2.__dict__)
    assert (c2.xvel is 0)
    assert (c2.yvel is 1)
    assert (c2.xpos is 0)
    assert (c2.ypos is 0)
    assert (c2.oid is 1)

    # Setting point S3
    #print ("Setting S3")
    client_ready.set()

@pcc_set
class Counter(object):
    oid = primarykey(int)
    count = dimension(int)
    def __init__(self, oid):
        self.oid = oid
        self.count = 0
    
    
def counter_merge_func(original, yours, theirs):
    original.count = (yours.count + theirs.count) - original.count
    return original



def server_df3(send_q, recv_q, server_ready, client_ready):
    df = Dataframe("SERVER_TEST3", [Counter], resolver={Counter: counter_merge_func})
    send_q.put(df.details)
    client_name = recv_q.get()
    # The server goes first.
    #print ("Server at start:", df.versioned_heap.version_graph.nodes.keys())

    df.checkout()
    #Add Counter to server
    c1 = Counter(0)

    df.add_many(Counter, [c1])
    assert (c1.count == 0)
    # Modify the counter value.
    c1.count += 1
    assert (c1.count == 1)
    # Push record into server.
    df.commit()
    #print ("Server after adding 2 cars:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point C1
    #print ("Setting C1")
    server_ready.set()
    # Waiting at point S1
    #print ("Waiting for S1")
    client_ready.wait()
    client_ready.clear()
    #print ("Server after waiting for client first time:", df.versioned_heap.version_graph.nodes.keys())
    #print (df.versioned_heap.state_to_app)
    df.checkout()
    assert (c1.count == 1)
    c1.count += 1
    assert (c1.count == 2)
    df.commit()
    #print (df.versioned_heap.state_to_app)
    #print ("Server after modifying once:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point C2
    #print ("Setting C2")
    server_ready.set()
    # Waiting at point S2
    #print ("Waiting for S2")
    client_ready.wait()
    client_ready.clear()
    #print ("Server after waiting for client second time.:", df.versioned_heap.version_graph.nodes.keys())

    # Check how the merge worked out.
    df.checkout()
    assert (c1.count == 3), c1.count
    # Setting point C3
    #print ("Setting C3")
    server_ready.set()
    # Waiting at point S3
    #print ("Waiting for S3")
    client_ready.wait()
    client_ready.clear()


def client_df3(send_q, recv_q, server_ready, client_ready):
    server_name = recv_q.get()
    df = Dataframe("CLIENT_TEST", [Counter], details=server_name, resolver={Counter: counter_merge_func})
    send_q.put(df.details)
    #print ("Client at start:", df.versioned_heap.version_graph.nodes.keys())
    # Waiting at point C1
    #print ("Waiting for C1")
    server_ready.wait()
    server_ready.clear()
    #print ("Client after waiting for server first time.:", df.versioned_heap.version_graph.nodes.keys())

    # Pull from the server.
    df.pull()
    #print ("Client after first pull:", df.versioned_heap.version_graph.nodes.keys())
    df.checkout()
    counters = df.read_all(Counter)
    assert (1 == len(counters))
    c1 = counters[0]
    assert (c1.count == 1)
    #print ("Setting S1")
    # Setting point S1
    client_ready.set()
    c1.count += 1
    assert (c1.count == 2)
    df.commit()
    #print ("Client after first modification:", df.versioned_heap.version_graph.nodes.keys())
    # Waiting at point C2
    #print ("Waiting for C2")
    server_ready.wait()
    server_ready.clear()
    #print ("Client after waiting for server:", df.versioned_heap.version_graph.nodes.keys())
    df.push()
    #print ("Client after pushing:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point S2
    #print ("Setting S2")
    client_ready.set()
    # Waiting at point c3
    #print ("Waiting for C3")
    server_ready.wait()
    server_ready.clear()
    df.pull()
    #print ("Client after pulling second time:", df.versioned_heap.version_graph.nodes.keys())

    df.checkout()

    assert (c1.count == 3)

    # Setting point S3
    #print ("Setting S3")
    client_ready.set()



def server_df4(send_q, recv_q, server_ready, client_ready):
    df = Dataframe("SERVER_TEST4", [Car, Counter], resolver={Counter: counter_merge_func})
    send_q.put(df.details)
    client_name = recv_q.get()
    # The server goes first.
    #print ("Server at start:", df.versioned_heap.version_graph.nodes.keys())

    df.checkout()
    #Add Counter to server
    c1 = Counter(0)
    car1 = Car(0)
    df.add_many(Counter, [c1])
    df.add_many(Car, [car1])
    assert (c1.count == 0)
    # Modify the counter value.
    c1.count += 1
    assert (c1.count == 1)
    # Push record into server.
    df.commit()
    #print ("Server after adding 2 cars:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point C1
    #print ("Setting C1")
    server_ready.set()
    # Waiting at point S1
    #print ("Waiting for S1")
    client_ready.wait()
    client_ready.clear()
    #print ("Server after waiting for client first time:", df.versioned_heap.version_graph.nodes.keys())
    #print (df.versioned_heap.state_to_app)
    df.checkout()
    assert (c1.count == 1)
    c1.count += 1
    assert (c1.count == 2)
    df.commit()
    #print (df.versioned_heap.state_to_app)
    #print ("Server after modifying once:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point C2
    #print ("Setting C2")
    server_ready.set()
    # Waiting at point S2
    #print ("Waiting for S2")
    client_ready.wait()
    client_ready.clear()
    #print ("Server after waiting for client second time.:", df.versioned_heap.version_graph.nodes.keys())

    # Check how the merge worked out.
    df.checkout()
    assert (c1.count == 3)
    # Setting point C3
    #print ("Setting C3")
    server_ready.set()
    # Waiting at point S3
    #print ("Waiting for S3")
    client_ready.wait()
    client_ready.clear()


def client_df4(send_q, recv_q, server_ready, client_ready):
    server_name = recv_q.get()
    df = Dataframe("CLIENT_TEST", [Counter], details=server_name, resolver={Counter: counter_merge_func})
    send_q.put(df.details)
    #print ("Client at start:", df.versioned_heap.version_graph.nodes.keys())
    # Waiting at point C1
    #print ("Waiting for C1")
    server_ready.wait()
    server_ready.clear()
    #print ("Client after waiting for server first time.:", df.versioned_heap.version_graph.nodes.keys())

    # Pull from the server.
    df.pull()
    #print ("Client after first pull:", df.versioned_heap.version_graph.nodes.keys())
    df.checkout()
    counters = df.read_all(Counter)
    cars = df.read_all(Car)
    assert (1 == len(counters))
    assert (0 == len(cars))
    c1 = counters[0]
    assert (c1.count == 1)
    #print ("Setting S1")
    # Setting point S1
    client_ready.set()
    c1.count += 1
    assert (c1.count == 2)
    df.commit()
    #print ("Client after first modification:", df.versioned_heap.version_graph.nodes.keys())
    # Waiting at point C2
    #print ("Waiting for C2")
    server_ready.wait()
    server_ready.clear()
    #print ("Client after waiting for server:", df.versioned_heap.version_graph.nodes.keys())
    df.push()
    #print ("Client after pushing:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point S2
    #print ("Setting S2")
    client_ready.set()
    # Waiting at point c3
    #print ("Waiting for C3")
    server_ready.wait()
    server_ready.clear()
    df.pull()
    #print ("Client after pulling second time:", df.versioned_heap.version_graph.nodes.keys())

    df.checkout()

    assert (c1.count == 3)

    # Setting point S3
    #print ("Setting S3")
    client_ready.set()



def server_df5(send_q, recv_q, server_ready, client_ready):
    df = Dataframe("SERVER_TEST5", [Car])
    send_q.put(df.details)
    client_name = recv_q.get()
    # The server goes first.

    df.checkout()
    #Add car to server
    c1 = Car(0)
    df.add_one(Car, c1)
    # Modify the car value.
    c1.xvel = 1
    # Push record into server.
    df.commit()
    # Setting point C1
    server_ready.set()
    # Client 
    # Pull and read the changes.
    # Waiting at point S1
    client_ready.wait()
    client_ready.clear()
    # modify the object.
    df.checkout()
    c2 = Car(1)
    df.add_one(Car, c2)
    df.commit()
    df.checkout()
    df.delete_one(Car, c2)
    c1.yvel = 1
    df.commit()
    # Setting point C2
    server_ready.set()
    # Waiting at point S2
    client_ready.wait()
    client_ready.clear()

    df.checkout()
    assert ("xvel" not in c1.__dict__)
    assert ("yvel" not in c1.__dict__)
    assert ("xpos" not in c1.__dict__)
    assert ("ypos" not in c1.__dict__)
    assert ("oid" not in c1.__dict__)
    assert (c1.xvel is 1)
    assert (c1.yvel is 1)
    assert (c1.xpos is 1)
    assert (c1.ypos is 1)
    assert (c1.oid is 0)
    c2 = df.read_one(Car, 1)
    assert ("xvel" not in c2.__dict__)
    assert ("yvel" not in c2.__dict__)
    assert ("xpos" not in c2.__dict__)
    assert ("ypos" not in c2.__dict__)
    assert ("oid" not in c2.__dict__)
    assert (c2.xvel is 0)
    assert (c2.yvel is 0)
    assert (c2.xpos is 0)
    assert (c2.ypos is 0)
    assert (c2.oid is 1)
    df.commit()

    # Going for delete.
    df.checkout()
    c3 = Car(2)
    df.add_one(Car, c3)
    c4 = df.read_one(Car, 2)
    assert (c3.xvel is c4.xvel)
    assert (c3.yvel is c4.yvel)
    assert (c3.xpos is c4.xpos)
    assert (c3.ypos is c4.ypos)
    assert (c3.oid is c4.oid)
    c2.yvel = 1
    c2.xvel = 1
    df.delete_one(Car, c2)
    assert (df.read_one(Car, 1) is None)
    assert (c2.__r_df__ is None)
    assert (c2.xvel == 1)
    assert (c2.yvel == 1)
    c2.xvel = 2
    c2.yvel = 2
    assert (c2.xvel == 2)
    assert (c2.yvel == 2)
    assert (Car.__r_table__.object_table[1] == {
                "oid": {"type": Datatype.INTEGER, "value": 1},
                "xvel": {"type": Datatype.INTEGER, "value": 2},
                "yvel": {"type": Datatype.INTEGER, "value": 2},
                "xpos": {"type": Datatype.INTEGER, "value": 0},
                "ypos": {"type": Datatype.INTEGER, "value": 0}
            })
    
    df.delete_one(Car, c3)
    assert (df.read_one(Car, 2) is None)
    df.commit()
    assert (set(df.local_heap.data[Car.__r_meta__.name].keys()) == set([0]))
    
    # Setting point C3
    server_ready.set()
    # Waiting for S3
    client_ready.wait()

def client_df5(send_q, recv_q, server_ready, client_ready):
    server_name = recv_q.get()
    df = Dataframe("CLIENT_TEST", [Car], details=server_name)
    send_q.put(df.details)
    # Waiting at point C1
    server_ready.wait()
    server_ready.clear()

    # Pull from the server.
    df.pull()
    df.checkout()
    cars = df.read_all(Car)
    assert (1 == len(cars))
    c = cars[0]
    assert ("xvel" not in c.__dict__)
    assert ("yvel" not in c.__dict__)
    assert ("xpos" not in c.__dict__)
    assert ("ypos" not in c.__dict__)
    assert ("oid" not in c.__dict__)
    assert (c.xvel is 1)
    assert (c.yvel is 0)
    assert (c.xpos is 0)
    assert (c.ypos is 0)
    assert (c.oid is 0)
    df.commit()
    # Setting point S1
    client_ready.set()
    # Waiting at point C2
    server_ready.wait()
    server_ready.clear()
    df.pull()
    df.checkout()
    cars = df.read_all(Car)
    assert (1 == len(cars))
    c = cars[0]
    assert ("xvel" not in c.__dict__)
    assert ("yvel" not in c.__dict__)
    assert ("xpos" not in c.__dict__)
    assert ("ypos" not in c.__dict__)
    assert ("oid" not in c.__dict__)
    assert (c.xvel is 1)
    assert (c.yvel is 1)
    assert (c.xpos is 0)
    assert (c.ypos is 0)
    assert (c.oid is 0)
    df.commit()
    df.checkout()
    c.xpos = 1
    c.ypos = 1
    c2 = Car(1)
    df.add_one(Car, c2)
    df.commit()
    df.push()
    # Setting point S2
    client_ready.set()
    # Waiting at point C3
    server_ready.wait()
    server_ready.clear()
    df.pull()
    df.checkout()
    assert (df.read_one(Car, 1) is None)
    assert (df.read_one(Car, 2) is None)
    # Setting point S3
    client_ready.set()
    
@pcc_set
class ClassWithCounter(object):
    oid = primarykey(int)
    counter = dimension(Counter)
    def __init__(self, oid):
        self.oid = oid
        self.counter = Counter(oid)

def server_df6(send_q, recv_q, server_ready, client_ready):
    df = Dataframe("SERVER_TEST6", [ClassWithCounter, Counter], resolver={Counter: counter_merge_func})
    send_q.put(df.details)
    client_name = recv_q.get()
    # The server goes first.

    df.checkout()
    c1 = ClassWithCounter(0)
    df.add_one(Counter, c1.counter)
    df.add_one(ClassWithCounter, c1)
    c1.counter.count += 1
    # Push record into server.
    df.commit()
    # Setting point C1
    server_ready.set()
    # Client 
    # Pull and read the changes.
    # Waiting at point S1
    client_ready.wait()
    client_ready.clear()

def client_df6(send_q, recv_q, server_ready, client_ready):
    server_name = recv_q.get()
    df = Dataframe("CLIENT_TEST", [ClassWithCounter, Counter], details=server_name, resolver={Counter: counter_merge_func})
    send_q.put(df.details)
    # Waiting at point C1
    server_ready.wait()
    server_ready.clear()

    # Pull from the server.
    df.pull()
    df.checkout()
    ccs = df.read_all(ClassWithCounter)
    assert (1 == len(ccs))
    c = ccs[0]
    assert ("counter" not in c.__dict__)
    assert ("oid" not in c.__dict__)
    assert (c.counter.oid is 0)
    assert (c.counter.count is 1)
    assert (c.oid is 0)
    df.commit()
    # Setting point S1
    client_ready.set()    

@pcc_set
class Blocker(object):
    oid = primarykey(int)
    prop = dimension(int)

    def __init__(self, oid):
        self.oid = oid
        self.prop = 0


def blocker_merge_func(original, yours, theirs):
    return theirs

def server_df7(send_q, recv_q, server_ready, client_ready):
    df = Dataframe("SERVER_TEST7", [Blocker], resolver={Blocker: blocker_merge_func})
    send_q.put(df.details)
    client_name = recv_q.get()
    # The server goes first.
    #print ("Server at start:", df.versioned_heap.version_graph.nodes.keys())

    df.checkout()
    #Add Counter to server
    b1 = Blocker(0)
    df.add_many(Blocker, [b1])
    assert (b1.prop == 0)
    b1.prop += 1
    assert (b1.prop == 1)
    # Push record into server.
    df.commit()
    #print ("Server after adding 2 cars:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point C1
    #print ("Setting C1")
    server_ready.set()
    # Waiting at point S1
    #print ("Waiting for S1")
    client_ready.wait()
    client_ready.clear()
    #print ("Server after waiting for client first time:", df.versioned_heap.version_graph.nodes.keys())
    #print (df.versioned_heap.state_to_app)
    df.checkout()
    assert (b1.prop == 1)
    b1.prop += 1
    assert (b1.prop == 2)
    df.commit()
    #print (df.versioned_heap.state_to_app)
    #print ("Server after modifying once:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point C2
    #print ("Setting C2")
    server_ready.set()
    # Waiting at point S2
    #print ("Waiting for S2")
    client_ready.wait()
    client_ready.clear()
    #print ("Server after waiting for client second time.:", df.versioned_heap.version_graph.nodes.keys())

    # Check how the merge worked out.
    df.checkout()
    assert (b1.prop == 10), b1.prop
    # Setting point C3
    #print ("Setting C3")
    server_ready.set()
    # Waiting at point S3
    #print ("Waiting for S3")
    client_ready.wait()
    client_ready.clear()
    assert (b1.prop == 10)
    b1.prop = 20
    assert (b1.prop == 20)
    df.commit()
    df.checkout()
    assert (b1.prop == 5)
    
    # Setting point C4
    #print ("Setting C4")
    server_ready.set()
    



def client_df7(send_q, recv_q, server_ready, client_ready):
    server_name = recv_q.get()
    df = Dataframe("CLIENT_TEST", [Blocker], details=server_name, resolver={Blocker: blocker_merge_func})
    send_q.put(df.details)
    #print ("Client at start:", df.versioned_heap.version_graph.nodes.keys())
    # Waiting at point C1
    #print ("Waiting for C1")
    server_ready.wait()
    server_ready.clear()
    #print ("Client after waiting for server first time.:", df.versioned_heap.version_graph.nodes.keys())

    # Pull from the server.
    df.pull()
    #print ("Client after first pull:", df.versioned_heap.version_graph.nodes.keys())
    df.checkout()
    blockers = df.read_all(Blocker)
    assert (1 == len(blockers))
    b1 = blockers[0]
    assert (b1.prop == 1)
    #print ("Setting S1")
    # Setting point S1
    client_ready.set()
    b1.prop = 10
    assert (b1.prop == 10)
    df.commit()
    #print ("Client after first modification:", df.versioned_heap.version_graph.nodes.keys())
    # Waiting at point C2
    #print ("Waiting for C2")
    server_ready.wait()
    server_ready.clear()
    #print ("Client after waiting for server:", df.versioned_heap.version_graph.nodes.keys())
    df.push()
    #print ("Client after pushing:", df.versioned_heap.version_graph.nodes.keys())
    # Setting point S2
    #print ("Setting S2")
    client_ready.set()
    # Waiting at point c3
    #print ("Waiting for C3")
    server_ready.wait()
    server_ready.clear()
    df.pull()
    #print ("Client after pulling second time:", df.versioned_heap.version_graph.nodes.keys())

    df.checkout()

    assert (b1.prop == 10)
    b1.prop = 5
    df.commit()
    df.push()

    # Setting point S3
    #print ("Setting S3")
    client_ready.set()

    # Waiting at point c4
    #print ("Waiting for C4")
    server_ready.wait()
    server_ready.clear()


