from multiprocessing import Process, Event as MPEvent, Queue
import unittest
import time

from rtypes import pcc_set, primarykey, dimension
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


class TestDataframe(unittest.TestCase):
    def test_p2p_one_side_push2(self):
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

def server_df1(send_q, recv_q, server_ready, client_ready):
    df = Dataframe("node1", [Car])
    send_q.put(df.details)
    n2_details = recv_q.get()
    df.add_remote("node2", n2_details)
    # The server goes first.

    df.checkout()
    #Add car to server
    c = Car(0)
    df.add_one(Car, c)
    # Modify the car value.
    c.xvel = 1
    # Push record into server.
    df.commit()
    df.push_await()
    # Setting point C1
    server_ready.set()
    # Waiting at point S1
    client_ready.wait()
    client_ready.clear()
    # Lets see what the client pushed.
    df.checkout()
    cars = df.read_all(Car)
    assert len(cars) == 1
    assert c.xvel == 2
    # Setting point C2
    server_ready.set()
    
    # Lets do a parallel push
    c1 = Car(1)
    df.add_one(Car, c1)
    c1.xvel = 2
    df.commit()
    # Waiting at point S2
    client_ready.wait()
    client_ready.clear()
    # Setting point C3
    server_ready.set()
    
    df.push_await()

    # Waiting at point S3
    client_ready.wait()
    client_ready.clear()
    # Setting point C4
    server_ready.set()
    
    # See how the divergence went
    df.checkout()
    cars = df.read_all(Car)
    assert len(cars) == 3
    c2 = df.read_one(Car, 2)

    assert ("xvel" not in c.__dict__)
    assert ("yvel" not in c.__dict__)
    assert ("xpos" not in c.__dict__)
    assert ("ypos" not in c.__dict__)
    assert ("oid" not in c.__dict__)
    assert (c.xvel is 2)
    assert (c.yvel is 0)
    assert (c.xpos is 0)
    assert (c.ypos is 0)
    assert (c.oid is 0)
    assert ("xvel" not in c1.__dict__)
    assert ("yvel" not in c1.__dict__)
    assert ("xpos" not in c1.__dict__)
    assert ("ypos" not in c1.__dict__)
    assert ("oid" not in c1.__dict__)
    assert (c1.xvel is 2)
    assert (c1.yvel is 0)
    assert (c1.xpos is 0)
    assert (c1.ypos is 0)
    assert (c1.oid is 1)
    assert ("xvel" not in c2.__dict__)
    assert ("yvel" not in c2.__dict__)
    assert ("xpos" not in c2.__dict__)
    assert ("ypos" not in c2.__dict__)
    assert ("oid" not in c2.__dict__)
    assert (c2.xvel is 3)
    assert (c2.yvel is 0)
    assert (c2.xpos is 0)
    assert (c2.ypos is 0)
    assert (c2.oid is 2)

    # Waiting at point S3
    client_ready.wait()
    client_ready.clear()
    # Now we are going to make another change.
    c2.yvel = 1
    df.commit()
    df.push_await()
    # Setting point C4
    server_ready.set()

    # # Going for delete.
    # df.checkout()
    # c3 = Car(2)
    # df.add_one(Car, c3)
    # c4 = df.read_one(Car, 2)
    # assert (c3.xvel is c4.xvel)
    # assert (c3.yvel is c4.yvel)
    # assert (c3.xpos is c4.xpos)
    # assert (c3.ypos is c4.ypos)
    # assert (c3.oid is c4.oid)
    # c2.yvel = 1
    # c2.xvel = 1
    # df.delete_one(Car, c2)
    # assert (df.read_one(Car, 1) is None)
    # assert (c2.__r_df__ is None)
    # assert (c2.xvel == 1)
    # assert (c2.yvel == 1)
    # c2.xvel = 2
    # c2.yvel = 2
    # assert (c2.xvel == 2)
    # assert (c2.yvel == 2)
    # assert (Car.__r_table__.object_table[1] == {
    #     "oid": 1, "xvel": 2, "yvel": 2, "xpos": 0, "ypos": 0})

    # df.delete_one(Car, c3)
    # assert df.read_one(Car, 2) is None
    # df.commit()
    # assert set(df.heap.data[Car.__r_meta__.name].keys()) == set([0])

    # # Setting point C3
    # server_ready.set()
    # # Waiting for S3
    # client_ready.wait()

def client_df1(send_q, recv_q, server_ready, client_ready):
    server_name = recv_q.get()
    df = Dataframe("node2", [Car], remotes={"node1": server_name})
    send_q.put(df.details)
    # Waiting at point C1
    server_ready.wait()
    server_ready.clear()

    # Server should have pushed an object.
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
    
    c.xvel = 2
    df.commit()
    df.push_await()
    # Setting point S1
    client_ready.set()

    # Waiting at point C2
    server_ready.wait()
    server_ready.clear()

    c2 = Car(2)
    c2.xvel = 3
    df.add_one(Car, c2)
    df.commit()

    # Setting point S2
    client_ready.set()
    # Waiting at point C3
    server_ready.wait()
    server_ready.clear()
    df.push_await()

    # Setting point S3
    client_ready.set()
    # Waiting at point C4
    server_ready.wait()
    server_ready.clear()
    
    df.checkout()
    c1 = df.read_one(Car, 1)
    cars = df.read_all(Car)
    assert len(cars) == 3
    assert ("xvel" not in c.__dict__)
    assert ("yvel" not in c.__dict__)
    assert ("xpos" not in c.__dict__)
    assert ("ypos" not in c.__dict__)
    assert ("oid" not in c.__dict__)
    assert (c.xvel is 2)
    assert (c.yvel is 0)
    assert (c.xpos is 0)
    assert (c.ypos is 0)
    assert (c.oid is 0)
    assert ("xvel" not in c1.__dict__)
    assert ("yvel" not in c1.__dict__)
    assert ("xpos" not in c1.__dict__)
    assert ("ypos" not in c1.__dict__)
    assert ("oid" not in c1.__dict__)
    assert (c1.xvel is 2)
    assert (c1.yvel is 0)
    assert (c1.xpos is 0)
    assert (c1.ypos is 0)
    assert (c1.oid is 1)
    assert ("xvel" not in c2.__dict__)
    assert ("yvel" not in c2.__dict__)
    assert ("xpos" not in c2.__dict__)
    assert ("ypos" not in c2.__dict__)
    assert ("oid" not in c2.__dict__)
    assert (c2.xvel is 3)
    assert (c2.yvel is 0)
    assert (c2.xpos is 0)
    assert (c2.ypos is 0)
    assert (c2.oid is 2)

    # Setting point S4
    client_ready.set()
    # Waiting at point C5
    server_ready.wait()
    server_ready.clear()
    df.checkout()

    cars = df.read_all(Car)
    assert len(cars) == 3
    assert ("xvel" not in c.__dict__)
    assert ("yvel" not in c.__dict__)
    assert ("xpos" not in c.__dict__)
    assert ("ypos" not in c.__dict__)
    assert ("oid" not in c.__dict__)
    assert (c.xvel is 2)
    assert (c.yvel is 0)
    assert (c.xpos is 0)
    assert (c.ypos is 0)
    assert (c.oid is 0)
    assert ("xvel" not in c1.__dict__)
    assert ("yvel" not in c1.__dict__)
    assert ("xpos" not in c1.__dict__)
    assert ("ypos" not in c1.__dict__)
    assert ("oid" not in c1.__dict__)
    assert (c1.xvel is 2)
    assert (c1.yvel is 0)
    assert (c1.xpos is 0)
    assert (c1.ypos is 0)
    assert (c1.oid is 1)
    assert ("xvel" not in c2.__dict__)
    assert ("yvel" not in c2.__dict__)
    assert ("xpos" not in c2.__dict__)
    assert ("ypos" not in c2.__dict__)
    assert ("oid" not in c2.__dict__)
    assert (c2.xvel is 3)
    assert (c2.yvel is 1)
    assert (c2.xpos is 0)
    assert (c2.ypos is 0)
    assert (c2.oid is 2)

    assert len(df.version_graph.versions) == 6, len(df.version_graph.versions)
    # df.pull()
    # df.checkout()
    # cars = df.read_all(Car)
    # assert (1 == len(cars))
    # c = cars[0]
    # assert ("xvel" not in c.__dict__)
    # assert ("yvel" not in c.__dict__)
    # assert ("xpos" not in c.__dict__)
    # assert ("ypos" not in c.__dict__)
    # assert ("oid" not in c.__dict__)
    # assert (c.xvel is 1)
    # assert (c.yvel is 1)
    # assert (c.xpos is 0)
    # assert (c.ypos is 0)
    # assert (c.oid is 0)
    # df.commit()
    # df.checkout()
    # c.xpos = 1
    # c.ypos = 1
    # c2 = Car(1)
    # df.add_one(Car, c2)
    # df.commit()
    # df.push()
    # # Setting point S2
    # client_ready.set()
    # # Waiting at point C3
    # server_ready.wait()
    # server_ready.clear()
    # df.pull()
    # df.checkout()
    # assert (df.read_one(Car, 1) is None)
    # assert (df.read_one(Car, 2) is None)
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
    
