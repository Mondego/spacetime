from rtypes import pcc_set, dimension, primarykey
from spacetime import Node
import uuid, time
from multiprocessing import Event

@pcc_set
class BasicType():
    oid = primarykey(int)
    prop1 = dimension(int)
    prop2 = dimension(int)
    def __init__(self, oid):
        self.oid = oid

def producer(df, count, num_nodes, start_index, started, event, rdetail):
    started.set()
    event.wait()
    rnode, details = rdetail
    print (rnode, details)
    df.add_remote(rnode, details)
    i = start_index
    while i < count:
        df.checkout()
        print (f"Adding BasicType({i})")
        df.add_one(BasicType, BasicType(i))
        df.commit()
        df.push()
        i += num_nodes
    while len(df.read_all(BasicType)) != count:
        df.checkout()
    print ("FINAL COUNT", start_index, [b.oid for b in df.read_all(BasicType)])

producer1 = Node(
    producer, appname='producer1',
    Types=[BasicType],
    server_port=9000)
producer2 = Node(
    producer, appname='producer2',
    Types=[BasicType],
    server_port=9001)
p1_details = ('producer1', ('127.0.0.1', 9000))
p2_details = ('producer2', ('127.0.0.1', 9001))
start1 = Event()
start2 = Event()
start = Event()
producer1.start_async(10, 2, 0, start1, start, p2_details)
producer2.start_async(10, 2, 1, start2, start, p1_details)
start1.wait()
start2.wait()
start.set()
producer1.join()
producer2.join()
