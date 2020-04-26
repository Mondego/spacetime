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

def producer(df, count, num_nodes, start_index, started, event, rdetails):
    started.set()
    event.wait()
    for rnode, details in rdetails:
        df.add_remote(rnode, details)
    i = start_index
    while i < count:
        df.checkout()
        df.add_one(BasicType, BasicType(i))
        df.commit()
        i += num_nodes
        if i < count:
            df.push()
        else:
            df.push_await()

    while len(df.read_all(BasicType)) != count:
        df.checkout()
        print ([b.oid for b in df.read_all(BasicType)])
    print ("FINAL COUNT", start_index, [b.oid for b in df.read_all(BasicType)])

NUM_PRODUCERS = 3
TOTAL_OBJ_COUNT = 12

PRODUCERS = [
    (Node(
        producer,
        appname=f'producer{i}', Types=[BasicType],
        server_port=9000+i,
        log_to_std=True, log_to_file=True),
     (f'producer{i}', ('127.0.0.1', 9000+i)),
     Event(), i)
    for i in range(NUM_PRODUCERS)]
DETAILS = {p[1] for p in PRODUCERS}

START = Event()
for pnode, detail, event, i in PRODUCERS:
    pnode.start_async(
        TOTAL_OBJ_COUNT, NUM_PRODUCERS, i, event, START, DETAILS - {detail})
for _, _, event, _ in PRODUCERS:
    event.wait()
START.set()
for pnode, _, _, _ in PRODUCERS:
    pnode.join()
