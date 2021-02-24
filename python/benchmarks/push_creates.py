import time

from benchmarks.datamodel import Stop, BasicObject
from spacetime import Node

def consumer(df, obj_count, num_consumers):
    while len(df.read_all(Stop)) != num_consumers:
        df.checkout_await()
    for stop in df.read_all(Stop):
        stop.start = True
    df.commit()
    
    timestamps = dict()
    while True:
        read_ts = time.time()
        objs = df.read_all(BasicObject)
        if objs:
            timestamps[read_ts] = objs
        if len(objs) == obj_count:
            break
        df.checkout_await()

    while any(not s.accepted for s in df.read_all(Stop)):
        df.checkout_await()
    
    final_ts = dict()
    for ts, objs in sorted(timestamps.items(), key=lambda x: x[0]):
        for obj in objs:
            if obj.oid not in final_ts:
                final_ts[obj.oid] = ts - obj.create_ts
    assert len(final_ts) == obj_count
    return sum(final_ts.values()) / obj_count

def producer(df, obj_ids, index):
    stop = Stop(index)
    df.add_one(Stop, stop)
    df.commit()
    df.push_await()
    while not stop.start:
        df.pull_await()
    for i in obj_ids:
        df.add_one(BasicObject, BasicObject(i, b""))
        df.commit()
        df.push()
    stop.accepted = True
    df.commit()
    df.push_await()

def run_bench(obj_count, num_consumers, rn):
    consumer_node = Node(consumer, Types=[BasicObject, Stop], server_port=9000+rn)
    consumer_node.start_async(obj_count, num_consumers)
    producers = [Node(producer, Types=[BasicObject, Stop], dataframe=("127.0.0.1", 9000+rn)) for i in range(num_consumers)]
    #print ("Consumers created.")
    for i, pro_node in enumerate(producers):
        pro_node.start_async(list(range(i,obj_count,num_consumers)), i)
    #print ("Setting the event.")
    for pro in producers:
        pro.join()
    return [consumer_node.join()]
    