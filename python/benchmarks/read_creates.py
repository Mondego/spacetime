import time

from benchmarks.datamodel import Stop, BasicObject
from spacetime import Node

def producer(df, obj_count, num_consumers):
    while len(df.read_all(Stop)) != num_consumers:
        df.checkout_await()
    for i in range(obj_count):
        df.add_one(BasicObject, BasicObject(i))
        df.commit()
    while any(not s.accepted for s in df.read_all(Stop)):
        df.checkout_await()
    #print ("Completed producer")

def consumer(df, obj_count, index):
    stop = Stop(index)
    df.add_one(Stop, stop)
    df.commit()
    df.push_await()
    obj = None
    i = 0
    record = list()
    while i < obj_count:
        df.pull_await()
        read_t = time.time()
        obj = df.read_one(BasicObject, i)
        while obj:
            record.append(read_t - obj.create_ts)
            i += 1
            obj = df.read_one(BasicObject, i)
    avg = sum(record)/len(record)
    #print (f"{index}: {avg}")
    stop.accepted = True
    df.commit()
    df.push_await()
    #print ("Completed consumer: ", index)
    return avg

def run_bench(obj_count, num_consumers, rn):
    producer_node = Node(producer, Types=[BasicObject, Stop], server_port=9000+rn)
    producer_node.start_async(obj_count, num_consumers)
    consumers = [Node(consumer, Types=[BasicObject, Stop], dataframe=("127.0.0.1", 9000+rn)) for i in range(num_consumers)]
    #print ("Consumers created.")
    for i, con_node in enumerate(consumers):
        con_node.start_async(obj_count, i)
    #print ("Setting the event.")
    producer_node.join()
    return [con.join() for con in consumers]