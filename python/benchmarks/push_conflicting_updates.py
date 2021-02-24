import time

from benchmarks.datamodel import Stop, BasicCounter, counter_merge
from spacetime import Node

def consumer(df, obj_count, num_consumers, count_upto):
    while len(df.read_all(Stop)) != num_consumers:
        df.checkout_await()

    count_objs = [BasicCounter(i) for i in range(obj_count)]
    df.add_many(BasicCounter, count_objs)
    for stop in df.read_all(Stop):
        stop.start = True
    df.commit()
    
    start_ts = time.perf_counter()
    # lost_time = 0
    while min(o.count for o in count_objs) < count_upto:
        df.checkout_await()
        # update_ts = time.perf_counter()
        # timestamps[update_ts - lost_time] = [o.count for o in count_objs]
        # lost_time += time.perf_counter() - update_ts
    end_ts = time.perf_counter()

    while any(not s.accepted for s in df.read_all(Stop)):
        df.checkout_await()
    # Number of conflicting updates received per second.
    return sum(o.count for o in count_objs), (end_ts - start_ts)
    

def updater(df, count_upto, index):
    stop = Stop(index)
    df.add_one(Stop, stop)
    df.commit()
    df.push_await()
    while not stop.start:
        df.pull_await()
        count_objs = df.read_all(BasicCounter)
    lost_time = 0
    while min(o.count for o in count_objs) < count_upto:
        st_time = time.perf_counter()
        for o in count_objs:
            o.count += 1
        lost_time += time.perf_counter() - st_time
        df.commit()
        df.push()
        df.pull()
    stop.accepted = True
    df.commit()
    df.push_await()
    return lost_time

def run_bench(obj_count, num_consumers, rn):
    count_upto = 100
    consumer_node = Node(consumer, Types=[BasicCounter, Stop], server_port=9000+rn, resolver={BasicCounter: counter_merge})
    consumer_node.start_async(obj_count, num_consumers, count_upto)
    updaters = [Node(updater, Types=[BasicCounter, Stop], dataframe=("127.0.0.1", 9000+rn), resolver={BasicCounter: counter_merge}) for i in range(num_consumers)]
    #print ("Consumers created.")
    for i, upd_node in enumerate(updaters):
        upd_node.start_async(count_upto, i)
    #print ("Setting the event.")
    avg_lost_time = sum(upd.join() for upd in updaters)/num_consumers
    transactions, time_taken = consumer_node.join()
    return [(time_taken - avg_lost_time)*obj_count / (transactions)]
    