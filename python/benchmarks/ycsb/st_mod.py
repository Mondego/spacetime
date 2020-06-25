import time

from multiprocessing import Event
from benchmarks.datamodel import Stop, BasicCounter
from spacetime import Node
import numpy as np
import sys

def my_print(*args):
    print(*args)
    sys.stdout.flush()

def pull(df):
    st = time.perf_counter()
    df.pull()
    return time.perf_counter() - st

def reader(df, obj_count, event):
    objs = df.read_all(BasicCounter)
    while not objs:
        df.pull_await()
        objs = df.read_all(BasicCounter)
    event.wait()
    op = 0
    action_times = list()
    st = time.perf_counter()
    while any(o.count < 1000 for o in objs):
        action_times.append(pull(df))
        op += 1
        #my_print(f"COUNT: {list(o.count for o in objs)}")
    et = time.perf_counter()

    return sum(action_times) / len(action_times), op, et - st
    #return 0.0, op, et - st

def push(df):
    st = time.perf_counter()
    df.push()
    return time.perf_counter() - st


def write(df, oids, event):
    objs = df.read_all(BasicCounter)
    while not objs:
        df.pull_await()
        objs = df.read_all(BasicCounter)
    event.wait()
    op = 0
    oids = list(oids)
    action_times = list()
    st = time.time()
    count = 0
    while count < 1000:
        count += 1
        for oid in oids:
            df.read_one(BasicCounter, oid).count = count
        df.commit()
        op += 1
        action_times.append(push(df))
        
        #my_print(f"COUNT: {count}")
    return sum(action_times)/ len(action_times), op, time.time() - st

def server(df, end_e, obj_count):
    df.add_many(BasicCounter, [BasicCounter(i) for i in range(obj_count)])
    df.commit()
    end_e.wait()

def run_expt(num_prod, obj_count, bot_count):
    end_e = Event()
    main_node = Node(server, Types=[BasicCounter], server_port=9000)
    main_node.start_async(end_e, obj_count)
    e = Event()
    readers = list()
    writers = list()
    for i in range(num_prod):
        write_node = Node(write, Types=[BasicCounter], dataframe=main_node.details)
        writers.append(write_node)
        write_node.start_async(range(i, obj_count, num_prod), e)
    
    for i in range(bot_count - num_prod):
        read_node = Node(reader, Types=[BasicCounter], dataframe=main_node.details)
        read_node.start_async(obj_count, e)
        readers.append(read_node)
    # Lets start!
    e.set()
    wnode_values = [wnode.join() for wnode in writers]
    rnode_values = [rnode.join() for rnode in readers]
    ract_avgs = list()
    wact_avgs = list()
    rtps = 0.0
    wtps = 0.0
    for ract_avg, rop_count, rtdiff in rnode_values:
        ract_avgs.append(ract_avg)
        rtps += (rop_count/rtdiff)

    for wact_avg, wop_count, wtdiff in wnode_values:
        wact_avgs.append(wact_avg)
        wtps += wop_count/wtdiff

    print (f"{num_prod}: RL:{np.mean(ract_avgs)*1000 if ract_avgs else 0.0:.4f}, WL:{np.mean(wact_avgs)*1000 if wact_avgs else 0.0:.4f}, RTPS: {rtps:.4f}, WTPS: {wtps:.4f} OP_COUNT: {rtps + wtps:.4f}")
    end_e.set()
    main_node.join()

def run(obj_count, bot_count):
    for i in range(bot_count):
        run_expt(i+1, obj_count, bot_count)
