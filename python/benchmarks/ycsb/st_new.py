import time

from multiprocessing import Event
from benchmarks.datamodel import Stop, BasicObject
from spacetime import Node
import numpy as np
import sys
import requests

def my_print(*args):
    print(*args)
    sys.stdout.flush()

def pull(df):
    st = time.perf_counter()
    df.pull()#instrument=True)
    return time.perf_counter() - st

def pull_await(df):
    st = time.perf_counter()
    df.pull()
    return time.perf_counter() - st

def reader(df, obj_count, event, i, await_pull):
    event.wait()
    op = 0
    timestamps = dict()
    action_times = list()
    prev_package = dict()
    ttime = time.perf_counter()
    st = time.perf_counter()
    while True:
        tdelta = pull_await(df) if await_pull else pull(df)
        #if package == prev_package:
        #    if time.perf_counter() - ttime > 20:
        #        print (len(df.read_all(BasicObject)), package, df.versioned_heap.head)
        #        raise RuntimeError(f"This client {i} is not pulling changes")
        #else:
        #ttime = time.perf_counter()
        #prev_package = package

        action_times.append(tdelta)
        read_ts = time.time()
        objs = df.read_all(BasicObject)
        if objs:
            timestamps[read_ts] = objs
        if len(objs) == obj_count:
            break
        op += 1
    et = time.perf_counter()
    final_ts = dict()
    for ts, objs in sorted(timestamps.items(), key=lambda x: x[0]):
        for obj in objs:
            if obj.oid not in final_ts:
                final_ts[obj.oid] = ts - obj.create_ts
                if final_ts[obj.oid] < 0:
                    print(final_ts[obj.oid])


    return sum(action_times) / len(action_times), sum(final_ts.values())/ obj_count, op, et - st
    #return 0.0, op, et - st

def push(df):
    st = time.perf_counter()
    df.push()
    return time.perf_counter() - st, None


def write(df, oids, event):
    event.wait()
    op = 0
    oids = list(oids)
    action_times = list()
    st = time.perf_counter()
    for oid in oids:
        df.add_one(BasicObject, BasicObject(oid))
        df.commit()
        tdelta, _ = push(df)
        action_times.append(tdelta)
        
        op += 1
    return sum(action_times)/ len(action_times), op, time.perf_counter() - st

def server(df, end_e):
    end_e.wait()

def run_expt(args, num_prod, await_pull=False, cpp=False):
    obj_count, bot_count = args.objcount, args.nodecount
    num_prod = int(0.1*num_prod*bot_count)
    end_e = Event()
    if args.run_server:
        main_node = Node(server, Types=[BasicObject], server_port=args.port)
        main_node.start_async(end_e)
    else:
        url = f"http://{args.host}:10000/st_start" if not cpp else f"http://{args.host}:10000/st_cpp_start"
        resp = requests.get(url)
        if not resp:
            raise RuntimeError("Server not started")
    e = Event()
    readers = list()
    writers = list()
    for i in range(num_prod):
        write_node = Node(write, Types=[BasicObject], dataframe=(args.host, args.port), pure_python=not cpp)
        writers.append(write_node)
        write_node.start_async(range(i, obj_count, num_prod), e)
    
    for i in range(bot_count - num_prod):
        read_node = Node(reader, Types=[BasicObject], dataframe=(args.host, args.port), pure_python=not cpp)
        read_node.start_async(obj_count, e, i, await_pull)
        readers.append(read_node)
    # Lets start!
    e.set()
    wnode_values = [wnode.join() for wnode in writers]
    rnode_values = [rnode.join() for rnode in readers]
    avgs = list()
    ract_avgs = list()
    wact_avgs = list()
    rtps = 0.0
    wtps = 0.0
    rcount = 0
    wcount = 0
    for ract_avg, avg, rop_count, rtdiff in rnode_values:
        ract_avgs.append(ract_avg)
        avgs.append(avg)
        rtps += (rop_count/rtdiff)
        rcount += rop_count

    for wact_avg, wop_count, wtdiff in wnode_values:
        wact_avgs.append(wact_avg)
        wtps += wop_count/wtdiff
        wcount += wop_count
    if args.run_server:
        end_e.set()
        main_node.join()
    else:
        resp = requests.get(f"http://{args.host}:10000/st_stop")
        if not resp:
            raise RuntimeError("Server not shutdown.")
    return {
        "update_latency": (np.mean(avgs) if avgs else 0.0, np.std(avgs) if avgs else 0.0),
        "read_latency": (np.mean(ract_avgs) if ract_avgs else 0.0, np.std(ract_avgs) if ract_avgs else 0.0),
        "write_latency": (np.mean(wact_avgs) if wact_avgs else 0.0, np.std(wact_avgs) if wact_avgs else 0.0),
        "read_tps": rtps,
        "write_tps": wtps,
        "read_ops": rcount,
        "write_count": wcount
    }

def run(args, num_prod, await_pull=False, cpp=False):
    #for i in range(bot_count):
    return run_expt(args, num_prod, await_pull, cpp)
