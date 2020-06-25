import time, redis

from multiprocessing import Event, Process, Queue
from subprocess import Popen

import numpy as np
import sys, requests

import mysql.connector

BasicObject = "benchmarks.datamodel.BasicObject"

user = "rohan"
password = "nahor1989"

def my_print(*args):
    print(*args)
    sys.stdout.flush()

def timeit(func, *args):
    st = time.perf_counter()
    ret = func(*args)
    return time.perf_counter() - st, ret

def read_all(rcon):
    query = "SELECT oid, create_ts FROM `benchmarks.datamodel.BasicObject`;"
    cursor = rcon.cursor()
    cursor.execute(query)
    results = dict()
    for oid, create_ts in cursor.fetchall():
        results[oid] = {"oid": oid, "create_ts": create_ts}
    rcon.commit()
    cursor.close()
    #rcon.consume_results()
    return results

def reader(host, port, obj_count, event, rret):
    event.wait()
    rcon = mysql.connector.connect(
        user=user, password=password,
        host=host, database="benchmarks", auth_plugin='mysql_native_password')
    op = 0
    timestamps = dict()
    action_times = list()
    st = time.perf_counter()
    while True:
        #action_times.append(pull(df))
        at, objs = timeit(read_all, rcon)
        read_ts = time.time()
        action_times.append(at)
        #print (at, objs)
        if objs:
            timestamps[read_ts] = objs
        if len(objs) == obj_count:
            break
        op += 1
    et = time.perf_counter()
    final_ts = dict()
    for ts, objs in sorted(timestamps.items(), key=lambda x: x[0]):
        for oid in objs:
            if oid not in final_ts:
                final_ts[oid] = ts - float(objs[oid]["create_ts"])
                if final_ts[oid] < 0:
                    print("Wrong ts", final_ts[oid])

    rret.put((sum(action_times) / len(action_times), sum(final_ts.values()) / obj_count, op, et - st))
    rcon.close()
    #return 0.0, op, et - st


def write(host, port, oids, event, wret):
    event.wait()
    wcon = mysql.connector.connect(
        user=user, password=password,
        host=host, database="benchmarks", auth_plugin='mysql_native_password')
    op = 0
    oids = list(oids)
    action_times = list()
    st = time.perf_counter()
    for oid in oids:
        at_s = time.perf_counter()
        cursor = wcon.cursor()
        query = "INSERT INTO `benchmarks.datamodel.BasicObject` VALUES (%s, %s);"
        cursor.execute(query, (oid, time.time()))
        cursor.close()
        wcon.commit()
        action_times.append(time.perf_counter() - at_s)
        op += 1
    wret.put((sum(action_times)/ len(action_times), op, time.perf_counter() - st))
    wcon.close()

def server(host):
    con = mysql.connector.connect(
        user=user, password=password,
        host=host, database="benchmarks", auth_plugin='mysql_native_password')
    cursor = con.cursor()
    cursor.execute(
        f"create table if not exists `benchmarks.datamodel.BasicObject` (oid INT, create_ts DOUBLE, PRIMARY KEY (oid));")
    cursor.execute(f"DELETE from `benchmarks.datamodel.BasicObject`;")
    con.commit()
    con.close()

def run_expt(args, num_prod):
    obj_count, bot_count = args.objcount, args.nodecount
    num_prod = int(0.1*num_prod*bot_count)
    server(args.host)
    e = Event()
    readers = list()
    writers = list()
    wret = Queue()
    for i in range(num_prod):
        write_node = Process(target=write, args=(args.host, 9000, range(i, obj_count, num_prod), e, wret))
        writers.append(write_node)
        write_node.start()
    
    rret = Queue()
    for i in range(bot_count - num_prod):
        read_node = Process(target=reader, args=(args.host, 9000, obj_count, e, rret))
        read_node.start()
        readers.append(read_node)
    # Lets start!
    e.set()
    wnode_values = list()
    rnode_values = list()
    for wnode in writers:
        wnode.join()
        wnode_values.append(wret.get())
    for rnode in readers:
        rnode.join()
        rnode_values.append(rret.get())
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
    return {
        "update_latency": (np.mean(avgs) if avgs else 0.0, np.std(avgs) if avgs else 0.0),
        "read_latency": (np.mean(ract_avgs) if ract_avgs else 0.0, np.std(ract_avgs) if ract_avgs else 0.0),
        "write_latency": (np.mean(wact_avgs) if wact_avgs else 0.0, np.std(wact_avgs) if wact_avgs else 0.0),
        "read_tps": rtps,
        "write_tps": wtps,
        "read_ops": rcount,
        "write_count": wcount
    }
    # print (f"{num_prod}: DL:{np.mean(avgs)*1000 if avgs else 0.0:.4f}ms, RL:{np.mean(ract_avgs)*1000 if ract_avgs else 0.0:.4f}, WL:{np.mean(wact_avgs)*1000 if wact_avgs else 0.0:.4f}, RTPS: {rtps:.4f}, WTPS: {wtps:.4f} OP_COUNT: {rtps + wtps:.4f}, ROP: {rcount}, WOP: {wcount}")


def run(args, num_prod):
    #for i in range(bot_count):
    return run_expt(args, num_prod)
