import time, redis

from multiprocessing import Event, Process, Queue
from subprocess import Popen, DEVNULL, check_output

import numpy as np
import sys
import requests

from benchmarks.redis.redis import RedisNode

BasicObject = "benchmarks.datamodel.BasicObject"

def my_print(*args):
    print(*args)
    sys.stdout.flush()

def timeit(func, *args):
    st = time.perf_counter()
    ret = func(*args)
    return time.perf_counter() - st, ret

def read_all(rcon, dtpname, dims):
    rows = rcon.scan_iter(match="{0}*".format(dtpname))
    pipe = rcon.pipeline()
    keys = list()
    for key in rows:
        pipe.hmget(key, dims)
        keys.append(key)
    resps = pipe.execute()
    result = dict()
    for k, v in zip(keys, resps):
        _, oid = k.split(b":")
        oid = oid.decode("utf-8")
        obj_result = dict()
        has_data = True
        if not v:
            continue
        for dim, value in zip(dims, v):
            if value is None:
                has_data = False
                break
            if dim == b"create_ts":
                value = float(v.decode("utf-8"))
            else:
                value = value.decode("utf-8")
            obj_result[dim] = value
        if has_data and obj_result:
            result[oid] = obj_result
    return result

def reader(host, port, reader_id, obj_count, event, rret, read_event):
    Popen(["redis-server", "--port", str(9001+reader_id), "--save", "''", "--replicaof", host, str(port), "--repl-diskless-sync", "yes"], stdout=DEVNULL, stderr=DEVNULL)
    #Popen(["redis-server", "--port", str(9001+reader_id), "--save", "''"], stdout=DEVNULL, stderr=DEVNULL)
    #Popen(["redis-cli", "-p", str(9001+reader_id),
    #      "slaveof", host, str(port)], stdout=DEVNULL, stderr=DEVNULL)
    read_event.set()
    event.wait()
    time.sleep(2)
    rcon = redis.Redis(host='127.0.0.1', port=9001+reader_id)
    op = 0
    timestamps = dict()
    action_times = list()
    st = time.perf_counter()
    while True:
        #action_times.append(pull(df))
        at, objs = timeit(read_all, rcon, BasicObject, ["oid", "create_ts"])
        read_ts = time.time()
        action_times.append(at)
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
                    print(final_ts[oid])

    rret.put((sum(action_times) / len(action_times), sum(final_ts.values()) / obj_count, op, et - st))
    #return 0.0, op, et - st


def write(host, port, oids, event, wret):
    event.wait()
    wcon = redis.Redis(host=host, port=port)
    op = 0
    oids = list(oids)
    action_times = list()
    st = time.perf_counter()
    for oid in oids:
        at_s = time.perf_counter()
        pipe = wcon.pipeline(transaction=True)
        obj = {"oid": oid, "create_ts": time.time()}
        pipe.hmset(f"{BasicObject}:{oid}", obj)
        pipe.execute()
        action_times.append(time.perf_counter() - at_s)
        #print ("Writing object", oid)
        op += 1
    wret.put((sum(action_times)/ len(action_times), op, time.perf_counter() - st))

def server(port, end_e):
    try:
        server_process = Popen(["redis-server", "--port", str(port), "--save", "''"], stdout=DEVNULL, stderr=DEVNULL)
        end_e.wait()
    finally:
        server_process.kill()


def run_expt(args, num_prod):
    # print (num_prod, obj_count, bot_count)
    obj_count, bot_count = args.objcount, args.nodecount
    num_prod = int(0.1*num_prod*bot_count)
    end_e = Event()
    if args.run_server:
        main_node = Process(target=server, args=(9000,end_e), daemon=True)
        main_node.start()
    else:
        resp = requests.get(f"http://{args.host}:10000/redis_start")
        if not resp:
            raise RuntimeError("Server not started")
    e = Event()
    readers = list()
    writers = list()
    wret = Queue()
    for i in range(num_prod):
        write_node = Process(target=write, args=(args.host, 9000, range(i, obj_count, num_prod), e, wret))
        writers.append(write_node)
        write_node.start()
    
    rret = Queue()
    read_events = list()
    for i in range(bot_count - num_prod):
        read_event = Event()
        read_events.append(read_event)
        read_node = Process(target=reader, args=(args.host, 9000, i, obj_count, e, rret, read_event))
        read_node.start()
        readers.append(read_node)
    # Lets start!
    for revent in read_events:
        revent.wait()
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
    
    # print (f"{num_prod}: DL:{np.mean(avgs)*1000 if avgs else 0.0:.4f}ms, RL:{np.mean(ract_avgs)*1000 if ract_avgs else 0.0:.4f}, WL:{np.mean(wact_avgs)*1000 if wact_avgs else 0.0:.4f}, RTPS: {rtps:.4f}, WTPS: {wtps:.4f} OP_COUNT: {rtps + wtps:.4f}, ROP: {rcount}, WOP: {wcount}")
    if args.run_server:
        end_e.set()
        main_node.join()
    else:
        resp = requests.get(f"http://{args.host}:10000/redis_stop")
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

def run(args, num_prod):
    return run_expt(args, num_prod)
