import time, cbor

from multiprocessing import Event, Process, Queue
from subprocess import Popen, DEVNULL, check_output
from spacetime.utils.rwlock import RWLockFair as RWLock
from threading import Thread
import socket
import numpy as np
import sys
import requests
from struct import pack, unpack, error

BasicObject = "benchmarks.datamodel.BasicObject"


def my_print(*args):
    print(*args)
    sys.stdout.flush()


def receive_data(con, where, delay=False):
    try:
        #print ("Receivng data", where)
        raw_cl = con.recv(4)
        #print (raw_cl, where)
        length = unpack("!L", raw_cl)[0]
        stack = list()
        #print ("downloading data", length, where)
        while length:
            data = con.recv(length)
            stack.append(data)
            length = length - len(data)
        rawdata = b"".join(stack)
        #print (rawdata, where)
        finaldata = cbor.loads(rawdata)
        #print ("sending ack", finaldata, where)
        if not delay:
            con.send(pack("!?", True))
        #print ("done", where)
        return finaldata
    except error:
        return None

def send_ack(con):
    con.send(pack("!?", True))

def send_all(con, data):
    try:
        succ = False
        #print (data)
        while not succ:
            raw = cbor.dumps(data)
            raw_sent = pack("!L", len(raw))
            #print (raw_sent)
            con.send(raw_sent)
            #print ("Sending", data, raw)
            while raw:
                sent = con.send(raw)
                if len(raw) == sent:
                    break
                raw = raw[sent:]
            #print ("Wating for ack", data)
            succ = unpack("!?", con.recv(1))[0]
            #print (succ)
        return True
    except error:
        return False


# sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     sync_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#     sync_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#     sync_socket.bind(("", 9001+reader_id))
#     sync_socket.listen()
#     con, _ = sync_socket.accept()
    

def reader(host, port, reader_id, obj_count, event, rret, read_event):
    req_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    req_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    req_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # req_socket.settimeout(2)
    req_socket.connect((host, port))
    if not send_all(req_socket, [reader_id, "READER"]):
        raise RuntimeError("Broken reader connection")
    read_event.set()
    event.wait()
    #print ("Starting ", reader_id)
    op = 0
    timestamps = dict()
    action_times = list()
    st = time.perf_counter()
    all_objs = dict()
    while True:
        #action_times.append(pull(df))
        read_st = time.perf_counter()
        if not send_all(req_socket, reader_id):
            raise RuntimeError("Broken reader connection")
        objs = receive_data(req_socket, "reader")
        action_times.append(time.perf_counter() - read_st)
        if objs is None and len(all_objs) != obj_count:
            raise RuntimeError("Error reader")
        read_ts = time.time()
        if objs:
            timestamps[read_ts] = objs
            all_objs.update(objs)
        if len(all_objs) == obj_count:
            break
        # else:
        #     print (reader_id, " only has ", len(all_objs), " objects.")
        op += 1
    et = time.perf_counter()
    final_ts = dict()
    for ts, objs in sorted(timestamps.items(), key=lambda x: x[0]):
        for oid in objs:
            if oid not in final_ts:
                final_ts[oid] = ts - float(objs[oid]["create_ts"])
                if final_ts[oid] < 0:
                    my_print(final_ts[oid], "broken 1")
    #print ("Reader", reader_id, "is done")
    rret.put((sum(action_times) / len(action_times), sum(final_ts.values()) / obj_count, op, et - st))
    #return 0.0, op, et - st


def write(host, port, oids, event, wret):
    req_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    req_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    req_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # req_socket.settimeout(2)
    req_socket.connect((host, port))
    if not send_all(req_socket, [0, "WRITER"]):
        raise RuntimeError("Broken writer connection")
    event.wait()
    op = 0
    oids = list(oids)
    action_times = list()
    st = time.perf_counter()
    for oid in oids:
        at_s = time.perf_counter()
        obj = {oid: {"oid": oid, "create_ts": time.time()}}
        if not send_all(req_socket, obj):
            raise RuntimeError("Broken writer connection at obj send")
        action_times.append(time.perf_counter() - at_s)
        #print ("Writing object", oid)
        op += 1
    wret.put((sum(action_times)/ len(action_times), op, time.perf_counter() - st))

def server(port, start_e, end_e):
    try:
        server_process = Process(target=serve, args=(port,), daemon=True)
        server_process.start()
        start_e.set()
        end_e.wait()
    finally:
        server_process.terminate()

class State(object):
    def __init__(self):
        self.clients = dict()
        self.client_locks = dict()
        self.client_queue = dict()
        self.readers = list()

    def add_client(self, client_id, client_con, reader_t):
        self.client_locks[client_id] = RWLock()
        self.clients[client_id] = client_con
        self.client_queue[client_id] = dict()
        self.client_read = dict()
        self.client_write = dict()
        self.readers.append(reader_t)

    def write(self, data):
        for cid, con in self.clients.items():
            with self.client_locks[cid].gen_wlock():
                self.client_queue[cid].update(data)


    def get_data(self, client_id):
        with self.client_locks[client_id].gen_wlock():
            data = self.client_queue[client_id]
            self.client_queue[client_id] = dict()
        return data

def serve(port):
    state = State()
    sync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sync_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sync_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sync_socket.bind(("", port))
    sync_socket.listen()
    writers = list()
    while True:
        con, _ = sync_socket.accept()
        writer_T = Thread(target=receive_updates, args=(con, state), daemon=True)
        writer_T.start()
        writers.append(writer_T)

def serve_client(con, state):
    while True:
        #my_print ("Serving client")
        req = receive_data(con, "serve_client")
        #my_print ("Serving client", req)
        if req is None:
            break
        data = state.get_data(req)
        #my_print ("Responding with ", data)
        send_all(con, data)

def receive_updates(con, state):
    resp = receive_data(con, "from accept", delay=True)
    if resp is None:
        #my_print ("Broken accept connection")
        return
    cid, tp = resp
    if tp != "WRITER":
        reader_T = Thread(target=serve_client, args=(con, state), daemon=True)
        reader_T.start()
        state.add_client(cid, con, reader_T)
        send_ack(con)
        return
    send_ack(con)
    while True:
        data = receive_data(con, "from writer")
        #my_print (data)
        if data is None:
            break
        if data == "STOP":
            #my_print ("Stopping writer")
            break
        state.write(data)


def run_expt(args, num_prod):
    # print (num_prod, obj_count, bot_count)
    obj_count, bot_count = args.objcount, args.nodecount
    num_prod = int(0.1*num_prod*bot_count)
    end_e = Event()
    if args.run_server:
        main_node = Process(target=server, args=(9000,end_e), daemon=True)
        main_node.start()
    else:
        resp = requests.get(f"http://{args.host}:10000/mp2_start")
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
    #my_print ("launching all nodes")
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
        resp = requests.get(f"http://{args.host}:10000/mp2_stop")
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
