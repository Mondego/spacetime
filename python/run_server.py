import time, os

from multiprocessing import Event, Process
from benchmarks.datamodel import BasicObject
from benchmarks.ycsb.mp_new import server as mp_server
from benchmarks.ycsb.mp2_new import server as mp2_server
from spacetime import Node
import numpy as np
import sys
from flask import Flask

import time, redis

from subprocess import Popen, DEVNULL

def st_server(df, end_e):
    end_e.wait()

app = Flask(__name__)
end_e = Event()

@app.route('/st_start')
def st_start():
    print ("Starting st server.")
    global main_node
    end_e.clear()
    main_node = Node(st_server, Types=[BasicObject], server_port=9000, pure_python=True)
    main_node.start_async(end_e)
    return ""

@app.route('/st_stop')
def st_stop():
    print ("Stopping st server.")
    end_e.set()
    main_node.join()
    return ''

@app.route('/st_cpp_start')
def st_cpp_start():
    print ("Starting st server.")
    global main_node
    end_e.clear()
    main_node = Node(st_server, Types=[BasicObject], server_port=9000, pure_python=False)
    main_node.start_async(end_e)
    return ""

def redis_server(port, end_e):
    try:
        if os.path.exists("dump.rdb"):
            os.remove("dump.rdb")
        server_process = Popen(["redis-server", "--port", str(port), "--save", "''", "--bind", "0.0.0.0"], stdout=DEVNULL, stderr=DEVNULL)
        end_e.wait()
    finally:
        server_process.kill()


@app.route('/redis_start')
def redis_start():
    global main_node
    end_e.clear()
    main_node = Process(target=redis_server, args=(9000, end_e))
    main_node.start()
    return ""

@app.route('/redis_stop')
def redis_stop():
    end_e.set()
    main_node.join()
    return ''

@app.route('/mp_start')
def mp_start():
    global main_node
    start_e = Event()
    end_e.clear()
    main_node = Process(target=mp_server, args=(9000, start_e, end_e))
    main_node.start()
    start_e.wait()
    return ""

@app.route('/mp_stop')
def mp_stop():
    end_e.set()
    main_node.join()
    return ''

@app.route('/mp2_start')
def mp2_start():
    global main_node
    start_e = Event()
    end_e.clear()
    main_node = Process(target=mp2_server, args=(9000, start_e, end_e))
    main_node.start()
    start_e.wait()
    return ""

@app.route('/mp2_stop')
def mp2_stop():
    end_e.set()
    main_node.join()
    return ''

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
