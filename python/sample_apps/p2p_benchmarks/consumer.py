from pprint import pprint
import os
import sys
import json
import time
from datamodel import BasicType

# hack to add latest spacetime stuff to $PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from spacetime import Node


def consumer_client(df, benchmark_q):
    df.add_remote('producer', ('127.0.0.1', 8000))
    df.version_graph.benchmark_q = benchmark_q
    print(df.version_graph.benchmark_q)
    print("/// ", df.details)
    while True:
        st = time.perf_counter()
        if df:
            # df.add_one(BasicType, BasicType())
            # df.commit()
            # df.fetch()
            df.checkout()
            bt = df.read_all(BasicType)
            # print(len(bt), len(df.version_graph.versions), df.version_graph.versions)
            # benchmark_q.put(len(bt)) if benchmark_q else None
        et = time.perf_counter()
        if (et - st) < 0.8:
            time.sleep(et - st)

def main(port, benchmark_q=None):
    consumer_node = Node(consumer_client, appname="consumer", Types=[BasicType], server_port=port, remotes={})
    # consumer_node.benchmark_q = benchmark_q
    consumer_node.start_async(benchmark_q=benchmark_q)

if __name__ == "__main__":
    port = 5000
    if len(sys.argv) >= 2:
        port = sys.argv[1]
    main(port)
