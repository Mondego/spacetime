
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
from spacetime import Node
from datamodel import BasicType
import time
import multiprocessing as mp
import rpdb

WAIT_FOR_START = 5.0

def producer_server(dataframe, benchmark_q):
    start = time.time()
    while (time.time() - start) < WAIT_FOR_START:
        print ("\rWaiting for %d " % (int(WAIT_FOR_START - (time.time() - start)),), "Seconds for clients to connect.")
        time.sleep(1)

    # rpdb.set_trace()
    dataframe.add_remote('consumer', ('0.0.0.0', 5000))
    dataframe.version_graph.benchmark_q = benchmark_q
    while True:
        st = time.perf_counter()
        if dataframe:
            dataframe.add_one(BasicType, BasicType())
            dataframe.commit()
            # dataframe.push(remote='consumer')
            dataframe.push()
            print(len(dataframe.version_graph.versions), dataframe.version_graph.head)
            # dataframe.pull()
        et = time.perf_counter()
        if (et - st) < 0.8:
            time.sleep(et - st)
#    while True:
#        # dataframe.pull()
#        dataframe.checkout_await()
#        clients = dataframe.read_all(BasicType)
#        print("**", dataframe.client_count, dataframe.versioned_heap)
#        print("++++", dataframe.read_all(BasicType))
#        print(dataframe)
#        print("//", clients)
#        # print(dataframe.sync())

#        time.sleep(1)





def main(port, benchmark_q=None):
    server = Node(producer_server, appname="producer", server_port=port, remotes={}, Types=[BasicType])
    server.benchmark_q = benchmark_q
    server.start_async(benchmark_q=benchmark_q)

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    main(port)