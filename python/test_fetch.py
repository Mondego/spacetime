from spacetime import Node, Dataframe, DebugDataframe
from rtypes import pcc_set, primarykey, dimension, merge
from spacetime import Register, server_func, CommitObj, AcceptFetchObj, FetchObj, CheckoutObj, PushObj, \
    AcceptPushObj, Vertex, Edge, Parent
import time

@pcc_set
class Foo:
    y = primarykey(int)
    z = dimension(int)

    def __init__(self, y):
        self.y = y
        self.z = y + 10

    def __str__(self):
        return "Foo_"+ str(self.y)


def producer(df, i):
    y = i
    x= 0
    while True:
        df.pull()
        print ("LENGTH OF FOOOOOOOS PRODUCER", i, len(df.read_all(Foo)))
        if x >= 10:
            break

def consumer(df):
    foos = list()
    x = 0
    while True:
        df.add_one(Foo, Foo(x))
        df.commit()
        x += 1
        if len(df.read_all(Foo)) >= 20:
            break
        #print(foos)
        #for foo in foos:
        #    print("Consumer received",foo)
        

def main():

    n = 2

    #To start central debugger node
    debugger_server = Node(server_func, Types=[Register, CommitObj, AcceptFetchObj, FetchObj, CheckoutObj,
                                               AcceptPushObj, PushObj, Vertex, Edge, Parent], server_port=30000)

    debugger_server.start_async([Foo])
    print("Debugger server started")
    consumer_node = Node(consumer, Types=[Foo], server_port=65402, debug=('127.0.0.1', 30000))
    consumer_node.start_async()


    producer_nodes = [0] * n
    # To start child apps
    for i in range(n):
        producer_nodes[i] = Node(producer, Types=[Foo], dataframe=('127.0.0.1', 65402), debug=('127.0.0.1', 30000))
        producer_nodes[i].start_async(i)

    for i in range(n):
        producer_nodes[i].join()

    consumer_node.join()

main()