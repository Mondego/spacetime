from spacetime import Node, Dataframe, DebugDataframe
from rtypes import pcc_set, primarykey, dimension, merge
from spacetime import Register, server_func, CommitObj, AcceptFetchObj, FetchObj, CheckoutObj, PushObj, AcceptPushObj
import time
@pcc_set
class Foo:
    y = primarykey(int)

    def __init__(self, y):
        self.y = y

    def __str__(self):
        return "Foo_"+ str(self.y)

def producer(df):
    for y in range(1):
        df.add_one(Foo, Foo(y))
        df.commit()
        df.push()
        time.sleep(10)

def consumer(df):
    time.sleep(5)
    foos = list()
    df.checkout()
    foos = df.read_all(Foo)
    print("in consumer")
    print(foos)
    for foo in foos:
        print(foo)

def main():

    n=1

    # To start central debugger node
    debugger_server = Node(server_func, Types=[Register, CommitObj, AcceptFetchObj, FetchObj, CheckoutObj, AcceptPushObj, PushObj], server_port=30000)

    debugger_server.start_async()
    print("Debugger server started")
    consumer_node = Node(consumer, Types=[Foo], server_port=65402, debug=("127.0.0.1", 30000))
    consumer_node.start_async()
    consumer_node.join()

    producer_nodes = [0]*n
    # To start child apps
    for i in range(n):
        producer_nodes[i] = Node(producer, Types=[Foo], debug=("127.0.0.1", 30000),dataframe=('127.0.0.1', 65402))
        producer_nodes[i].start_async()

    for i in range(n):
        producer_nodes[i].join()

main()