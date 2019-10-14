from spacetime import Node, Dataframe, DebugDataframe
from rtypes import pcc_set, primarykey, dimension, merge


import time


@pcc_set
class Foo:
    y = primarykey(int)

    def __init__(self, y):
        self.y = y

    def __str__(self):
        return "Foo_" + str(self.y)


def producer(df):
    print("in producer")
    i = 0
    while True:
        df.add_one(Foo, Foo(i))
        df.commit()
        time.sleep(1)
        i += 1


def consumer(df):
    while True:
        df.pull_await()
        foos = df.read_all(Foo)
        print(foos)


def main():
    n = 1

    # To start central debugger node
    # debugger_server = Node(server_func, Types={Register, CheckoutObj, CommitObj, PushObj,
    #                                            AcceptPullObj, AcceptPushObj, ConfirmPullObj}, server_port=30000)
    # debugger_server.start_async()

    producer_nodes = [0] * n
    # To start child apps
    for i in range(n):
        producer_nodes[i] = Node(producer, Types=[Foo], server_port=30000)
        producer_nodes[i].start_async()

    time.sleep(2)
    consumer_node = Node(consumer, Types=[Foo], dataframe=("127.0.0.1", 30000))
    consumer_node.start_async()

    for i in range(n):
        producer_nodes[i].join()


main()