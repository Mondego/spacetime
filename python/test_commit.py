from spacetime import Node, Dataframe, DebugDataframe
from rtypes import pcc_set, primarykey, dimension, merge
from spacetime import Register, server_func, CommitObj, AcceptFetchObj, FetchObj

@pcc_set
class Foo:
    y = primarykey(int)

    def __init__(self, y):
        self.y = y

    def __str__(self):
        return "Foo_"+ str(self.y)


def producer(df, y):
    print("in producer", y)
    df.add_one(Foo, Foo(y))
    df.commit()

def main():

    n=1

    # To start central debugger node
    debugger_server = Node(server_func, Types=[Register, CommitObj, AcceptFetchObj, FetchObj], server_port=30000)

    debugger_server.start_async()
    print("Debugger server started")

    producer_nodes = [0]*n
    # To start child apps
    for i in range(n):
        producer_nodes[i] = Node(producer, Types=[Foo], debug=('127.0.0.1', 30000))
        producer_nodes[i].start_async(i)

    for i in range(n):
        producer_nodes[i].join()


main()