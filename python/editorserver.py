
from spacetime import Node
from editorclient import EditorClient
import sys
import time
import pdb

WAIT_FOR_START = 10.0

def editor_server(dataframe):
    start = time.time()
    while (time.time() - start) < WAIT_FOR_START:
        print ("\rWaiting for %d " % (int(WAIT_FOR_START - (time.time() - start)),), "Seconds for clients to connect.")
        time.sleep(1)
    while True:
        # dataframe.pull()
        dataframe.checkout_await()
        clients = dataframe.read_all(EditorClient)
        print("**", dataframe.client_count, dataframe.versioned_heap)
        print("++++", dataframe.read_all(EditorClient))
        print(dataframe)
        print("//", clients)
        # print(dataframe.sync())

        time.sleep(1)





def main(port):
    server = Node(editor_server, server_port=port, Types=[EditorClient])
    server.start()

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    main(port)