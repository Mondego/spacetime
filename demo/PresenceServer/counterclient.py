from flask import Flask, send_from_directory, request
from datamodel import Counter
import sys
import time
# hack to add latest spacetime stuff to $PYTHONPATH
from spacetime import Node



def editor_node(df, port=5000):
    while True:
        df.pull()
        # df.checkout()
        c = df.read_one(Counter, "countDocument")
        # print("Received value ", c.count)
        print("+++", c.count)
        # c.count += 1
        st = time.perf_counter()
        if df:
            # df.commit()
            # print("counterclient.py: doing a push")
            # df.push_await()
            # print("counterclient.py: finished a push")
            et = time.perf_counter()
            if (et - st) < 0.3:
                time.sleep(et - st)

if __name__ == "__main__":
    editor_port = 5000
    if len(sys.argv) >= 2:
        editor_port = sys.argv[1]
    Node(editor_node, Types=[Counter], is_server=False, dataframe=("127.0.0.1", 9000), pure_python=False).start(port=editor_port)
