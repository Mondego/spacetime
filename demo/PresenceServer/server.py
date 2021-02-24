import sys
import os
import time

from datamodel import Counter
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
# from utillib import dllist4
from spacetime import Node

def host(df):
    final_c = -1
    counter = Counter()
    df.add_one(Counter, counter)
    df.commit()
    while True:
        # df.pull()
        df.checkout()
        c = df.read_one(Counter, "countDocument")
        if c.count != final_c:
            final_c = c.count
            print("+++ c = ", final_c, flush=True)
        st = time.perf_counter()
        if df:
            # df.commit()
            # df.push()
            et = time.perf_counter()
            if (et - st) < 0.3:
                time.sleep(et - st)
    print("final_c: ", final_c)

if __name__ == "__main__":
    print("At least print this")
    Node(host, server_port=9000, is_server=True, Types=[Counter], pure_python=False).start()
