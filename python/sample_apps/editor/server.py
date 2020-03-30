import sys
import os
import time

from datamodel import Document
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from utillib.spacetimelist import SpacetimeList
# from utillib import dllist4
from spacetime import Node

def host(df):
    document = Document()
    df.add_one(Document, document)
    df.commit()
    shared_edit = SpacetimeList([])
    while True:
        df.checkout_await()
        st = time.perf_counter()
        # print (document.history_list)
        shared_edit.__merge__(document.history_list)
        try:
            document.history_list = list(shared_edit.document.history_list)
            pass
        except Exception as e:
            print()
            print("Exception on accessing shared history_list")
        print(shared_edit.get_sequence())
        if df:
            df.commit()
            df.push()
            et = time.perf_counter()
            if (et - st) < 0.3:
                time.sleep(et - st)

if __name__ == "__main__":
    Node(host, server_port=9000, Types=[Document]).start()
