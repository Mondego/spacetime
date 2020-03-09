import sys
import os
import time

from datamodel import Document
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
# from utillib import dllist4
from spacetime import Node

def host(df):
    document = Document()
    df.add_one(Document, document)
    df.commit()
    while True:
        df.checkout_await()
        print (document.history_list)

if __name__ == "__main__":
    Node(host, server_port=9000, Types=[Document]).start()
