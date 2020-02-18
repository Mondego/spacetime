from flask import Flask, send_from_directory, request
from pprint import pprint
import os
import sys
import json

# hack to add latest spacetime stuff to $PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from utillib import dllist4

app = Flask(__name__, static_url_path='')

@app.route('/')
def index():
    print("in here!")
    import os
    os.chdir(os.path.dirname(__file__))
    pd = os.getcwd()
    print(os.getcwd())
    return  send_from_directory(os.path.join(pd, 'static'), 'quilleditor.html')

@app.route('/receivechange', methods=['POST'])
def receive_update():
    if request.method == 'POST':
        delta = json.loads(request.form.get('delta'))
        pprint(delta)
        ld = len(delta)
        if ld == 3:
            print("Change event")
        elif ld == 2:
            print("Insert or delete event")
        else:
            print("Append or delete all event")

        # iterate over the delta ops, one by one
        idx_retain = -1
        for op in delta:
            if "retain" in op:
                idx_retain = op["retain"] - 1
            elif "insert" in op:
                insert_value(op["insert"], idx_retain)
                idx_retain += len(op["insert"])
            elif "delete" in op:
                delete_values(op["delete"], idx_retain)
        return "Stored change"