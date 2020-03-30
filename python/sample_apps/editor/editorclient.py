from flask import Flask, send_from_directory, request
from pprint import pprint
import os
import sys
import json
from datamodel import Document
# hack to add latest spacetime stuff to $PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
# from utillib import dllist4
from utillib.spacetimelist import SpacetimeList
from spacetime import Node


def make_editor_app(df):
    app = Flask(__name__, static_url_path='')
    df.pull()
    document = SpacetimeList(df)

    @app.route('/')
    def index():
        print("in here!")
        import os
        # os.chdir(os.path.dirname(__file__))
        # pd = os.getcwd()
        # print(os.getcwd())
        
        # return  send_from_directory(os.path.join(pd, 'static'), 'quilleditor.html')
        return  send_from_directory('/home/lg/2020/research/spacetime/python/sample_apps/editor/static', 'quilleditor.html')

    @app.route('/pullchanges')
    def pull_changes():
        return(''.join(document.get_sequence()))

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
                    idx_retain = op["retain"]
                elif "insert" in op:
                    #insert_value(op["insert"], idx_retain)
                    print(idx_retain)
                    if idx_retain == -1:
                        document.insert(op["insert"])
                    else:
                        print('++', idx_retain)
                        document.insert(op["insert"], loc=idx_retain)
                    idx_retain += len(op["insert"])
                elif "delete" in op:
                    for _ in range(op["delete"]):
                        document.delete(i=idx_retain)
                    # delete_values(op["delete"], idx_retain)
            
            print(document.get_sequence())

        return "Stored change"
    return app

def editor_node(df, port=5000):
    make_editor_app(df).run(port=port)

if __name__ == "__main__":
    editor_port = 5000
    if len(sys.argv) >= 2:
        editor_port = sys.argv[1]
    Node(editor_node, Types=[Document], dataframe=("127.0.0.1", 9000)).start(port=editor_port)
