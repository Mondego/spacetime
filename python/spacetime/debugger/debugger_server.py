from spacetime.dataframe import Dataframe
from threading import Thread
from rtypes import pcc_set, primarykey, dimension, merge
from spacetime.debugger.debug_dataframe import DebugDataframe
from spacetime.debugger.debugger_types import CommitObj, FetchObj, AcceptFetchObj, Register, \
    CheckoutObj, PushObj, AcceptPushObj, Parent
from spacetime.debugger.node_state import NodeState
from spacetime.managers.version_graph import Node as Vertex, Edge
from spacetime.utils.utils import merge_state_delta
from flask import Flask, render_template, redirect, request
import json
import copy
import time
from collections import defaultdict
from json2table import convert
from spacetime.utils.utils import merge_state_delta


def get_event_type(event):
    if event == 0:
        return "New"
    if event == 1:
        return "Modification"
    if event == 2:
        return "Delete"

def delta_to_html(payload):
    #simplified_edge = {"Type": {"oid":{"dim": "value"}}}
    simplified_edge = {"Type": {"event ":{"oid": {"dim": "value"}}}}
    for tp in payload:
        simplified_edge[tp] = {}
        for oid in payload[tp]:
            event = get_event_type(payload[tp][oid]['types'][tp])
            simplified_edge[tp][event] = {}
            simplified_edge[tp][event][oid] = {}
            for dim in payload[tp][oid]['dims']:
                simplified_edge[tp][event][oid][dim] = payload[tp][oid]['dims'][dim]['value']
    #print(simplified_edge)
    return convert(simplified_edge)


def merge_edges(appname, end_version):
    NODE = NODES[appname]
    payloads = list()
    version = end_version
    while version.prev_master:
        payloads.append(NODE.edge_map[(version.prev_master, version.current)].payload)
        version = NODE.vertex_map[version.prev_master]
    #print("payloads", payloads)
    merged = dict()
    for payload in payloads[::-1]:
        merged = merge_state_delta(merged, payload, delete_it=True)
    print(merged)
    return delta_to_html(merged)


def convert_to_json(appname, nodes, edges):
    node_list, edge_list, lst = list(), list(), list()
    for i, node in enumerate(nodes):
        # if node == self.head:
        #     node_list.append({'id': i, 'name': node.current[:4], 'type': 'head',
        #                       'is_master': str(node.is_master),
        #                       'style': "stroke:#006400" if node.is_master else "stroke:#8B0000"})
        #else:
            node_list.append({'id': i, 'name': node.current[:4], 'type': 'not_head', 'state': merge_edges(appname, node),
                              'is_master': str(node.is_master), 'full_name': node.current,
                              'style': "stroke:#006400" if node.is_master else "stroke:#8B0000"})
            lst.append(node.current)

    for edge in edges:
        source_node = edge.from_node
        source_id = lst.index(source_node)
        target_node = edge.to_node
        target_id = lst.index(target_node)
        edge_list.append({'source_id': source_id, 'target_id': target_id,'label':delta_to_html(edge.payload), #'label': str(edge.payload),
                          'style': 'stroke:#000000;stroke-width: 2px;', 'arrowheadStyle': '',
                          'source_node': edge.from_node, 'target_node' : edge.to_node
                          })


    graph_jsonified = {'nodes': node_list,
                       'links': edge_list}
    #print(json.dumps(graph_jsonified))
    return json.dumps(graph_jsonified)


def register_func(df, appname):

    register_obj = Register(appname)
    df.add_one(Register, register_obj)
    df.commit()
    df.push()
    while register_obj.port is 0:
        # time.sleep(5)
        df.pull()
    port = register_obj.port
    df.delete_one(Register, register_obj)
    df.commit()
    df.push()
    return port


DATAFRAMES = dict()
NODES = dict()
EXISTING_OBJ = [CommitObj, FetchObj, AcceptFetchObj, CheckoutObj, PushObj, AcceptPushObj]


def check_for_new_nodes(df, apptypes):
    while True:
        df.pull()
        new_register_objects = df.read_all(Register)
        for register_obj in new_register_objects:
            if register_obj.port:
                continue

            print("A new node registers with the server:" + str(register_obj.appname) + "\n")
            current_df = Dataframe(register_obj.appname,
                                    {CommitObj, FetchObj, AcceptFetchObj, CheckoutObj, PushObj, AcceptPushObj,
                                    Vertex, Edge, Parent})  # Create a dataframe for the new client
            print("The dataframe that is created for this node is" + str(current_df) + "\n")
            DATAFRAMES[register_obj.appname] = current_df
            NODES[register_obj.appname] = NodeState(register_obj.appname, current_df, apptypes)
            register_obj.port = current_df.details[1]
            print("The port assigned to this node is " + str(register_obj.port) + "\n")
            df.commit()

def update_parent_and_children():
    for node in NODES.values():
        node.update()
        if node.parent:
            continue
        df = node.df
        parent = df.read_one(Parent, Parent.singleton)
        while not parent:
            df.checkout()
        parent = parent.parent_app
        if parent:
            node.parent = NODES[parent]
            NODES[parent].child[node.appname] = node


def debugger():
    app = Flask(__name__)
    
    @app.route('/', methods=['GET'])
    def topology():
        apps = list()
        nodes_json = list()
        edges_json = list()
        update_parent_and_children()
        for i, app in enumerate(DATAFRAMES):
            apps.append(app)
            nodes_json.append({'id': i, 'name': app})
            if NODES[app].parent:
                edges_json.append({'source_id': apps.index(app),
                                        'target_id': apps.index(NODES[app].parent.appname)})

        topology_jsonified = {'nodes': nodes_json,
                                'links': edges_json}
        print(json.dumps(topology_jsonified))
        return render_template("Topology.html", graph_view=json.dumps(topology_jsonified))

    # @app.route("/home/<string:appname>/", methods=['GET'])
    # def app_view(appname):
    #     # return the view of the graph for this node
    #     df = DATAFRAMES[appname]
    #     df.checkout()
    #     node_objects = df.read_all(Vertex)
    #     edge_objects = df.read_all(Edge)
    #     graph_json = convert_to_json(node_objects, edge_objects)
    #     print(graph_json)
    #     return render_template("Graph.html", graph_view=graph_json, appname=appname)

    @app.route("/home/<string:appname>/", methods=['GET'])
    def app_view(appname):
        # return the view of the graph for this node
        node = NODES[appname]
        node.update()
        graph_json = convert_to_json(appname, node.vertices, node.edges)
        #print(graph_json)
        #print(NODES[appname].next_steps, NODES[appname].prev_steps)
        # pick up NODES[appname].open_tables
        #print(type(NODES[appname].current_command))
        return render_template("Graph.html", graph_view=graph_json, appname=appname,
                                next_steps=json.dumps(NODES[appname].next_steps),
                                highlight=NODES[appname].current_stage[type(NODES[appname].current_command)],
                                prev_steps=json.dumps(NODES[appname].prev_steps), tables= NODES[appname].open_tables)
    
    @app.route("/home/<string:appname>/next", methods=['GET'])
    def app_view_next(appname):
        NODES[appname].update()
        NODES[appname].execute()
        print(NODES[appname].command_list)
        return redirect(f"/home/{appname}/")

    @app.route("/home/<string:appname>/statefor", methods=['GET'])
    def app_view_state(appname):
        key = request.args["key"]
        keytype = request.args["keytype"]
        print(key, keytype)
        NODES[appname].add_to_table(key, keytype)
        print(NODES[appname].open_tables)
        return redirect(f"/home/{appname}/")

    @app.route("/home/<string:appname>/swap", methods=['POST'])
    def swap(appname):
        posns = request.get_json()
        print("post", posns)
        NODES[appname].swap(posns["pos1"], posns["pos2"])
        graph_json = convert_to_json(appname, NODES[appname].vertices, NODES[appname].edges)
        return render_template("Graph.html", graph_view=graph_json, appname=appname,
                               next_steps=json.dumps(NODES[appname].next_steps),
                               highlight=NODES[appname].current_stage, prev_steps=json.dumps(NODES[appname].prev_steps))
        #return redirect(f"/home/{appname}/")
    
    @app.route("/run", methods=['GET', 'POST'])
    def app_run():
        # breakpoint = request.args["q"]
        if request.method == "POST":
            breakpoint = request.form["q"]
        if  request.method == "GET":
            breakpoint = request.args["q"]
        print(breakpoint)
        where, command = breakpoint.split(":")
        where = where.strip()
        command = command.strip()
        where = list(NODES.keys()) if where == "all" else where.split(",")
        while True:
            for node in NODES.values():
                node.update()
                node.execute()
                df = node.managed_heap
                try:
                    print (node.appname,"Foo:", len(df.read_all(Foo)), eval(command), command)
                    if eval(command):
                        return redirect(f"/home/{node.appname}")
                except Exception as e:
                    return f"Bad Eval {e}"
    app.run()

def server_func(df, apptypes):
    for tp in apptypes:
        globals()[tp.__name__] = tp
    #print (Foo)
    check_for_new_nodes_thread = Thread(target=check_for_new_nodes, args=(df, apptypes))
    check_for_new_nodes_thread.start()
    debugger_thread = Thread(target=debugger)
    debugger_thread.start()
    check_for_new_nodes_thread.join()
    debugger_thread.join()