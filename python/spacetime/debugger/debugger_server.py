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

# def merge_edges(Node):
#     start_node = "ROOT"
#
#     for delta in :
#         merged = merge_state_delta(merged, delta)



def convert_to_json(nodes, edges):
    node_list, edge_list, lst = list(), list(), list()
    for i, node in enumerate(nodes):
        # if node == self.head:
        #     node_list.append({'id': i, 'name': node.current[:4], 'type': 'head',
        #                       'is_master': str(node.is_master),
        #                       'style': "stroke:#006400" if node.is_master else "stroke:#8B0000"})
        #else:
            node_list.append({'id': i, 'name': node.current[:4], 'type': 'not_head',
                              'is_master': str(node.is_master),
                              'style': "stroke:#006400" if node.is_master else "stroke:#8B0000"})
            lst.append(node.current)

    for edge in edges:
        source_node = edge.from_node
        source_id = lst.index(source_node)
        target_node = edge.to_node
        target_id = lst.index(target_node)
        edge_list.append({'source_id': source_id, 'target_id': target_id,'label':delta_to_html(edge.payload), #'label': str(edge.payload),
                          'style': 'stroke:#000000;stroke-width: 2px;', 'arrowheadStyle': ''})


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
    return register_obj.port


def server_func(df):

    existing_obj = [CommitObj, FetchObj, AcceptFetchObj, CheckoutObj, PushObj, AcceptPushObj]
    dataframes = dict()
    prev_steps = defaultdict(list)
    nodes = dict()

    def check_for_new_nodes():
        no_of_nodes_joined = 0
        existing_register_objects = []
        while True:
            df.pull()
            new_register_objects = df.read_all(Register)
            for register_obj in new_register_objects:
                if register_obj not in existing_register_objects:
                    existing_register_objects.append(register_obj)
                    print("A new node registers with the server:" + str(register_obj.appname) + "\n")
                    current_df = Dataframe(register_obj.appname,
                                           {CommitObj, FetchObj, AcceptFetchObj, CheckoutObj, PushObj, AcceptPushObj,
                                             Vertex, Edge, Parent})  # Create a dataframe for the new client
                    print("The dataframe that is created for this node is" + str(current_df) + "\n")
                    dataframes[register_obj.appname] = current_df
                    nodes[register_obj.appname] = NodeState(register_obj.appname, current_df)
                    register_obj.port = current_df.details[1]
                    print("The port assigned to this node is " + str(register_obj.port) + "\n")
                    no_of_nodes_joined += 1
                    print("no of nodes that have registered with the server is " + str(no_of_nodes_joined) + "\n")
                    df.commit()

    def update_parent_and_children():
        for node in nodes:
            current_dataframes = dataframes.copy()
            df = current_dataframes[node]
            df.checkout()
            parents = df.read_all(Parent)
            if len(parents) > 0 and len(parents[0].parent_app) > 0:
                parent = parents[0].parent_app
                nodes[node].parent = nodes[parent]
                nodes[parent].child[node] = nodes[node]


    def debugger():

        app = Flask(__name__)
        all_nodes = dict()

        @app.route('/', methods=['GET'])
        def topology():
            apps = list()
            nodes_json = list()
            edges_json = list()
            current_dataframes = dataframes.copy()
            update_parent_and_children()
            for i, app in enumerate(current_dataframes):
                apps.append(app)
                nodes_json.append({'id': i, 'name': app})
                if nodes[app].parent:
                    edges_json.append({'source_id': apps.index(app),
                                           'target_id': apps.index(nodes[app].parent.appname)})

            topology_jsonified = {'nodes': nodes_json,
                                  'links': edges_json}
            print(json.dumps(topology_jsonified))
            return render_template("Topology.html", graph_view=json.dumps(topology_jsonified))

        @app.route("/home/<string:appname>/", methods=['GET'])
        def app_view(appname):
            # return the view of the graph for this node
            current_dataframes = dataframes.copy()
            df = current_dataframes[appname]
            df.checkout()
            node_objects = df.read_all(Vertex)
            edge_objects = df.read_all(Edge)
            graph_json = convert_to_json(node_objects, edge_objects)
            print(graph_json)
            return render_template("Graph.html", graph_view=graph_json, appname=appname)

        # @app.route("/home/<string:appname>/next", methods=['GET'])
        # def app_view_next(appname):
        #
        #     current_dataframes = dataframes.copy()
        #     next_steps = {appname: [] for appname in current_dataframes}
        #
        #     df = current_dataframes[appname]
        #     df.checkout()
        #     new_objects = list()
        #     for type in existing_obj:
        #         # print(type)
        #         new_objects += df.read_all(type)
        #     # print(new_objects)
        #     highlight = [0 for i in range(len(new_objects))]
        #     for obj in new_objects:
        #         # print("in CDN", obj, obj.state)
        #         if isinstance(obj, CheckoutObj):
        #             if obj.state == obj.CheckoutState.INIT:
        #                 obj.start()
        #                 df.commit()
        #                 print("CDN gives permission to the node for checkout")
        #             if obj.state == obj.CheckoutState.CHECKOUTCOMPLETE:
        #                 print("The CDN knows checkout is complete")
        #                 obj.start_GC()
        #                 print("The CDN gives permission to start GC")
        #                 df.commit()
        #
        #         if isinstance(obj, CommitObj):
        #             if obj.state == obj.CommitState.INIT:
        #                 obj.start()
        #                 df.commit()
        #                 print("Go ahead from the CDN to the node for commit")
        #                 # print (obj.state, obj.CommitState.COMMITCOMPLETE)
        #             if obj.state == obj.CommitState.COMMITCOMPLETE:
        #                 print("The CDN knows the commit is complete")
        #                 obj.start_GC()
        #                 df.commit()
        #
        #         if isinstance(obj, PushObj):
        #             if obj.state == obj.PushState.INIT:
        #                 obj.start()
        #                 df.commit()
        #                 print("Go ahead from the CDN to the node for push")
        #
        #             if obj.state == obj.PushState.FETCHDELTACOMPLETE:
        #                 print("CDN gets the delta from the sender node and creates a corres. acceptPushObj")
        #                 acceptPushObj = AcceptPushObj(obj.sender_node, obj.receiver_node, obj.from_version,
        #                                               obj.to_version, obj.delta, obj.oid)
        #                 acceptPushObj.start()
        #                 receiver_df = current_dataframes[obj.receiver_node]
        #                 receiver_df.add_one(AcceptPushObj, acceptPushObj)
        #                 receiver_df.commit()
        #                 obj.wait()
        #                 df.commit()
        #
        #         if isinstance(obj, AcceptPushObj):
        #             if obj.state == obj.AcceptPushState.RECEIVECOMPLETE:
        #                 sender_df = current_dataframes[obj.sender_node]
        #                 pushObj = sender_df.read_one(PushObj, obj.oid)
        #                 pushObj.start_GC()
        #                 sender_df.commit()
        #                 obj.start_GC()
        #                 df.commit()
        #
        #         if isinstance(obj, FetchObj):
        #             if obj.state == obj.FetchState.INIT:
        #                 print("CDN gets a fetch object and creates a corres. acceptfetchObj")
        #                 acceptFetchObj = AcceptFetchObj(obj.requestor_node, obj.requestee_node,
        #                                                 obj.from_version, obj.to_version, b"", obj.oid)
        #                 acceptFetchObj.start()
        #                 obj.wait()
        #                 df.commit()
        #                 requestee_df = current_dataframes[obj.requestee_node]
        #                 requestee_df.add_one(AcceptFetchObj, acceptFetchObj)
        #                 requestee_df.commit()
        #
        #             if obj.state == obj.FetchState.FETCHCOMPLETE:
        #                 obj.start_GC()
        #                 df.commit()
        #                 requestee_df = current_dataframes[obj.requestee_node]
        #                 acceptFetchObj = requestee_df.read_one(AcceptFetchObj, obj.oid)
        #                 acceptFetchObj.start_GC()
        #                 requestee_df.commit()
        #
        #         if isinstance(obj, AcceptFetchObj):
        #             if obj.state == obj.AcceptFetchState.SENDCOMPLETE:
        #                 # print("CDN sends the retrieved delta to the requestor")
        #                 requestor_df = current_dataframes[obj.requestor_node]
        #                 fetchObj = requestor_df.read_one(FetchObj, obj.oid)
        #                 fetchObj.to_version = obj.to_version
        #                 fetchObj.delta = obj.delta
        #                 fetchObj.start()
        #                 requestor_df.commit()
        #                 obj.wait()
        #                 df.commit()
        #
        #     node_objects = df.read_all(Vertex)
        #     edge_objects = df.read_all(Edge)
        #     graph_json = convert_to_json(node_objects, edge_objects)
        #     print(graph_json)
        #     return render_template("Graph.html", graph_view=graph_json, appname=appname,
        #                            next_steps=json.dumps(next_steps),
        #                                highlight=highlight, prev_steps=json.dumps(prev_steps))

        @app.route("/home/<string:appname>/next", methods=['GET'])
        def app_view_next(appname):
            print("prev_steps", prev_steps)
            current_dataframes = dataframes.copy()
            df = current_dataframes[appname]
            df.checkout()
            highlight = 0
            nodes[appname].update()
            nodes[appname].execute()
            nodes[appname].update()
                # print("in CDN", obj, obj.state)
            df.checkout()
            node_objects = df.read_all(Vertex)
            edge_objects = df.read_all(Edge)
            graph_json = convert_to_json(node_objects, edge_objects)
            print(nodes[appname].command_list, nodes[appname].next_steps, nodes[appname].prev_steps)
            return render_template("Graph.html", graph_view=graph_json, appname=appname, next_steps=json.dumps(nodes[appname].next_steps),
                                   highlight=nodes[appname].current_stage, prev_steps=json.dumps(nodes[appname].prev_steps))

        @app.route("/home/<string:appname>/swap", methods=['POST'])
        def swap(appname):
            posns = request.get_json()
            nodes[appname].swap(posns["pos1"], posns["pos2"])
            return redirect(f"/home/{appname}/next")

        def graph():
            while True:
                current_dataframes = dataframes.copy()
                for df in current_dataframes.values():
                    #print(df.details)
                    df.checkout()
                    node_objects = df.read_all(Vertex)
                    edge_objects = df.read_all(Edge)
                    graph_json = convert_to_json(node_objects, edge_objects)
                    new_objects = list()
                    for type in existing_obj:
                        #print(type)
                        new_objects += df.read_all(type)
                    print(new_objects)

                    for obj in new_objects:
                        #print("in CDN", obj, obj.state)
                        if isinstance(obj, CheckoutObj):
                            if obj.state == obj.CheckoutState.INIT:
                                obj.start()
                                df.commit()
                                print("CDN gives permission to the node for checkout")

                            if obj.state == obj.CheckoutState.CHECKOUTCOMPLETE:
                                    print("The CDN knows checkout is complete")
                                    obj.start_GC()
                                    print("The CDN gives permission to start GC")
                                    df.commit()

                        if isinstance(obj, CommitObj):
                            if obj.state == obj.CommitState.INIT:
                                obj.start()
                                df.commit()
                                print("Go ahead from the CDN to the node for commit")
                                #print (obj.state, obj.CommitState.COMMITCOMPLETE)
                            if obj.state == obj.CommitState.COMMITCOMPLETE:
                                    print("The CDN knows the commit is complete")
                                    obj.start_GC()
                                    df.commit()

                        if isinstance(obj, PushObj):
                            if obj.state == obj.PushState.INIT:
                                obj.start()
                                df.commit()
                                print("Go ahead from the CDN to the node for push")

                            if obj.state == obj.PushState.FETCHDELTACOMPLETE:
                                print("CDN gets the delta from the sender node and creates a corres. acceptPushObj")
                                acceptPushObj = AcceptPushObj(obj.sender_node, obj.receiver_node, obj.from_version,
                                                              obj.to_version, obj.delta, obj.oid)
                                acceptPushObj.start()
                                receiver_df = current_dataframes[obj.receiver_node]
                                receiver_df.add_one(AcceptPushObj, acceptPushObj)
                                receiver_df.commit()
                                obj.wait()
                                df.commit()

                        if isinstance(obj, AcceptPushObj):
                            if obj.state == obj.AcceptPushState.RECEIVECOMPLETE:
                                sender_df = current_dataframes[obj.sender_node]
                                pushObj = sender_df.read_one(PushObj, obj.oid)
                                pushObj.start_GC()
                                sender_df.commit()
                                obj.start_GC()
                                df.commit()

                        if isinstance(obj, FetchObj):
                            if obj.state == obj.FetchState.INIT:
                                print("CDN gets a fetch object and creates a corres. acceptfetchObj")
                                acceptFetchObj = AcceptFetchObj(obj.requestor_node, obj.requestee_node,
                                                                obj.from_version, obj.to_version, b"", obj.oid)
                                acceptFetchObj.start()
                                obj.wait()
                                df.commit()
                                requestee_df = current_dataframes[obj.requestee_node]
                                requestee_df.add_one(AcceptFetchObj, acceptFetchObj)
                                requestee_df.commit()

                            if obj.state == obj.FetchState.FETCHCOMPLETE:
                                obj.start_GC()
                                df.commit()
                                requestee_df = current_dataframes[obj.requestee_node]
                                acceptFetchObj = requestee_df.read_one(AcceptFetchObj, obj.oid)
                                acceptFetchObj.start_GC()
                                requestee_df.commit()

                        if isinstance(obj, AcceptFetchObj):
                            if obj.state == obj.AcceptFetchState.SENDCOMPLETE:
                                    #print("CDN sends the retrieved delta to the requestor")
                                    requestor_df = current_dataframes[obj.requestor_node]
                                    fetchObj = requestor_df.read_one(FetchObj, obj.oid)
                                    fetchObj.to_version = obj.to_version
                                    fetchObj.delta = obj.delta
                                    fetchObj.start()
                                    requestor_df.commit()
                                    obj.wait()
                                    df.commit()

                return render_template("DAG.html", graph_view=graph_json, request_view=[])

        app.run()

    check_for_new_nodes_thread = Thread(target=check_for_new_nodes)
    check_for_new_nodes_thread.start()
    debugger_thread = Thread(target=debugger)
    debugger_thread.start()
    check_for_new_nodes_thread.join()
    debugger_thread.join()