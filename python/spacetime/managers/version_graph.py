from multiprocessing import Process, RLock, Manager, Event
from threading import Thread
import json
from flask import Flask, request, render_template
import requests


class Node(object):
    def __eq__(self, other):
        return other.current == self.current

    def __hash__(self):
        return hash(self.current)

    def __init__(self, current, is_master):
        self.current = current
        self.prev_master = None
        self.next_master = None
        self.all_prev = set()
        self.all_next = set()
        self.is_master = is_master

    def set_next(self, version):
        if self.next_master is None:
            self.next_master = version
        self.all_next.add(version)

    def set_prev(self, version):
        if self.prev_master is None:
            self.prev_master = version
        self.all_prev.add(version)


class Edge(object):
    def __init__(self, from_node, to_node, payload):
        self.from_node = from_node
        self.to_node = to_node
        self.payload = payload


class Graph(object):
    def __init__(self):
        self.tail = Node("ROOT", True)
        self.head = self.tail
        self.nodes = {"ROOT": self.tail}
        self.edges = dict()
        self.state_to_app = dict()
        self.app_to_state = dict()

    def convert_to_json(self):
        node_list=[]
        edge_list=[]
        alist=[]
        for i, node in enumerate(self.nodes.values()):
            if node == self.head:
                node_list.append({'id': i, 'name': node.current[:4], 'type': 'head',
                                  'is_master': str(node.is_master),
                                  'style': "stroke:#006400" if node.is_master else "stroke:#8B0000"})
            else:    
                node_list.append({'id': i, 'name': node.current[:4], 'type': 'not_head',
                                  'is_master': str(node.is_master),
                                  'style': "stroke:#006400" if node.is_master else "stroke:#8B0000"})
            alist.append(node.current)

        j = len(self.nodes.values())
        for app in self.app_to_state.keys():
            alist.append(app)
            node_list.append({'id': j, 'name': app[:9], 'type': 'app', 'is_master': 'False',
                              'style': "stroke:#696969; #stroke-width: 3px; stroke-dasharray: 5, 5;"})
            j += 1

        for edge in self.edges.values():
            source_node = edge.from_node
            source_id = alist.index(source_node)
            target_node = edge.to_node
            target_id = alist.index(target_node)
            edge_list.append({'source_id': source_id,'target_id': target_id, 'label': str(edge.payload),
                              'style':'stroke:#000000;stroke-width: 2px;','arrowheadStyle': ''})

        for app in self.app_to_state.keys():
            source_node = app
            target_node = self.app_to_state[app]
            source_id = alist.index(source_node)
            target_id = alist.index(target_node)
            edge_list.append({'source_id': source_id, 'target_id': target_id, 
                              'label': '','style': "stroke: #696969; stroke-width: 3px; stroke-dasharray: 5, 5;",
                              'arrowheadStyle': 'fill: #696969'})
            
        graph_jsonified = {'nodes': node_list,
                           'links': edge_list }
        #graph_hier = self.create_hierarchy('ROOT', '', True)
        print(json.dumps(graph_jsonified))
        return json.dumps(graph_jsonified)

    def create_hierarchy(self, node, graph_hier, first_sibling):
        
        if first_sibling:
            graph_hier = graph_hier + '{\n"name": "' + node + '"\n'
        else:
            graph_hier = graph_hier + ',{\n"name": "' + node + '"\n'
            
        i = 0
        already_has_a_child = False
        first_sibling = False
        is_not_leaf = False
        edge_list = list(self.edges.values())

        while i < len(edge_list):
            if first_sibling:
                first_sibling = False
            if edge_list[i].from_node == node:
                is_not_leaf = True
                if not already_has_a_child:
                    already_has_a_child = True
                    first_sibling = True
                    graph_hier = graph_hier + ',"children":['
                child = edge_list[i].to_node
                graph_hier = self.create_hierarchy(child, graph_hier, first_sibling)
            i += 1

        if i == len(self.edges):
            if is_not_leaf:
                graph_hier = graph_hier + ']}'
            else:
                graph_hier = graph_hier + '}'
            return graph_hier


    def __getitem__(self, key):
        if isinstance(key, slice):
            step_reverse = ((key.step is not None) and (key.step < 0))
            if key.start:
                try:
                    start = self.nodes[key.start]
                except KeyError:
                    return list()
            elif step_reverse:
                start = self.head
            else:
                start = self.tail

            if key.stop:
                end = self.nodes[key.stop]
            elif step_reverse:
                end = self.tail
            else:
                end = self.head
            current = start
            while current != end:
                if step_reverse:
                    edge = self.edges[(current.prev_master, current.current)]
                    current = self.nodes[current.prev_master]
                else:
                    edge = self.edges[(current.current, current.next_master)]
                    current = self.nodes[current.next_master]
                yield edge.payload
            return
        else:
            raise RuntimeError("Cannot deal with non slice operators.")

    def continue_chain(
            self, from_version, to_version, package, force_branch=False):
        from_version_node = self.nodes.setdefault(
            from_version, Node(from_version, from_version == self.head.current))

        to_version_node = self.nodes.setdefault(
            to_version, Node(
                to_version, (not force_branch) and from_version == self.head.current))
        to_version_node.set_prev(from_version)

        edge = Edge(from_version, to_version, package)
        self.edges[(from_version, to_version)] = edge

        from_version_node.set_next(to_version)
        if from_version == self.head.current:
            self.head = to_version_node
        return

    def delete_item(self, version):
        pass

    def maintain_edges(self):
        edges_to_del = set()
        for edge in self.edges:
            start, end = self.nodes[edge[0]], self.nodes[edge[1]]
            if not start.is_master and not end.is_master:
                # Can delete the edge that took start to the master line.
                # The other application clearly ignored it.
                # Lets delete so that nodes can be merged.
                for node_v in start.all_next:
                    if self.nodes[node_v].is_master and (start.current, node_v) in self.edges:
                        edges_to_del.add((start.current, node_v))
                        start.next_master = end.current
        for start, end in edges_to_del:
            del self.edges[(start, end)]
            self.nodes[start].all_next.remove(end)
            self.nodes[end].all_prev.remove(start)

    def merge_node(self, node, merger_function):
        del self.nodes[node.current]
        old_change = self.edges[(node.prev_master, node.current)].payload
        new_change = self.edges[(node.current, node.next_master)].payload
        new_payload = merger_function(old_change, new_change)

        del self.edges[(node.prev_master, node.current)]
        del self.edges[(node.current, node.next_master)]
        self.nodes[node.prev_master].all_next.remove(node.current)
        self.nodes[node.next_master].all_prev.remove(node.current)
        if (self.nodes[node.prev_master].is_master
                and self.nodes[node.next_master].is_master and not node.is_master):
            # This is a branch, that can be deleted.
            return
        if (node.prev_master, node.next_master) not in self.edges:
            self.edges[(node.prev_master, node.next_master)] = Edge(
                node.prev_master, node.next_master, new_payload)
        else:
            # Figure out how to avoid this computation.
            assert self.edges[(node.prev_master, node.next_master)].payload == new_payload, (
                self.edges[(node.prev_master, node.next_master)].payload, new_payload)
        if self.nodes[node.prev_master].next_master == node.current:
            self.nodes[node.prev_master].next_master = node.next_master
        self.nodes[node.prev_master].all_next.add(node.next_master)
        if self.nodes[node.next_master].prev_master == node.current:
            self.nodes[node.next_master].prev_master = node.prev_master
        self.nodes[node.next_master].all_prev.add(node.prev_master)

    def maintain_nodes(self, state_to_ref, merger_function, master):
        mark_for_merge = set()
        mark_for_delete = set()
        for version, node in self.nodes.items():
            if node == self.head or node == self.tail:
                continue
            if version in state_to_ref and state_to_ref[version]:
                # Node is still marked.
                continue
            if len(node.all_next) > 1 or len(node.all_prev) > 1:
                # Node is marked in a branch.
                continue
            if node.is_master == master:
                mark_for_merge.add(node)

        for node in mark_for_merge:
            self.merge_node(node, merger_function)

    def maintain(self, state_to_ref, app_to_state, merger_function):
        self.state_to_app = state_to_ref
        self.app_to_state = app_to_state
        # Delete edges that are useless.
        c = deepcopy(self.nodes), deepcopy(self.edges)
        self.maintain_edges()
        # Merge nodes that are chaining without anyone looking at them
        # First divergent.
        self.maintain_nodes(state_to_ref, merger_function, False)
        # The master line.
        self.maintain_nodes(state_to_ref, merger_function, True)


class VersionGraphProcess(Process):

    def __init__(self, appname):
        self.appname = appname
        self.graph = Graph()
        super().__init__()
        self.manager = Manager()
        self.result_manager = Manager()
        self.read_write_queue = self.manager.Queue()
        self.request_list = []
        self.app = self.create_flask_app()
        self.start()

    def __del__(self):
        res = requests.get('http://127.0.0.1:5000/shutdown').content
        print(res)

    def create_flask_app(self):
        app = Flask(__name__)

        # app.config["DEBUG"] = True

        def shutdown_server():
            func = request.environ.get('werkzeug.server.shutdown')
            if func is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            func()

        @app.route('/next', methods=['GET'])
        def graph():
            if not self.request_list:
                return render_template('DAG.html', graph_view=self.graph.convert_to_json(), request_view=[req[0] for req in self.request_list])
            req = self.request_list.pop(0)
            if req[0] == "Continue chain":
                from_version, to_version, package, continue_chain_event = req[1:]
                self.process_continue_chain(from_version, to_version, package, continue_chain_event)
            elif req[0] == "Maintain":
                state_to_ref, app_to_state, merger_function, maintain_event = req[1:]
                self.process_maintain(state_to_ref, app_to_state, merger_function, maintain_event)
            elif req[0] == "Get item":
                key, result_queue = req[1], req[2]
                self.process_get_item(key, result_queue)
            return render_template('DAG.html',graph_view=self.graph.convert_to_json(),request_view=[req[0] for req in self.request_list])

        @app.route('/data.json', methods=['GET'])
        def return_json():
            return self.graph.convert_to_json()

        @app.route('/displayQueue', methods=['GET'])
        def display_request_queue():
            return  render_template('DAG.html', graph_view=self.graph.convert_to_json(), request_view=[req[0] for req in self.request_list])

        @app.route('/shutdown', methods=['GET'])
        def shutdown():
            shutdown_server()
            return 'Server shutting down...'


        return app

    def process_continue_chain(self, from_version, to_version, package, continue_chain_event):
        self.graph.continue_chain(from_version, to_version, package)
        continue_chain_event.set()

    def process_maintain(self, state_to_ref, app_to_state, merger_function, maintain_event):
        self.graph.maintain(state_to_ref, app_to_state,  merger_function)
        maintain_event.set()

    def process_get_item(self, key, result_queue):
        result_queue.put(list(self.graph.__getitem__(key)))

    def run(self):
        flask_thread = Thread(target=self.app.run, daemon=True)
        flask_thread.start()
        while True:
            req = self.read_write_queue.get()
            self.request_list.append(req)

    def continue_chain(self, from_version, to_version, package):
        continue_chain_event = self.manager.Event()
        self.read_write_queue.put(("Continue chain", from_version, to_version, package, continue_chain_event))
        continue_chain_event.wait()

    def maintain(self, state_to_ref, app_to_state, merger_function):
        maintain_event = self.manager.Event()
        self.read_write_queue.put(("Maintain", state_to_ref, app_to_state, merger_function, maintain_event))
        maintain_event.wait()

    def __getitem__(self, key):
        result_queue = self.result_manager.Queue()
        self.read_write_queue.put(("Get item", key, result_queue))
        return result_queue.get()
