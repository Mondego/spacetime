from threading import Thread
from spacetime.debugger.debugger_types import CommitObj, FetchObj, AcceptFetchObj, Register, \
    CheckoutObj, PushObj, AcceptPushObj, Parent
from spacetime.managers.version_graph import Node as Vertex, Edge
from spacetime.managers.managed_heap import ManagedHeap
from json2table import convert
from spacetime.utils.utils import merge_state_delta


class NodeState(object):
    @property
    def current_command(self):
        if self.command_list:
            return self.command_list[0]
        return None

    @property
    def vertices(self):
        return self.df.read_all(Vertex)
        
    @property
    def edges(self):
        return self.df.read_all(Edge)
    
    @property
    def vertex_map(self):
        return {v.current:v for v in self.vertices}
        
    @property
    def edge_map(self):
        return {(e.from_node, e.to_node):e for e in self.edges}

    def __init__(self, appname, df, apptypes):

        self.df = df
        self.appname = appname
        self.parent = None
        self.child = dict()
        self.managed_heap = ManagedHeap(apptypes)
        self.command_list = list()
        self.next_steps = list()
        self.prev_steps = list()
        self.tps = [CommitObj, FetchObj, AcceptFetchObj, CheckoutObj, PushObj, AcceptPushObj]
        self.current_stage = {tp:-1 for tp in self.tps}
        self._open_tables = list()
        self.df.checkout()

    def get_event_type(self, event):
        if event == 0:
            return "New"
        if event == 1:
            return "Modification"
        if event == 2:
            return "Delete"

    def delta_to_html(self, payload):
        # simplified_edge = {"Type": {"oid":{"dim": "value"}}}
        simplified_edge = {"Type": {"event ": {"oid": {"dim": "value"}}}}
        for tp in payload:
            simplified_edge[tp] = {}
            for oid in payload[tp]:
                event = self.get_event_type(payload[tp][oid]['types'][tp])
                simplified_edge[tp][event] = {}
                simplified_edge[tp][event][oid] = {}
                for dim in payload[tp][oid]['dims']:
                    simplified_edge[tp][event][oid][dim] = payload[tp][oid]['dims'][dim]['value']
        # print(simplified_edge)
        return convert(simplified_edge)

    def delta_to_table(self, keytype, key, payload):
        if keytype == "edge":
            versions = key.split(",")
            key = "From " + versions[0][:4] + " to " + versions[1][:4]
        elif keytype == "node":
            key = key[:4]

        html_string = "<table style=\"font-size:80%\">"
        html_string += "<th>" + keytype + " : " + key + "</th>"
        for tp in payload:
            html_string += "<tr><td>" + tp + "</td></tr>"
            dims_set = set()
            html_string += "<tr><td>" + "oid" + "</td>"
            for oid in payload[tp]:
                for dim in payload[tp][oid]["dims"]:
                    if dim not in dims_set:
                        dims_set.add(dim)
                        html_string += "<td>" + dim + "</td>"

            html_string += "</tr>"
            for oid in payload[tp]:
                if keytype == "edge":
                    if payload[tp][oid]['types'][tp] == 0:
                        html_string += "<tr style=\"background-color:YellowGreen;\">"
                    elif payload[tp][oid]['types'][tp] == 1:
                        html_string += "<tr style=\"background-color:PowderBlue;\">"
                    elif payload[tp][oid]['types'][tp] == 2:
                        html_string += "<tr style=\"background-color:Coral;\">"
                elif keytype == "node":
                    html_string += "<tr>"
                html_string += "<td>" + str(oid) + "</td>"
                if payload[tp][oid]['types'][tp] in [0, 1]:
                    for dim in dims_set:
                        if dim in payload[tp][oid]["dims"]:
                            html_string += "<td>" + str(payload[tp][oid]["dims"][dim]["value"]) + "</td>"
                        else:
                            html_string += "<td>" + " " + "</td>"
                html_string += "</tr>"
            html_string += "</table>"
            print(html_string)
            return html_string

    def merge_edges(self, end_version):
        payloads = list()
        print(self.vertex_map)
        version = self.vertex_map[end_version]
        while version.prev_master:
            payloads.append(self.edge_map[(version.prev_master, version.current)].payload)
            version = self.vertex_map[version.prev_master]
        # print("payloads", payloads)
        merged = dict()
        for payload in payloads[::-1]:
            merged = merge_state_delta(merged, payload, delete_it=True)
        #return self.delta_to_html(merged)
        return merged

    @property
    def open_tables(self):
        return [(key, self.get_state_at(key, table_type)) for key, table_type in self._open_tables] # if self.valid(key, table_type)]

    def get_state_at(self, key, table_type):
        if table_type == "edge":
            keys = key.split(',')
            from_node, to_node = keys[0], keys[1]
            # print(self.delta_to_table(table_type, key, self.edge_map[(from_node, to_node)].payload))
            return self.delta_to_table(table_type, key, self.edge_map[(from_node, to_node)].payload)

        if table_type == "node":
            return self.delta_to_table(table_type, key, self.merge_edges(key))

    def add_to_table(self, key, table_type):
        # also if it is not in open_tables already.
        if (key, table_type) not in self._open_tables:
            self._open_tables.append((key, table_type))

    def update(self):
        self.next_steps = list()
        self.df.checkout()
        # print (f"{self.appname} Received some request")
        for tp in self.tps:
            for obj in self.df.read_all(tp):
                if obj not in self.command_list and obj != self.current_command:
                    self.command_list.append(obj)

        for command in self.command_list:
            self.next_steps.append(self.get_stages_for_command(command))

    def remove_current_command(self):
            self.command_list.pop(0)

    def get_stages_for_command(self, obj):
        if isinstance(obj, CheckoutObj):
            return ["Checkout [ From " + str(obj.node) + " ] State : " + str(obj.from_version), "Read Changes",
                    "Garbage Collect", "Finish"]

        if isinstance(obj, CommitObj):
            return ["Receive Commit: " + str(obj.from_version) + " to " + str(obj.to_version) +
                    "[ from " + str(obj.node) + " ]", "Apply change", "Garbage Collect", "Finish"]

        if isinstance(obj, PushObj):
            return ["Push [ " + obj.from_version + " to " + obj.to_version + " ] to" + str(obj.receiver_node),
                    "Read changes", "Send changes", "Wait for confirmation from receiver", "Garbage collect", "Finish"]

        if isinstance(obj, AcceptPushObj):
            return ["Accept Push [ " + obj.from_version + " to " + obj.to_version + " ] from" + str(obj.sender_node),
                    "Apply changes", "Garbage collect", "Finish"]

        if isinstance(obj, FetchObj):
            return ["Fetch [ " + obj.from_version + " to " + obj.to_version + " ] from" +
                        str(obj.requestee_node), "Wait for response", "Apply changes", "Garbage collect", "Finish"]

        if isinstance(obj, AcceptFetchObj):
            return ["Accept Fetch [ " + obj.from_version + " to " + obj.to_version + " ] to" + str(obj.receiver_node),
                    "Read changes", "Send changes","Wait for confirmation from receiver", "Garbage collect", "Finish"]

    def execute_checkout(self, obj):
        checkout_display = self.get_stages_for_command(obj)
        if obj.state == obj.CheckoutState.FINISHED:
            self.current_stage[CheckoutObj] = -1
        else:
            self.current_stage[CheckoutObj] += 1
        if obj.state == obj.CheckoutState.FINISHED:
            self.next_steps.pop(0)
            self.prev_steps.append(checkout_display[0])
            self.remove_current_command()
            self.df.delete_one(CheckoutObj, obj)
        elif obj.state == obj.CheckoutState.INIT:
            obj.state = obj.CheckoutState.NEW
            #self.current_stage = 0
        elif obj.state == obj.CheckoutState.NEW:
            obj.start()
            #self.current_stage = 1
            print("CDN gives permission to the node for checkout")
        elif obj.state == obj.CheckoutState.CHECKOUTCOMPLETE:
            print("The CDN knows checkout is complete")
            obj.start_GC()
            print("The CDN gives permission to start GC")
            #self.current_stage = 2
        elif obj.state == obj.CheckoutState.GCCOMPLETE:
            obj.finish()
            #self.current_stage = 3
        self.df.commit()

    def execute_commit(self, obj):
        commit_display = self.get_stages_for_command(obj)

        if obj.state == obj.CommitState.FINISHED:
            self.current_stage[CommitObj] = -1
        else:
            self.current_stage[CommitObj] += 1

        if obj.state == obj.CommitState.FINISHED:
            self.next_steps.pop(0)
            self.prev_steps.append(commit_display[0])
            self.remove_current_command()
            self.df.delete_one(CommitObj, obj)

        elif obj.state == obj.CommitState.INIT:
            obj.state = obj.CommitState.NEW
            #self.current_stage = 0
        elif obj.state == obj.CommitState.NEW:
            obj.start()
            #self.df.commit()
            print("Go ahead from the CDN to the node for commit")
            #self.current_stage = 1
            # print (obj.state, obj.CommitState.COMMITCOMPLETE)
        elif obj.state == obj.CommitState.COMMITCOMPLETE:
            print("The CDN knows the commit is complete")
            start_version = self.managed_heap.version
            node = self.vertex_map[start_version]
            payload = list()
            while node.next_master is not None:
                payload.append(
                    (node.next_master,
                     self.edge_map[(node.current, node.next_master)].payload))
                node = self.vertex_map[node.next_master]

            for e_version, data in payload:
                self.managed_heap.receive_data(
                    data, [start_version, e_version])
                start_version = e_version
            #print (self.managed_heap.data)
            obj.start_GC()
            #self.df.commit()
            #self.current_stage = 2
        elif obj.state == obj.CommitState.GCCOMPLETE:
            obj.finish()
            #self.df.commit()
            #self.current_stage = 3
        self.df.commit()

    def execute_push(self, obj):
        push_display = self.get_stages_for_command(obj)
        if obj.state == obj.PushState.FINISHED:
            self.current_stage[PushObj] = -1
        elif obj.state == obj.PushState.WAIT:
            self.current_stage[PushObj] = 3
            return
        else:
            self.current_stage[PushObj] += 1
            #self.df.commit()
        if obj.state == obj.PushState.FINISHED:
            self.next_steps.pop(0)
            self.prev_steps.append(push_display[0])
            self.remove_current_command()
            self.df.delete_one(PushObj, obj)
            #self.df.commit()
        elif obj.state == obj.PushState.GCCOMPLETE:
            obj.finish()
        elif obj.state == obj.PushState.INIT:
            obj.state = obj.PushState.NEW
            #self.current_stage = 0
            #self.df.commit()
        elif obj.state == obj.PushState.NEW:
            obj.start()
            #self.df.commit()
            #self.current_stage = 1
            print("Go ahead from the CDN to the node for push")
        elif obj.state == obj.PushState.FETCHDELTACOMPLETE:
            print("CDN gets the delta from the sender node and creates a corres. acceptPushObj, push obj:", obj.state, obj.oid)
            acceptPushObj = AcceptPushObj(obj.sender_node, obj.receiver_node, obj.from_version,
                                          obj.to_version, obj.delta, obj.oid)
            print("acceptPush Obj: ", acceptPushObj.oid)
            #acceptPushObj.start()
            receiver_df = self.parent.df
            receiver_df.add_one(AcceptPushObj, acceptPushObj)
            receiver_df.commit()
            #self.current_stage[PushObj] += 1
            obj.wait()

            #self.df.commit()
            #self.current_stage = 2

        #if obj.state == obj.PushState.WAIT:
            #self.current_stage = 3
            #self.df.commit()
        self.df.commit()

    def execute_accept_push(self, obj):
        accept_push_display = self.get_stages_for_command(obj)
        if obj.state == obj.AcceptPushState.FINISHED:
            self.current_stage[AcceptPushObj] = -1
        else:
            self.current_stage[AcceptPushObj] += 1
        if obj.state == obj.AcceptPushState.INIT:
            obj.state = obj.AcceptPushState.NEW
            #self.current_stage = 0
        elif obj.state == obj.AcceptPushState.NEW:
            obj.start()
            #self.df.commit()
            #self.current_stage = 1
        elif obj.state == obj.AcceptPushState.RECEIVECOMPLETE:
            # next_steps.append("Accept Push")
            sender_df = self.child[obj.sender_node].df
            #print(self.appname, self.child[obj.sender_node].appname)
            #pushObj = sender_df.read_one(PushObj, obj.oid)
            pushObj = sender_df.read_one(PushObj, obj.push_obj_oid)
            #print(pushObj, obj.push_obj_oid)
            pushObj.start_GC()
            sender_df.commit()
            obj.start_GC()
            
            start_version = self.managed_heap.version
            node = self.vertex_map[start_version]
            payload = list()
            while node.next_master is not None:
                payload.append(
                    (node.next_master,
                     self.edge_map[(node.current, node.next_master)].payload))
                #print (self.vertices, self.edges)
                #print ((node.current, node.next_master), self.edge_map[(node.current, node.next_master)].payload)
                node = self.vertex_map[node.next_master]
            #print(self.managed_heap.data)
            for e_version, data in payload:
                self.managed_heap.receive_data(
                    data, [start_version, e_version])
                start_version = e_version

            #self.df.commit()
            #self.current_stage = 2
        elif obj.state == obj.AcceptPushState.GCCOMPLETE:
            obj.finish()
            #self.df.commit()
            #self.current_stage = 3
        elif obj.state == obj.AcceptPushState.FINISHED:
            self.next_steps.pop(0)
            self.prev_steps.append(accept_push_display[0])
            self.remove_current_command()
            self.df.delete_one(AcceptPushObj, obj)
            #self.df.commit()
        self.df.commit()

    def execute_fetch(self, obj):
        if obj.state == obj.FetchState.FINISHED:
            self.current_stage[FetchObj] = -1
        elif obj.state == obj.FetchState.WAIT:
            self.current_stage[FetchObj] = 1
            return
        else:
            self.current_stage[FetchObj] += 1
        fetch_display = self.get_stages_for_command(obj)
        if obj.state == obj.FetchState.INIT:
            obj.state = obj.FetchState.NEW
            #self.df.commit()
            #self.current_stage = 0
        elif obj.state == obj.FetchState.NEW:
            print("CDN gets a fetch object and creates a corres. acceptfetchObj")
            acceptFetchObj = AcceptFetchObj(obj.requestor_node, obj.requestee_node,
                                            obj.from_version, obj.to_version, b"", obj.oid)
            acceptFetchObj.start()
            obj.wait()
            #self.df.commit()
            requestee_df = self.parent.df
            requestee_df.add_one(AcceptFetchObj, acceptFetchObj)
            requestee_df.commit()
            #self.current_stage = 1
        elif obj.state == obj.FetchState.RECEIVEDCHANGES:
            obj.start()
            #self.df.commit()
            #self.current_stage = 2
        elif obj.state == obj.FetchState.FETCHCOMPLETE:
            start_version = self.managed_heap.version
            node = self.vertex_map[start_version]
            payload = list()
            while node.next_master is not None:
                payload.append(
                    (node.next_master,
                     self.edge_map[(node.current, node.next_master)].payload))
                if node.current == None:
                    break
                node = self.vertex_map[node.next_master]

            for e_version, data in payload:
                self.managed_heap.receive_data(
                    data, [start_version, e_version])
                start_version = e_version
            obj.start_GC()
            #self.df.commit()
            requestee_df = self.parent.df
            acceptFetchObj = requestee_df.read_one(AcceptFetchObj, obj.oid)
            acceptFetchObj.receive_confirmation()
            requestee_df.commit()
            #self.current_stage = 3
        elif obj.state == obj.FetchState.GCCOMPLETE:
            obj.finish()
            #self.df.commit()
            #self.current_stage = 4
        elif obj.state == obj.FetchState.FINISHED:
            self.next_steps.pop(0)
            self.prev_steps.append(fetch_display[0])
            self.remove_current_command()
            self.df.delete_one(PushObj, obj)
            #self.df.commit()
        self.df.commit()

    def execute_accept_fetch(self, obj):
        if obj.state == obj.AcceptFetchState.FINISHED:
            self.current_stage[AcceptFetchObj] = -1
        elif obj.state == obj.AcceptFetchState.WAIT:
            self.current_stage[AcceptFetchObj] = 3
            return
        else:
            self.current_stage[AcceptFetchObj] += 1
        accept_fetch_display = self.get_stages_for_command(obj)
        if obj.state == obj.AcceptFetchState.INIT:
            obj.state = obj.AcceptFetchState.NEW
            #self.df.commit()
            #self.current_stage = 0
        elif obj.state == obj.AcceptFetchState.NEW:
            obj.start()
            #self.df.commit()
            #self.current_stage = 1
        elif obj.state == obj.AcceptFetchState.SENDCOMPLETE:
            # print("CDN sends the retrieved delta to the requestor")
            requestor_df = self.child[obj.requestor_node].df
            fetchObj = requestor_df.read_one(FetchObj, obj.fetch_obj_oid)
            fetchObj.to_version = obj.to_version
            fetchObj.delta = obj.delta
            fetchObj.receive_changes()
            requestor_df.commit()
            obj.wait()
            #self.df.commit()
            #self.current_stage = 2
        elif obj.state == obj.AcceptFetchState.RECEIVEDCONFIRMATION:
            obj.start_GC()
            #self.df.commit()
            #self.current_stage = 3
        elif obj.state == obj.AcceptFetchState.GCCOMPLETE:
            obj.finish()
            #self.df.commit()
            #self.current_stage = 4
        elif obj.state == obj.AcceptFetchState.FINISHED:
            self.next_steps.pop(0)
            self.prev_steps.append(accept_fetch_display[0])
            self.remove_current_command()
            self.df.delete_one(AcceptFetchObj, obj)
            #self.df.commit()
        self.df.commit()

    def execute(self):
        # Execute one step of self.current_command.
        # This is called by appname/next

        obj = self.current_command
        print(self.appname, self.command_list)#, self.parent, self.child)
        #print( obj, obj.state)
        if isinstance(obj, CheckoutObj):
            self.execute_checkout(obj)

        if isinstance(obj, CommitObj):
            self.execute_commit(obj)

        if isinstance(obj, PushObj):
            self.execute_push(obj)

        if isinstance(obj, AcceptPushObj):
           self.execute_accept_push(obj)

        if isinstance(obj, FetchObj):
           self.execute_fetch(obj)

        if isinstance(obj, AcceptFetchObj):
            self.execute_accept_fetch(obj)

        #self.populate_next_steps()

    def populate_next_steps(self):
        for command in self.command_list:
            self.next_steps.append(self.get_stages_for_command(command))

    def swap(self, pos1, pos2):
        # self.current_command[pos1] should become [pos2] and vice versa.
        pos1, pos2 = int(pos1), int(pos2)
        self.command_list[pos1], self.command_list[pos2] = self.command_list[pos2], self.command_list[pos1]
        print("after swap")
        for command in self.command_list:
            print(command, command.state)
