from threading import Thread
from spacetime.debugger.debugger_types import CommitObj, FetchObj, AcceptFetchObj, Register, \
    CheckoutObj, PushObj, AcceptPushObj, Parent, AppToState
from spacetime.managers.version_graph import Node as Vertex, Edge
from spacetime.managers.managed_heap import ManagedHeap
from json2table import convert
from spacetime.utils.utils import merge_state_delta
import time
from spacetime.utils.enums import Event

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

    @property
    def app_to_state(self):
        return self.df.read_one(AppToState, self.appname).app_to_state

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
        self.current_stage = {tp:0 for tp in self.tps}
        self._open_tables = list()
        self.df.checkout()

    def get_event_type(self, event):
        if event == 0:
            return "New"
        if event == 1:
            return "Modification"
        if event == 2:
            return "Delete"

    def delta_to_table(self, keytype, key, payload):
        print (payload)
        if keytype == "edge":
            versions = key.split(",")
            key = "From " + versions[0][:4] + " to " + versions[1][:4]
        elif keytype == "node":
            key = key[:4]
        css = {
            "edge": "tbledge",
            "node": "tblnode",
            "type": "tbltype"
        }
        html_string = f'<button type="button" class="tblcollapsible {css[keytype]}">{keytype.upper()} - {key}</button>'
        html_string +=  '<div class="content">'
        for tpname, objs in payload.items():
            html_string += f'  <button type="button" class="tblcollapsible {css["type"]}">{tpname.split(".")[-1]}</button>'
            html_string +=  '  <div class="content">'
            dimset = {dimname for objchange in objs.values() for dimname in objchange["dims"] if "dims" in objchange}
            dims = list(dimset)
            html_string += f"    <table>"
            html_string += f"      <thead><tr>{''.join(f'''<td align='center'>{d}</td>''' for d in ['Primary Key'] + dims)}</tr></thead>"
            for oid, objchange in objs.items():
                html_string += f"      <tr{self.get_background_color(keytype, objchange['types'], tpname)}><td align='center'>{oid}</td>{self.get_row(objchange, dims)}</tr>"
            html_string += "    </table>"
            html_string += "  </div>"
        html_string += "</div>"
        print (html_string, list(payload.keys()))
        return html_string
    
    def get_row(self, objchange, dims):
        width = 100.0/(len(dims)+1)
        return "".join(f"<td width='{width}%' align='center'>{objchange['dims'][d]['value'] if 'dims' in objchange and d in objchange['dims'] else ''}</td>" for d in dims)

    def get_background_color(self, keytype, typemap, tpname):
        backgrounds = {Event.New: ' style=\"background-color:YellowGreen;\"', Event.Modification: '', Event.Delete: ' style=\"background-color:Orange;\"'}
        if keytype == "edge":
            return backgrounds[typemap[tpname]]
        return ''

    def merge_edges(self, end_version):
        payloads = list()
        #print(self.vertex_map)
        version = self.vertex_map[end_version]
        while version.prev_master:
            payloads.append(self.edge_map[(version.prev_master, version.current)].payload)
            version = self.vertex_map[version.prev_master]
        # #print("payloads", payloads)
        merged = dict()
        for payload in payloads[::-1]:
            merged = merge_state_delta(merged, payload, delete_it=True)
        #return self.delta_to_html(merged)
        print ("Merged:", merged)
        return merged

    @property
    def open_tables(self):
        deletes = list()
        tables = list()
        for key, table_type in self._open_tables:
            try:
                state = self.get_state_at(key, table_type)
                tables.append((key, state))
            except KeyError as e:
                #print (e)
                deletes.append((key, table_type))
        for key, table_type in deletes:
            self._open_tables.remove((key, table_type))
        return "\n".join(s if s else "" for k, s in tables) if tables else ""

    def get_state_at(self, key, table_type):
        if table_type == "edge":
            keys = key.split(',')
            from_node, to_node = keys[0], keys[1]
            # #print(self.delta_to_table(table_type, key, self.edge_map[(from_node, to_node)].payload))
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
        # #print (f"{self.appname} Received some request")
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
                    "[ from " + f"<a href=/home/{str(obj.node)}/>{str(obj.node)}</a>" + " ]", "Apply change", "Garbage Collect", "Finish"]
        parent = "id='parent'"
        child = "id='child'"
        if isinstance(obj, PushObj):
            return ["Push [ " + obj.from_version + " to " + obj.to_version + " ] to " + f"<a {parent if self.current_command == obj else ''} href=/home/{str(obj.receiver_node)}/>{str(obj.receiver_node)}</a>",
                    "Read changes", "Send changes", "Wait for confirmation from receiver", "Garbage collect", "Finish"]

        if isinstance(obj, AcceptPushObj):
            return ["Accept Push [ " + obj.from_version + " to " + obj.to_version + " ] from " + f"<a {child if self.current_command == obj else ''} href=/home/{str(obj.sender_node)}/>{str(obj.sender_node)}</a>",
                    "Apply changes", "Send Confirmation", "Garbage collect", "Finish"]

        if isinstance(obj, FetchObj):
            return ["Fetch [ since " + obj.from_version + " ] from " +
                        f"<a {parent if self.current_command == obj else ''} href=/home/{str(obj.requestee_node)}/>{str(obj.requestee_node)}</a>", "Send Request", "Wait for response", "Apply changes", "Send Confirmation", "Garbage collect", "Finish"]

        if isinstance(obj, AcceptFetchObj):
            return ["Accept Fetch Request [ since " + obj.from_version + " ] from " + f"<a {child if self.current_command == obj else ''} href=/home/{str(obj.requestor_node)}/>{str(obj.requestor_node)}</a>",
                    "Read changes", "Send changes","Wait for confirmation", "Garbage collect", "Finish"]

    def execute_checkout(self, obj):
        checkout_display = self.get_stages_for_command(obj)
        self.current_stage[CheckoutObj] += 1
        if obj.state == obj.CheckoutState.INIT:
            obj.start()
            #self.current_stage = 1
            self.df.commit()
            #print("CDN gives permission to the node for checkout")
            while True:
                self.df.checkout()
                if obj.state == obj.CheckoutState.CHECKOUTCOMPLETE:
                    break
        elif obj.state == obj.CheckoutState.CHECKOUTCOMPLETE:
            #print("The CDN knows checkout is complete")
            obj.start_GC()
            self.df.commit()
            #print("The CDN gives permission to start GC")
            while True:
                self.df.checkout()
                if obj.state == obj.CheckoutState.GCCOMPLETE:
                    break
            #self.current_stage = 2
        elif obj.state == obj.CheckoutState.GCCOMPLETE:
            obj.finish()
            self.df.commit()
            while True:
                self.df.checkout()
                if self.df.read_one(CheckoutObj, obj.oid) is None:
                    break
            self.prev_steps.append(checkout_display[0])
            self.remove_current_command()
            self.current_stage[CheckoutObj] = 0
            #self.current_stage = 3


    def execute_commit(self, obj):
        commit_display = self.get_stages_for_command(obj)

        self.current_stage[CommitObj] += 1
        if obj.state == obj.CommitState.INIT:
            obj.start()
            self.df.commit()
            #print("Go ahead from the CDN to the node for commit")
            while True:
                self.df.checkout()
                if obj.state == obj.CommitState.COMMITCOMPLETE:
                    break
            #print("The CDN knows the commit is complete")
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
            #self.current_stage = 1
            # #print (obj.state, obj.CommitState.COMMITCOMPLETE)
        elif obj.state == obj.CommitState.COMMITCOMPLETE:
            
            ##print (self.managed_heap.data)
            obj.start_GC()
            self.df.commit()
            while True:
                self.df.checkout()
                if obj.state == obj.CommitState.GCCOMPLETE:
                    break
            
            #self.df.commit()
            #self.current_stage = 2
        elif obj.state == obj.CommitState.GCCOMPLETE:
            obj.finish()
            self.df.commit()
            while True:
                self.df.checkout()
                if self.df.read_one(CommitObj, obj.oid) is None:
                    break

            #self.next_steps.pop(0)
            self.prev_steps.append(commit_display[0])
            self.remove_current_command()
            self.current_stage[CommitObj] = 0
            #self.current_stage = 3

    def execute_push(self, obj):
        push_display = self.get_stages_for_command(obj)
        self.current_stage[PushObj] += 1
        if obj.state == obj.PushState.INIT:
            obj.start()
            self.df.commit()
            while True:
                self.df.checkout()
                if obj.state == obj.PushState.FETCHDELTACOMPLETE:
                    break
        elif obj.state == obj.PushState.FETCHDELTACOMPLETE:
            #print("CDN gets the delta from the sender node and creates a corres. acceptPushObj, push obj:", obj.state, obj.oid)
            acceptPushObj = AcceptPushObj(obj.sender_node, obj.receiver_node, obj.from_version,
                                          obj.to_version, obj.delta, obj.oid)
            #print("acceptPush Obj: ", acceptPushObj.oid)
            #acceptPushObj.start()
            obj.wait()
            self.df.commit()
            receiver_df = self.parent.df
            receiver_df.add_one(AcceptPushObj, acceptPushObj)
            #self.current_stage[PushObj] += 1
            z = 0
        elif obj.state == obj.PushState.WAIT:
            self.current_stage[PushObj] -= 1    
            #print ("waiting", obj.state, obj.PushState.PUSHCOMPLETE)
        elif obj.state == obj.PushState.PUSHCOMPLETE:
            obj.gc_init()
        elif obj.state == obj.PushState.GCINIT:
            obj.start_GC()
            self.df.commit()
            while True:
                self.df.checkout()
                if obj.state == obj.PushState.GCCOMPLETE:
                    break
        elif obj.state == obj.PushState.GCCOMPLETE:
            obj.finish()
            self.df.commit()
            while True:
                self.df.checkout()
                if self.df.read_one(PushObj, obj.oid) is None:
                    break
            self.prev_steps.append(push_display[0])
            self.remove_current_command()
            self.current_stage[PushObj] = 0

    def execute_accept_push(self, obj):
        accept_push_display = self.get_stages_for_command(obj)
        self.current_stage[AcceptPushObj] += 1
        if obj.state == obj.AcceptPushState.INIT:
            obj.start()
            self.df.commit()
            while True:
                self.df.checkout()
                if obj.state == obj.AcceptPushState.RECEIVECOMPLETE:
                    break
            #self.current_stage = 1
            # next_steps.append("Accept Push")
            #print ("Completed accept push phase 1.")
            start_version = self.managed_heap.version
            node = self.vertex_map[start_version]
            payload = list()
            while node.next_master is not None:
                payload.append(
                    (node.next_master,
                     self.edge_map[(node.current, node.next_master)].payload))
                ##print (self.vertices, self.edges)
                ##print ((node.current, node.next_master), self.edge_map[(node.current, node.next_master)].payload)
                node = self.vertex_map[node.next_master]
            ##print(self.managed_heap.data)
            for e_version, data in payload:
                self.managed_heap.receive_data(
                    data, [start_version, e_version])
                start_version = e_version
        elif obj.state == obj.AcceptPushState.RECEIVECOMPLETE:
            sender_df = self.child[obj.sender_node].df
            ##print(self.appname, self.child[obj.sender_node].appname)
            #pushObj = sender_df.read_one(PushObj, obj.oid)
            pushObj = sender_df.read_one(PushObj, obj.push_obj_oid)
            ##print(pushObj, obj.push_obj_oid)
            pushObj.complete_PUSH()
            obj.wait()           
        elif obj.state == obj.AcceptPushState.WAIT:
            obj.start_GC()
            self.df.commit()
            while True:
                self.df.checkout()
                if obj.state == obj.AcceptPushState.GCCOMPLETE:
                    break
            #self.current_stage = 2
        elif obj.state == obj.AcceptPushState.GCCOMPLETE:
            obj.finish()
            self.df.commit()
            while True:
                self.df.checkout()
                if self.df.read_one(AcceptPushObj, obj.oid) is None:
                    break
            self.prev_steps.append(accept_push_display[0])
            self.remove_current_command()
            self.current_stage[AcceptPushObj] = 0
            #self.df.commit()
            #self.current_stage = 3

    def execute_fetch(self, obj):
        fetch_display = self.get_stages_for_command(obj)
        self.current_stage[FetchObj] += 1

        if obj.state == obj.FetchState.INIT:
            #print("CDN gets a fetch object and creates a corres. acceptfetchObj")
            acceptFetchObj = AcceptFetchObj(obj.requestor_node, obj.requestee_node,
                                            obj.from_version, obj.to_version, b"", obj.oid)
            obj.wait()
            #self.df.commit()
            requestee_df = self.parent.df
            requestee_df.add_one(AcceptFetchObj, acceptFetchObj)
            obj.accept_fetch_obj_oid = acceptFetchObj.oid
            #self.current_stage = 1
        elif obj.state == obj.FetchState.WAIT:
            self.current_stage[FetchObj] -= 1
        elif obj.state == obj.FetchState.RECEIVEDCHANGES:
            obj.ready_to_send()
        elif obj.state == obj.FetchState.READYSEND:
            obj.start()
            self.df.commit()
            while True:
                self.df.checkout()
                if obj.state == obj.FetchState.FETCHCOMPLETE:
                    break
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
            #self.df.commit()
            #self.current_stage = 2
        elif obj.state == obj.FetchState.FETCHCOMPLETE:
            # send ack
            requestee_df = self.parent.df
            acceptFetchObj = requestee_df.read_one(AcceptFetchObj, obj.accept_fetch_obj_oid)
            acceptFetchObj.receive_confirmation()
            obj.init_gc()
        elif obj.state == obj.FetchState.GCINIT:
            obj.start_GC()
            self.df.commit()
            while True:
                self.df.checkout()
                if obj.state == obj.FetchState.GCCOMPLETE:
                    break
            #self.current_stage = 3
        elif obj.state == obj.FetchState.GCCOMPLETE:
            obj.finish()
            self.df.commit()
            while True:
                self.df.checkout()
                if self.df.read_one(FetchObj, obj.oid) is None:
                    break
            self.prev_steps.append(fetch_display[0])
            self.remove_current_command()
            self.current_stage[FetchObj] = 0

    def execute_accept_fetch(self, obj):
        accept_fetch_display = self.get_stages_for_command(obj)
        self.current_stage[AcceptFetchObj] += 1
        if obj.state == obj.AcceptFetchState.INIT:
            obj.start()
            self.df.commit()
            while True:
                self.df.checkout()
                if obj.state == obj.AcceptFetchState.SENDCOMPLETE:
                    break
            #self.df.commit()
            #self.current_stage = 1
        elif obj.state == obj.AcceptFetchState.SENDCOMPLETE:
            # #print("CDN sends the retrieved delta to the requestor")
            requestor_df = self.child[obj.requestor_node].df
            fetchObj = requestor_df.read_one(FetchObj, obj.fetch_obj_oid)
            fetchObj.to_version = obj.to_version
            fetchObj.delta = obj.delta
            fetchObj.receive_changes()
            obj.wait()
            #self.df.commit()
            #self.current_stage = 2
        elif obj.state == obj.AcceptFetchState.WAIT:
            self.current_stage[AcceptFetchObj] -= 1    
        elif obj.state == obj.AcceptFetchState.RECEIVEDCONFIRMATION:
            obj.ready_to_send()
        elif obj.state == obj.AcceptFetchState.READYSEND:
            obj.start_GC()
            self.df.commit()
            while True:
                self.df.checkout()
                if obj.state == obj.AcceptFetchState.GCCOMPLETE:
                    break
            #self.current_stage = 3
        elif obj.state == obj.AcceptFetchState.GCCOMPLETE:
            obj.finish()
            self.df.commit()
            while True:
                self.df.checkout()
                if self.df.read_one(AcceptFetchObj, obj.oid) is None:
                    break
            self.prev_steps.append(accept_fetch_display[0])
            self.remove_current_command()
            self.current_stage[AcceptFetchObj] = 0

    def execute(self):
        # Execute one step of self.current_command.
        # This is called by appname/next

        obj = self.current_command
        #print(self.appname, self.command_list)#, self.parent, self.child)
        ##print( obj, obj.state)
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
        pos1, pos2 = int(pos1), int(pos2)
        self.command_list[pos1], self.command_list[pos2] = self.command_list[pos2], self.command_list[pos1]
