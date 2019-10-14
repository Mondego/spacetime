from threading import Thread
from spacetime.debugger.debugger_types import CommitObj, FetchObj, AcceptFetchObj, Register, \
    CheckoutObj, PushObj, AcceptPushObj, Parent


class NodeState(Thread):
    def __init__(self, appname, df):
        self.df = df
        self.appname = appname
        self.parent = None
        self.child = dict()
        self.managed_heap = None
        self.command_list = list()
        self.current_command = None
        self.next_steps = list()
        self.prev_steps = list()
        self.current_stage = 0
        self.tps = [CommitObj, FetchObj, AcceptFetchObj, CheckoutObj, PushObj, AcceptPushObj]
        self.df.checkout()
        super().__init__(daemon=True)
        self.start()

    def run(self):
        while True:
            self.df.checkout_await()
            for tp in self.tps:
                for obj in self.df.read_all(tp):
                    if obj not in self.command_list and obj != self.current_command:
                        self.command_list.append(obj)

    def get_next_command(self, command):
        if command == self.current_command:
            self.command_list.pop(0)
            self.current_command = self.command_list[0]

    def get_stages_for_command(self, obj):
        if isinstance(obj, CheckoutObj):
            return ["Checkout [From " + str(obj.node) + " ] State : " + str(obj.from_version),"Read Changes",
                    "Garbage Collect", "Finish"]

        if isinstance(obj, CommitObj):
            return ["Receive Commit: " + str(obj.from_version) + " to " + str(obj.to_version) +
                    "[from " + str(obj.node) + " ]", "Apply change", "Garbage Collect", "Finish"]

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
            self.prev_steps.append(checkout_display)
            self.get_next_command(obj)
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
        if obj.state == obj.CheckoutState.FINISHED:
            self.current_stage = 0
        else:
            self.current_stage += 1
        self.df.commit()

    def execute_commit(self, obj):
        commit_display = self.get_stages_for_command(obj)
        if obj.state == obj.CommitState.FINISHED:
            self.prev_steps.append(commit_display)
            self.get_next_command(obj)
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
            obj.start_GC()
            #self.df.commit()
            #self.current_stage = 2
        elif obj.state == obj.CommitState.GCCOMPLETE:
            obj.finish()
            #self.df.commit()
            #self.current_stage = 3

        self.df.commit()
        if obj.state == obj.CommitState.FINISHED:
            self.current_stage = 0
        else:
            self.current_stage += 1

    def execute_push(self, obj):
        push_display = self.get_stages_for_command(obj)
        if obj.state == obj.PushState.GCCOMPLETE:
            obj.finish()
            #self.df.commit()

        elif obj.state == obj.PushState.FINISHED:
            self.prev_steps.append(push_display)
            self.get_next_command(obj)
            self.df.delete_one(PushObj, obj)
            #self.df.commit()

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
            print("CDN gets the delta from the sender node and creates a corres. acceptPushObj")
            acceptPushObj = AcceptPushObj(obj.sender_node, obj.receiver_node, obj.from_version,
                                          obj.to_version, obj.delta, obj.oid)
            #acceptPushObj.start()
            receiver_df = self.parent.df
            receiver_df.add_one(AcceptPushObj, acceptPushObj)
            receiver_df.commit()
            obj.wait()
            #self.df.commit()
            #self.current_stage = 2

        #if obj.state == obj.PushState.WAIT:
            #self.current_stage = 3
            #self.df.commit()
        self.df.commit()
        if obj.state == obj.PushState.FINISHED:
            self.current_stage = 0
        elif obj.state == obj.PushState.WAIT:
            return
        else:
            self.current_stage += 1

    def execute_accept_push(self, obj):
        accept_push_display = self.get_stages_for_command(obj)

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
            pushObj = sender_df.read_one(PushObj, obj.oid)
            pushObj.start_GC()
            sender_df.commit()
            obj.start_GC()
            #self.df.commit()
            #self.current_stage = 2

        elif obj.state == obj.AcceptPushState.GCCOMPLETE:
            obj.finish()
            #self.df.commit()
            #self.current_stage = 3

        elif obj.state == obj.AcceptPushState.FINISHED:
            self.prev_steps.append(accept_push_display)
            self.get_next_command(obj)
            self.df.delete_one(AcceptPushObj, obj)
            #self.df.commit()

        self.df.commit()
        if obj.state == obj.AcceptPushState.FINISHED:
            self.current_stage = 0
        else:
            self.current_stage += 1

    def execute_fetch(self, obj):
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
            self.prev_steps.append(fetch_display)
            self.get_next_command(obj)
            self.df.delete_one(PushObj, obj)
            #self.df.commit()

        self.df.commit()
        if obj.state == obj.FetchState.FINISHED:
            self.current_stage = 0
        elif obj.state == obj.FetchState.WAIT:
            return
        else:
            self.current_stage += 1

    def execute_accept_fetch(self, obj):
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
            fetchObj = requestor_df.read_one(FetchObj, obj.oid)
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
            self.prev_steps.append(accept_fetch_display)
            self.get_next_command(obj)
            self.df.delete_one(AcceptFetchObj, obj)
            #self.df.commit()
        self.df.commit()
        if obj.state == obj.AcceptFetchState.FINISHED:
            self.current_stage = 0
        elif obj.state == obj.AcceptFetchState.WAIT:
            return
        else:
            self.current_stage += 1

    def execute(self):
        # Execute one step of self.current_command.
        # This is called by appname/next
        print(self.appname)
        for command in self.command_list:
            print(command, command.state)

        self.next_steps = list()
        obj = self.current_command
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

        for command in self.command_list:
            self.next_steps.append(self.get_stages_for_command(command))
        print(self.next_steps)
        print(self.current_stage)

    def swap(self, pos1, pos2):
        # self.current_command[pos1] should become [pos2] and vice versa.
        pass