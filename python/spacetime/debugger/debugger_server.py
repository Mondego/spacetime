from spacetime.dataframe import Dataframe
from threading import Thread
from rtypes import pcc_set, primarykey, dimension, merge
from spacetime.debugger.debug_dataframe import DebugDataframe
from spacetime.debugger.debugger_types import CommitObj, FetchObj, AcceptFetchObj, Register, CheckoutObj, PushObj, AcceptPushObj
import copy
import time


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
                    f = open("debugger_log.txt", "a+")
                    f.write("A new node registers with the server:" + str(register_obj.appname) + "\n")
                    f.close()

                    current_df = Dataframe(register_obj.appname,
                                           { CommitObj, FetchObj, AcceptFetchObj, CheckoutObj, PushObj, AcceptPushObj})  # Create a dataframe for the new client
                    print("The dataframe that is created for this node is" + str(current_df) + "\n")
                    dataframes[register_obj.appname] = current_df
                    register_obj.port = current_df.details[1]

                    print("The port assigned to this node is " + str(register_obj.port) + "\n")
                    f = open("debugger_log.txt", "a+")
                    f.write("The port assigned to this node is " + str(register_obj.port) + "\n")
                    f.close()

                    no_of_nodes_joined += 1
                    print("no of nodes that have registered with the server is " + str(no_of_nodes_joined) + "\n")
                    f = open("debugger_log.txt", "a+")
                    f.write("no of nodes that have registered with the server is " + str(no_of_nodes_joined) + "\n")
                    f.close()

                    df.commit()

    def logger():
        while True:
            current_dataframes = dataframes.copy()
            for df in current_dataframes.values():
                #print(df.details)
                df.checkout()
                new_objects = list()
                for type in existing_obj:
                    #print(type)
                    new_objects += df.read_all(type)
                #print(new_objects)
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

    check_for_new_nodes_thread = Thread(target=check_for_new_nodes)
    check_for_new_nodes_thread.start()
    logger_thread = Thread(target=logger)
    logger_thread.start()
    check_for_new_nodes_thread.join()
    logger_thread.join()