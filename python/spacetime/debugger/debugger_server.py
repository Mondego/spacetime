from spacetime.dataframe import Dataframe
from threading import Thread
from rtypes import pcc_set, primarykey, dimension, merge
from spacetime.debugger.debug_dataframe import DebugDataframe
from spacetime.debugger.debugger_types import CommitObj, FetchObj, AcceptFetchObj, Register
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

    existing_obj ={ "CommitObj":[], "FetchObj":[],"AcceptFetchObj":[]}
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
                                           { CommitObj, FetchObj, AcceptFetchObj})  # Create a dataframe for the new client
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
            current_dataframes = dataframes
            for df in current_dataframes.values():
                #print(df.details)
                df.checkout()
                new_objects = list()
                for type in existing_obj.keys():
                    new_objects += df.read_all(eval(type))
                for obj in new_objects:
                    if isinstance(obj, CommitObj):
                        #print("in CDN",obj,obj.state)

                        if obj.state == obj.CommitState.INIT:

                            obj.start()
                            df.commit()

                            print("Go ahead from the CDN to the node for commit")
                            print (obj.state, obj.CommitState.COMMITCOMPLETE)
                        if obj.state == obj.CommitState.COMMITCOMPLETE:
                                print("The CDN knows the commit is complete")
                                obj.start_GC()
                                df.commit()


                    if isinstance(obj, FetchObj):
                        if obj.state == obj.FetchState.INIT:
                            acceptFetchObj = AcceptFetchObj(obj.requestor_node, obj.requestee_node,
                                                            obj.from_version, obj.to_version, None, obj.oid)
                            acceptFetchObj.start()
                            obj.accept_fetch_obj_oid = acceptFetchObj.oid
                            df.commit()
                            requestee_df = current_dataframes[obj.requestee_node]
                            requestee_df.add_one(AcceptFetchObj, acceptFetchObj)
                            requestee_df.commit()

                        if obj.state == obj.FetchState.FETCHCOMPLETE:
                            obj.start_GC()

                        if isinstance(obj, AcceptFetchObj):
                            if obj.state == AcceptFetchObj.AcceptFetchState.SENDCOMPLETE:
                                requestor_df = current_dataframes[obj.requestor_node]
                                fetchObj = requestor_df.read_one(FetchObj, obj.fetch_obj_oid)
                                fetchObj.to_version = obj.to_version
                                fetchObj.delta = obj.delta
                                fetchObj.start()
                                requestor_df.commit()



    f = open("debugger_log.txt", "w+")
    f.write("Logging:"+ "\n")
    f.close()
    check_for_new_nodes_thread = Thread(target=check_for_new_nodes)
    check_for_new_nodes_thread.start()
    logger_thread = Thread(target=logger)
    logger_thread.start()
    check_for_new_nodes_thread.join()
    logger_thread.join()