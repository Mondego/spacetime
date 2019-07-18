from uuid import uuid4
from multiprocessing import Process, Queue
from threading import Thread

from spacetime.dataframe import Dataframe
from spacetime.utils.enums import VersionBy, ConnectionStyle, AutoResolve

from spacetime.debugger.debugger_server import register_func
from spacetime.debugger.debug_dataframe import DebugDataframe
from spacetime.debugger.debugger_types import  CommitObj, FetchObj, AcceptFetchObj, Register


def get_details(dataframe):
    if isinstance(dataframe , Dataframe):
        return dataframe.details
    elif isinstance(dataframe, tuple):
        return dataframe
    elif isinstance(dataframe, list):
        return tuple(dataframe)
    raise RuntimeError(
        "Do not know how to connect to dataframe with given data", dataframe)

def get_app(func, types, producer,
            getter_setter, getter, setter, deleter, threading=False):

    class App(Thread if threading else Process):
        @property
        def type_map(self):
            return {
                "producer": self.producer,
                "gettersetter": self.getter_setter,
                "getter": self.getter,
                "setter": self.setter,
                "deleter": self.deleter,
            }

        @property
        def all_types(self):
            return self.producer.union(
                self.getter_setter).union(
                    self.getter).union(
                        self.setter).union(
                            self.deleter).union(self.types)

        @property
        def details(self):
            if not self._port:
                raise RuntimeError(
                    "Port is only bound when Node is started. "
                    "Call start, then query the port")
            return self._port

        def __init__(
                self, appname, dataframe=None, server_port=0,
                instrument=None, dump_graph=None,
                connection_as=ConnectionStyle.TSocket, resolver=None,
                autoresolve=AutoResolve.FullResolve, mem_instrument=False):
            self._port = None
            self.appname = appname
            self.producer = producer
            self.getter_setter = getter_setter
            self.getter = getter
            self.setter = setter
            self.deleter = deleter
            self.types = types

            self.func = func
            self.args = tuple()
            self.kwargs = dict()
            self.dataframe_details = (
                get_details(dataframe) if dataframe else None)

            self.server_port = server_port
            self.instrument = instrument
            self.dump_graph = dump_graph
            self.connection_as = connection_as
            self.resolver = None
            self.autoresolve = autoresolve
            self._ret_value = Queue()
            self.mem_instrument = mem_instrument
            super().__init__()
            self.daemon = False

        def run(self):
            # Create the dataframe.
            dataframe = self._create_dataframe()
            self._port_fetcher.put(dataframe.details)
            # Fork the dataframe for initialization of app.
            dataframe.checkout()
            # Run the main function of the app.
            self._ret_value.put(self.func(dataframe, *self.args, **self.kwargs))
            # Merge the final changes back to the dataframe.
            dataframe.commit()
            dataframe.push()

        def _start(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self._port_fetcher = Queue()
            super().start()
            self._port = self._port_fetcher.get()
            self._port_fetcher.close()
            del self._port_fetcher

        def start(self, *args, **kwargs):
            self._start(*args, **kwargs)
            return self.join()

        def start_async(self, *args, **kwargs):
            self._start(*args, **kwargs)

        def join(self):
            ret_value = self._ret_value.get()
            self._ret_value.close()
            super().join()
            return ret_value
            

        def _create_dataframe(self):
            df = Dataframe(
                self.appname, self.all_types,
                details=self.dataframe_details,
                server_port=self.server_port,
                connection_as=self.connection_as,
                instrument=self.instrument,
                dump_graph=self.dump_graph,
                resolver=None,
                autoresolve=self.autoresolve,
                mem_instrument=self.mem_instrument)
            #print(self.appname, self.all_types, details, df.details)
            return df
    return App


class app(object):
    def __init__(
            self, Types=list(), Producer=list(), GetterSetter=list(),
            Getter=list(), Setter=list(), Deleter=list()):
        self.producer = set(Producer)
        self.getter_setter = set(GetterSetter)
        self.getter = set(Getter)
        self.setter = set(Setter)
        self.deleter = set(Deleter)
        self.types = set(Types)

    def __call__(self, func):
        return get_app(
            func, self.types, self.producer, self.getter_setter,
            self.getter, self.setter, self.deleter)

def debug_function(df, appname, func, types, parent_details, server_port, *args, **kwargs):
    debug_dataframe = DebugDataframe(df, appname, types, server_port, parent_details)
    #debug_dataframe.pull()
    return_value = func(debug_dataframe, *args, **kwargs)
    #debug_dataframe.push()
    return return_value

class DebugNode(Process):
    def __init__(self, appname, target, Types, debug, parent, server_port):
        self.appname = appname
        self.func = target
        self.types = Types
        self.debug = debug
        self.parent_details = (get_details(parent) if parent else None)
        self.server_port = server_port
        self.node = None
        self.args = list()
        self.kwargs = dict()
        super().__init__()

    def start(self, *args, **kwargs):
        self.start_async(*args, **kwargs)
        return self.join()

    def join(self):
        super().join()

    def start_async(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        # To register with debugger server and get the port to communicate with the dataframe in the debugger server
        register_node = Node(register_func, Types=[Register], dataframe=self.debug)
        self.port = register_node.start(self.appname)
        print("port", self.port)
        self.node = Node(debug_function, Types=[CommitObj, FetchObj, AcceptFetchObj], dataframe=(self.debug[0], self.port))
        self.node.start_async(self.appname, self.func, self.types, self.parent_details, self.server_port,
                              *self.args, **self.kwargs)
        super().start()

def Node(
        target, appname=None,
        dataframe=None, server_port=0,
        Types=list(), Producer=list(), GetterSetter=list(),
        Getter=list(), Setter=list(), Deleter=list(),
        threading=False,
        instrument=None, dump_graph=None,
        connection_as=ConnectionStyle.TSocket, resolver=None,
        autoresolve=AutoResolve.FullResolve, mem_instrument=False, debug=None):
    if not appname:
        appname = "{0}_{1}".format(target.__name__, str(uuid4()))
    if debug:
        return DebugNode(appname, target, Types, debug, dataframe, server_port)
    app_cls = get_app(
        target, set(Types), set(Producer), set(GetterSetter),
        set(Getter), set(Setter), set(Deleter), threading=threading)
    return app_cls(
        appname, dataframe=dataframe, server_port=server_port,
        instrument=instrument, dump_graph=dump_graph,
        connection_as=connection_as, resolver=None,
        autoresolve=autoresolve, mem_instrument=mem_instrument)
