from uuid import uuid4
from multiprocessing import Process, Queue
from threading import Thread
import cProfile

from spacetime.dataframe import Dataframe
from spacetime.utils.enums import VersionBy, ConnectionStyle, AutoResolve

def get_details(dataframe):
    if isinstance(dataframe, Dataframe):
        return dataframe.details
    elif isinstance(dataframe, tuple):
        return dataframe
    elif isinstance(dataframe, list):
        return tuple(dataframe)
    raise RuntimeError(
        "Do not know how to connect to dataframe with given data", dataframe)

def get_app(func, types, threading=False):

    class Node(Thread if threading else Process):
        @property
        def type_map(self):
            return {"types": self.types}

        @property
        def all_types(self):
            return self.types

        @property
        def details(self):
            if not self._port:
                raise RuntimeError(
                    "Port is only bound when Node is started. "
                    "Call start, then query the port")
            return self._port

        def __init__(
                self, appname, remotes=None, server_port=0, resolver=None):
            self._port = None
            self.appname = appname
            self.types = types

            self.func = func
            self.args = tuple()
            self.kwargs = dict()
            self.dataframe_details = {
                remote: get_details(dataframe) if dataframe else None
                for remote, dataframe in remotes.items()}

            self.server_port = server_port
            self.resolver = resolver
            self._ret_value = Queue()
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
            # dataframe.commit()
            # dataframe.push()

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
                server_port=self.server_port,
                remotes=self.dataframe_details,
                resolver=self.resolver)
            #print(self.appname, self.all_types, details, df.details)
            return df
    return Node

def Node(target, appname=None, Types=list(),
         remotes=None, server_port=0, threading=False, resolver=None):
    if remotes is None:
        remotes = dict()
    if not appname:
        appname = "{0}_{1}".format(target.__name__, str(uuid4()))
    app_cls = get_app(target, set(Types), threading=threading)
    return app_cls(
        appname, remotes=remotes, server_port=server_port, resolver=resolver)
