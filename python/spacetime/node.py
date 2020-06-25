from uuid import uuid4
from multiprocessing import Process, Queue
from threading import Thread
import cProfile
import time

# from spacetime.dataframe import Dataframe
from spacetime.utils.enums import VersionBy, ConnectionStyle, AutoResolve
from spacetime.dataframe_cpp import DataframeCPP
from spacetime.dataframe_pure import DataframePure

def get_details(dataframe):
    if (isinstance(dataframe , DataframeCPP)
            or isinstance(dataframe, DataframePure)):
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
                self, appname, dataframe=None, is_server=False, server_port=0,
                resolver=None, pure_python=False):
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
            self.is_server = is_server or self.server_port != 0
            self.pure_python = pure_python
            self.resolver = resolver
            self._ret_value = Queue()
            super().__init__()
            self.daemon = False

        def run(self):
            # Create the dataframe.
            self.cr = None
            dataframe = self._create_dataframe()
            try:
                self._port_fetcher.put(dataframe.details)
                # Fork the dataframe for initialization of app.
                dataframe.checkout()
                # Run the main function of the app.
                self._ret_value.put(
                    self.func(dataframe, *self.args, **self.kwargs))
                # Merge the final changes back to the dataframe.
                dataframe.commit()
                dataframe.push()
                # if not dataframe.repository.is_connected():
                #     time.sleep(3)
            finally:
                # del dataframe
                dataframe.force_del_repo()

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
            Dataframe = DataframePure if self.pure_python else DataframeCPP
            df = Dataframe(
                self.appname, self.all_types,
                details=self.dataframe_details,
                server_port=self.server_port,
                resolver=self.resolver,
                is_server=self.is_server)
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

def Node(
        target, appname=None,
        dataframe=None, server_port=0, is_server=False,
        Types=list(), Producer=list(), GetterSetter=list(),
        Getter=list(), Setter=list(), Deleter=list(),
        threading=False, resolver=None, pure_python=False):
    if not appname:
        appname = "{0}_{1}".format(target.__name__, str(uuid4()))
    app_cls = get_app(
        target, set(Types), set(Producer), set(GetterSetter),
        set(Getter), set(Setter), set(Deleter), threading=threading)
    return app_cls(
        appname, dataframe=dataframe, is_server=is_server,
        server_port=server_port, resolver=resolver, pure_python=pure_python)
