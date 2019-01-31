from uuid import uuid4
from multiprocessing import Process

from spacetime.dataframe import Dataframe
from spacetime.utils.enums import VersionBy, ConnectionStyle

def get_details(dataframe):
    if isinstance(dataframe , Dataframe):
        return dataframe.details
    elif isinstance(dataframe, tuple):
        return dataframe
    raise RuntimeError(
        "Do not know how to connect to dataframe with given data")

def get_app(func, types, producer,
            getter_setter, getter, setter, deleter):

    class App(Process):
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

        def __init__(
                self, dataframe=None, server_port=0,
                version_by=VersionBy.FULLSTATE, instrument=None, dump_graph=None,
                connection_as=ConnectionStyle.TSocket):
            self.appname = "{0}_{1}".format(func.__name__, str(uuid4()))
            self.producer = producer
            self.getter_setter = getter_setter
            self.getter = getter
            self.setter = setter
            self.deleter = deleter
            self.types = types

            self.func = func
            self.args = tuple()
            self.kwargs = dict()
            self.version_by = version_by
            self.dataframe_details = (
                get_details(dataframe) if dataframe else None)

            self.server_port = server_port
            self.instrument = instrument
            self.dump_graph = dump_graph
            self.connection_as = connection_as
            super().__init__()
            self.daemon = False

        def run(self):
            # Create the dataframe.
            dataframe = self._create_dataframe()
            # Fork the dataframe for initialization of app.
            dataframe.checkout()
            # Run the main function of the app.
            self.func(dataframe, *self.args, **self.kwargs)
            # Merge the final changes back to the dataframe.
            dataframe.commit()
            dataframe.push()

        def _start(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            super().start()

        def start(self, *args, **kwargs):
            self._start(*args, **kwargs)
            self.join()

        def start_async(self, *args, **kwargs):
            self._start(*args, **kwargs)

        def _create_dataframe(self):
            df = Dataframe(
                    self.appname, self.all_types,
                    details=self.dataframe_details,
                    server_port=self.server_port,
                    version_by=self.version_by,
                    connection_as=self.connection_as,
                    instrument=self.instrument,
                    dump_graph=self.dump_graph)
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

def Application(
        target, dataframe=None, server_port=0,
        Types=list(), Producer=list(), GetterSetter=list(),
        Getter=list(), Setter=list(), Deleter=list(),
        version_by=VersionBy.FULLSTATE, instrument=None, dump_graph=None, connection_as=ConnectionStyle.TSocket):
    app_cls = get_app(
        target, set(Types), set(Producer), set(GetterSetter),
        set(Getter), set(Setter), set(Deleter))
    return app_cls(
        dataframe=dataframe, server_port=server_port, version_by=version_by,
        instrument=instrument, dump_graph=dump_graph, connection_as=connection_as)
