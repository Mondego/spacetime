from multiprocessing import Process, Event, Queue
from uuid import uuid4

import time
import redis

from subprocess import check_output, call


def my_print(*args):
    print(*args)
    sys.stdout.flush()

class RedisNode(Process):
    @property
    def details(self):
        return self.port

    def __init__(self, func, port, slave=None):
        self.port = port
        self.appname = "{0}_{1}".format(func.__name__, str(uuid4()))
        self.func = func
        self.args = tuple()
        self.kwargs = dict()
        self.dataframe_details = slave if slave else None

        self._ret_value = Queue()
        super().__init__()
        self.daemon = False

    def run(self):
        # Create the dataframe.
        dataframe = self._create_redis_dataframe()
        # Fork the dataframe for initialization of app.
        # Run the main function of the app.
        self._ret_value.put(self.func(dataframe, *self.args, **self.kwargs))

    def _start(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        super().start()
        
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
       

    def _create_redis_dataframe(self):
        df = RedisDataframe(self.port, details=self.dataframe_details)
        return df


class RedisDataframe(object):
    def __init__(self, port, details=None):
        self.port = port
        self.details = details
        self.rcon, self.wcon = self.setup_redis()
        self.commit_stack = dict()
        self.pipe = self.wcon.pipeline(transaction=True)

    def setup_redis(self):
        p = Process(target=self.redis)
        p.daemon = True
        p.start()
        time.sleep(1)
        if self.details:

            check_output(["redis-cli", "-p", str(self.port),
                "slaveof", "127.0.0.1", str(self.details)])
        rcon = redis.Redis(port=self.port)
        wcon = redis.Redis(port=self.details) if self.details else rcon
        return rcon, wcon

    def redis(self):
        check_output(["redis-server", "--port", str(self.port), "--save", "''"])

    def read_all(self, dtpname, dims):
        rows = self.rcon.scan_iter(match="{0}*".format(dtpname))
        pipe = self.rcon.pipeline(transaction=True)
        keys = list()
        for key in rows:
            pipe.hmget(key, dims)
            keys.append(key)
        resps = pipe.execute()
        result = dict()
        for k, v in zip(keys, resps):
            _, oid = k.split(b":")
            if not v:
                continue
            for dim, value in zip(dims, v):
                if dim == b"create_ts":
                    value = float(v.decode("utf-8"))
                else:
                    value = value.decode("utf-8")
                result.setdefault(oid.decode("utf-8"), dict())[dim] = value
        return result

    def read_one(self, dtpname, dims, oid):
        result = dict()
        resp = self.rcon.hmget("{0}:{1}".format(dtpname, oid), dims)
        result = dict()
        resp = [v for v in resp if v]
        if not resp:
            return result
        for dim, value in zip(dims, resp):
            if dim == b"create_ts":
                value = float(value.decode("utf-8"))
            else:
                value = value.decode("utf-8")
            result[dim] = value
        return result

    def add_one(self, dtpname, oid, obj):
        self.commit_stack.setdefault(dtpname, dict())[oid] = obj

    def add_many(self, dtpname, objs):
        for oid, obj in objs.items():
            self.add_one(dtpname, oid, obj)

    def write_dim(self, dtpname, oid, dim, value):
        self.commit_stack.setdefault(dtpname, dict()).setdefault(oid, dict())[dim] = value

    def push(self):
        pipe = self.wcon.pipeline(transaction=True)
        for dtpname in self.commit_stack:
            for oid, delta in self.commit_stack[dtpname].items():
                pipe.hmset("{0}:{1}".format(dtpname, oid), delta)
        pipe.execute()
        self.commit_stack = dict()
    
    def checkout(self):
        self.commit_stack = dict()
    
    def commit(self):
        self.push()

    def sync(self):
        self.push()
        self.checkout()
