from multiprocessing import Process, Event
from multiprocessing import Queue
from threading import Thread
import queue


def new_func(inq, outq):
    while True:
        command = inq.get()
        func, chunk, filter_func = command
        for args in chunk:
            output = func(args)
            if filter_func is None or filter_func(output):
                outq.put(output)

class Pool():
    def __init__(self, process_count, max_queue_size=1000):
        self.max_queue_size = max_queue_size
        self.inq = Queue(self.max_queue_size)
        self.outq = Queue(self.max_queue_size)
        self.processes = [
            Process(target=new_func, args=(self.inq, self.outq), daemon=True)
            for _ in range(process_count)]
        for p in self.processes:
            p.start()

    def __enter__(self):
        return self

    def __exit__(self, err_cls, err, tb):
        if err:
            raise err
        self.close()

    def close(self):
        for p in self.processes:
            p.terminate()
        self.inq.close()
        self.outq.close()

    def map(self, func, iterable,
            filter_func=None, chunksize=1, use_tqdm=False):
        tqdm_obj = None
        if use_tqdm:
            import tqdm
            tqdm_obj = tqdm.tqdm()
        stop_event = Event()
        input_thread = Thread(
            target=self.input_provider,
            args=(func, iterable, filter_func, stop_event, chunksize),
            daemon=True)
        input_thread.start()
        while (not stop_event.is_set()
               or self.inq.qsize() > 0
               or self.outq.qsize() > 0):
            try:
                yield self.outq.get_nowait()
                if tqdm_obj is not None:
                    tqdm_obj.update(1)
            except queue.Empty:
                pass
        input_thread.join()

    def input_provider(self, func, iterable, filter_func, stopevent, chunksize):
        chunk = list()
        for args in iterable:
            chunk.append(args)
            if len(chunk) == chunksize:
                self.inq.put((func, chunk, filter_func))
                chunk = list()
        if chunk:
            self.inq.put((func, chunk, filter_func))
        stopevent.set()

if __name__ == "__main__":
    # Test code
    def foo(x):
        return (x, x)

    def foo_filter(x):
        return x[0]%2

    pool = Pool(10)
    output = pool.map(foo, ((i,) for i in range(100000)), use_tqdm=True)
    assert isinstance(output, type(i for i in range(1))), type(output)
    output_list = set(output)
    assert output_list == {(i,i) for i in range(100000)}, output_list

    output2 = pool.map(foo, ((i,) for i in range(100)), use_tqdm=True)
    assert isinstance(output2, type(i for i in range(1))), type(output2)
    output_list2 = set(output2)
    assert output_list2 == {(i,i) for i in range(100)}, output_list

    with Pool(100) as pool2:
        output3 = pool2.map(foo, ((i,) for i in range(100)), use_tqdm=True)
        assert isinstance(output3, type(i for i in range(1))), type(output3)
        output_list3 = set(output3)
        assert output_list3 == {(i,i) for i in range(100)}, output_list3

    with Pool(2) as pool3:
        output4 = pool3.map(foo, ((i,) for i in range(100)))
        assert isinstance(output4, type(i for i in range(1))), type(output4)
        output_list4 = set(filter(lambda i: i[0]%2, output4))
        assert output_list4 == {(i,i) for i in range(100) if i%2}, output_list4

    with Pool(2) as pool4:
        output5 = pool4.map(
            foo, ((i,) for i in range(100)),
            filter_func=foo_filter, use_tqdm=True)
        assert isinstance(output5, type(i for i in range(1))), type(output5)
        output_list5 = set(output5)
        assert output_list5 == {(i,i) for i in range(100) if i%2}, output_list5

    with Pool(2) as pool5:
        output6 = pool5.map(
            foo, ((i,) for i in range(100)),
            filter_func=foo_filter, chunksize=10, use_tqdm=True)
        assert isinstance(output6, type(i for i in range(1))), type(output6)
        output_list6 = set(output6)
        assert output_list6 == {(i,i) for i in range(100) if i%2}, output_list6

