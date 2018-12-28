from rtypes import pcc_set, primarykey, dimension
from spacetime import app
from benchmarks.register import register

import time, json

@pcc_set
class BaseSet(object):
    oid = primarykey(int)
    prop1 = dimension(int)
    prop2 = dimension(str)
    prop3 = dimension(float)
    prop4 = dimension(str)

    def __init__(self, oid, p1, p2, p3, p4):
        self.oid = oid
        self.prop1 = p1
        self.prop2 = p2
        self.prop3 = p3
        self.prop4 = p4

@app(Producer=[BaseSet])
def producer(dataframe):
    MAX_OBJ_COUNT = 1000
    dataframe.add_many(BaseSet, [
        BaseSet(
            i, i+1, "{0}".format(i), float(i),
            "{0}".format(i)*1000) for i in range(MAX_OBJ_COUNT)])
    timing = list()
    current = start = time.time()
    count = 0
    while dataframe.sync() and count < 1000:
        timing.append(1000*(time.time() - current))
        current =time.time()
        for obj in dataframe.read_all(BaseSet):
            obj.prop1 += 1
        count += 1
    json.dump(
        {"start": start, "timings": timing, "end": time.time()},
        open("benchmarks/results/baseset_update.producer.json", "w"))
    print ("Producer is done")
    
@app(GetterSetter=[BaseSet])
def consumer(dataframe):
    timing = list()
    current = start = time.time()
    i_count = 0
    while dataframe.sync() and i_count < 1000:
        timing.append(1000*(time.time() - current))
        current =time.time()
        objs = dataframe.read_all(BaseSet)
        print (sum(obj.prop1 for obj in objs))
        i_count += 1
    json.dump(
        {"start": start, "timings": timing, "end": time.time()},
        open("benchmarks/results/baseset_update.consumer.json", "w"))

@register
@app(Types=[BaseSet])
def update_objs(dataframe):
    prod_app = producer(dataframe=dataframe)
    con_app = consumer(dataframe=dataframe)
    con_app.start_async()
    prod_app.start()
    print ("Producer completed")
    con_app.join()


# main().start()
