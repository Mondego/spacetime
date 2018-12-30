from rtypes import pcc_set, primarykey, dimension
from spacetime import app
from benchmarks.register import register, VERSIONBY

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
    print ("Running Producer update objs {0}".format(
            VERSIONBY[dataframe.version_by]))
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
        open("benchmarks/results/baseset.update.producer.{0}.json".format(
            VERSIONBY[dataframe.version_by]), "w"))
    print ("Completed Producer update objs {0}".format(
            VERSIONBY[dataframe.version_by]))
    
@app(GetterSetter=[BaseSet])
def consumer(dataframe):
    print ("Running Consumer update objs {0}".format(
            VERSIONBY[dataframe.version_by]))
    timing = list()
    current = start = time.time()
    i_count = 0
    prev_total = 0
    while dataframe.sync() and i_count < 1000:
        timing.append(1000*(time.time() - current))
        current =time.time()
        objs = dataframe.read_all(BaseSet)
        #print (i_count, len(objs))
        total = (sum(obj.prop1 for obj in objs))
        assert (total >= prev_total)
        prev_total = total
        i_count += 1
    json.dump(
        {"start": start, "timings": timing, "end": time.time()},
        open("benchmarks/results/baseset.update.consumer.{0}.json".format(
            VERSIONBY[dataframe.version_by]), "w"))
    print ("Completed Consumer update objs {0}".format(
            VERSIONBY[dataframe.version_by]))

@register
@app(Types=[BaseSet])
def update_objs(dataframe, version_by):
    prod_app = producer(dataframe=dataframe, version_by=version_by)
    con_app = consumer(dataframe=dataframe, version_by=version_by)
    con_app.start_async()
    prod_app.start()
    con_app.join()


# main().start()
