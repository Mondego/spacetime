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
    print ("Running Producer delete objs {0}".format(
            VERSIONBY[dataframe.version_by]))
    MAX_OBJ_COUNT = 1000
    dataframe.add_many(BaseSet, [
        BaseSet(
            i, i+1, "{0}".format(i), float(i),
            "{0}".format(i)*1000) for i in range(MAX_OBJ_COUNT)])
    print ("Completed Producer delete objs {0}".format(
            VERSIONBY[dataframe.version_by]))
    
@app(Deleter=[BaseSet])
def consumer(dataframe):
    print ("Running Consumer delete objs {0}".format(
            VERSIONBY[dataframe.version_by]))
    timing = list()
    current = start = time.time()
    i_count = 0
    while dataframe.sync() and dataframe.read_all(BaseSet):
        timing.append(1000*(time.time() - current))
        current =time.time()
        dataframe.delete_one(BaseSet, dataframe.read_one(BaseSet, i_count))
        i_count += 1
        
    json.dump(
        {"start": start, "timings": timing, "end": time.time()},
        open("benchmarks/results/baseset.delete.consumer.{0}.json".format(
            VERSIONBY[dataframe.version_by]), "w"))
    print ("Completed Consumer delete objs {0}".format(
            VERSIONBY[dataframe.version_by]))

@register
@app(Types=[BaseSet])
def delete_objs(dataframe, version_by):
    prod_app = producer(dataframe=dataframe, version_by=version_by)
    con_app = consumer(dataframe=dataframe, version_by=version_by)
    prod_app.start()
    con_app.start()


# main().start()
