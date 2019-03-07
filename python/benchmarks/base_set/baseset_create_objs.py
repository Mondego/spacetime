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

def producer(dataframe):
    print ("Running Producer create objs {0}".format(
            VERSIONBY[dataframe.version_by]))
    done = False
    MAX_OBJ_COUNT = 1000
    obj_count = 0
    timing = list()
    current = start = time.time()
    while dataframe.sync() and obj_count < MAX_OBJ_COUNT:
        timing.append(1000*(time.time() - current))
        current =time.time()
        obj = BaseSet(
            obj_count, obj_count+1, "{0}".format(obj_count), float(obj_count),
            "{0}".format(obj_count)*1000000)
        dataframe.add_one(BaseSet, obj)
        obj_count += 1
    json.dump(
        {"start": start, "timings": timing, "end": time.time()},
        open("benchmarks/results/baseset.create.producer.{0}.json".format(
            VERSIONBY[dataframe.version_by]), "w"))
    print ("Completed Producer create objs {0}".format(
            VERSIONBY[dataframe.version_by]))
    
def consumer(dataframe):
    print ("Running Consumer create objs {0}".format(
            VERSIONBY[dataframe.version_by]))
    timing = list()
    current = start = time.time()
    while dataframe.sync():
        timing.append(1000*(time.time() - current))
        current =time.time()
        objs = dataframe.read_all(BaseSet)
        if len(objs) == 1000:
            break
    json.dump(
        {"start": start, "timings": timing, "end": time.time()},
        open("benchmarks/results/baseset.create.consumer.{0}.json".format(
            VERSIONBY[dataframe.version_by]), "w"))
    print ("Completed Consumer create objs {0}".format(
            VERSIONBY[dataframe.version_by]))

def create_objs(dataframe, version_by):
    prod_app = Node(producer, types=[BaseSet], dataframe=dataframe, version_by=version_by)
    con_app = Node(consumer, types=[BaseSet], dataframe=dataframe, version_by=version_by)
    con_app.start_async()
    prod_app.start()
    con_app.join()

def main():
    app = Node(create_objs, types=[BaseSet], dataframe=dataframe, version_by=version_by))
    app.start()

# main().start()
