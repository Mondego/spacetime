from rtypes import pcc_set, dimension, primarykey, merge
from spacetime import Node
import uuid, time

@pcc_set
class BasicType():
    oid = primarykey(str)
    prop1 = dimension(int)
    prop2 = dimension(int)
    def __init__(self, oid):
        self.oid = oid

@merge
def merge_basictype1(original, yours, theirs):
    return yours

@merge
def merge_basictype2(original, yours, theirs):
    return theirs


def producer(df, count):
    while True:
        df.add_one(BasicType, BasicType(str(uuid.uuid4())))
        df.commit()
        time.sleep(1)

def consumer(df):
    while True:
        df.pull_await()
        print(df.read_all(BasicType))

producer = Node(
    producer, appname='producer',
    Types=[BasicType],
    resolver={BasicType: merge_basictype1})
producer.start_async(10000)

consumer = Node(
    consumer, Types=[BasicType],
    remotes={'producer': producer},
    resolver={BasicType: merge_basictype2})
consumer.start_async()
consumer.join()
producer.join()
