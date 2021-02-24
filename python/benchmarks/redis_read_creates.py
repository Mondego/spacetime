import time

from benchmarks.redis.redis import RedisNode

Stop = "benchmarks.datamodel.Stop"
BasicObject = "benchmarks.datamodel.BasicObject"

def producer(df, obj_count, num_consumers):
    while len(df.read_all(Stop, ["oid", "accepted", "start"])) != num_consumers:
        df.checkout()
    for i in range(obj_count):
        df.add_one(BasicObject, i, {"oid": i, "create_ts": time.time()})
        df.commit()
    while any(s["accepted"] == "False" for s in df.read_all(Stop, ["oid", "accepted", "start"]).values()):
        df.checkout()
    #print ("Completed producer")

def consumer(df, obj_count, index):
    df.add_one(Stop, index, {"oid": index, "accepted": "False", "start": "False"})
    df.commit()
    df.push()
    obj = None
    i = 0
    record = list()
    while i < obj_count:
        #df.pull()
        read_t = time.time()
        obj = df.read_one(BasicObject, ["oid", "create_ts"], i)
        while obj:
            record.append(read_t - float(obj["create_ts"]))
            i += 1
            obj = df.read_one(BasicObject, ["oid", "create_ts"], i)
    avg = sum(record)/len(record)
    #print (f"{index}: {avg}")
    df.write_dim(Stop, index, "accepted", "True")
    df.commit()
    df.push()
    #print ("Completed consumer: ", index)
    return avg

def run_bench(obj_count, num_consumers, rn):
    producer_node = RedisNode(producer, 8000+rn)
    producer_node.start_async(obj_count, num_consumers)
    consumers = [RedisNode(consumer, 8000-((rn+1)*(i+1)), slave=8000+rn) for i in range(num_consumers)]
    #print ("Consumers created.")
    for i, con_node in enumerate(consumers):
        con_node.start_async(obj_count, i)
    #print ("Setting the event.")
    producer_node.join()
    return [con.join() for con in consumers]