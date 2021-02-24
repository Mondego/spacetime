from benchmarks.ycsb.mysql_new import run as run_mysql
from benchmarks.ycsb.redis_new import run as run_redis
from benchmarks.ycsb.st_new import run as run_st
from benchmarks.ycsb.mp_new import run as run_mp
from benchmarks.ycsb.mp2_new import run as run_mp2
from progressbar import ProgressBar
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--host", type=str, default="0.0.0.0")
parser.add_argument("--port", type=int, default=9000)
#parser.add_argument("--objcount", type=int, default=100)
#parser.add_argument("--nodecount", type=int, default=20)

class Config():
  def __init__(self, host, port, objcount, nodecount, run_server):
    self.host = host
    self.port = port
    self.objcount = objcount
    self.nodecount = nodecount
    self.run_server = run_server


args = parser.parse_args()
#args.run_server = False
import json
objcounts = [10, 20, 50, 100, 500, 1000]
nodecounts = [2, 10, 20, 50, 100]
s = 1
e = 10
for objcount in objcounts:
  for nodecount in nodecounts:
    if nodecount < objcount:
      continue
    #objcount = nodecount * 10
    #if objcount == 1000 and nodecount in {50, 100}:
    #    continue
    print ("Running ", objcount, nodecount)
    run_args = Config(args.host, args.port, objcount, nodecount, False)
    mysql = dict()
    redis = dict()
    st_pull = dict()
    st_await = dict()
    st_cpp_pull = dict()
    st_cpp_await = dict()
    mp = dict()
    mp2 = dict()
    # for i in ProgressBar()(range(s,e)):
    #    mysql[i] = run_mysql(run_args, i)

    # for i in ProgressBar()(range(s,e)):
    #     redis[i] = run_redis(run_args, i)

    # for i in ProgressBar()(range(s,e)):
    #     st_pull[i] = run_st(run_args, i)

    # for i in ProgressBar()(range(s,e)):
    #     st_await[i] = run_st(run_args, i, await_pull=True)

    for i in ProgressBar()(range(s,e)):
        st_cpp_pull[i] = run_st(run_args, i, cpp=True)

    for i in ProgressBar()(range(s,e)):
        st_cpp_await[i] = run_st(run_args, i, await_pull=True, cpp=True)

    for i in ProgressBar()(range(s,e)):
        mp[i] = run_mp(run_args, i)

    for i in ProgressBar()(range(s,e)):
        mp2[i] = run_mp2(run_args, i)

    json.dump({
        "mysql":mysql,
        "redis":redis,
        "spacetime":st_pull,
        "spacetime_await": st_await,
        "spacetime_cpp": st_cpp_pull,
        "spacetime_cpp_await": st_cpp_await,
        "messagepassing": mp,
        "messagepassing_pull": mp2,
    }, open(f"spacetime_local.results.{objcount}.{nodecount}.json", "w"), indent=2)

