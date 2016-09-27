'''
Created on Aug 24, 2016

@author: arthurvaladares
'''
from __future__ import absolute_import

import logging
import logging.handlers
import os
import sys
import argparse
import importlib
import time

from multiprocessing import Event, Process
filepath = os.path.realpath(os.path.dirname(__file__))
while filepath in sys.path:
    sys.path.remove(filepath)
sys.path.append(os.path.realpath(os.path.join(filepath, "../")))

from common.instrument import SpacetimeInstruments as si
from benchmark.bframe import BenchmarkFrame

# BASELINE : pulls and pushes all objects of interest at every tick
# DIFF_PUSH : pushes only modified/new/deleted objects
# DIFF_PUSHPULL : DIFF_PUSH + pulls only modified/new/deleted objects. (DIFF_PULL ONLY WORKS WITH DIFF_PUSH, so, they need to go together)
# FULL : Current optimization state

MODES = ["BASELINE", "DIFF_PUSH", "DIFF_PUSHPULL", "FULL"]

class TestCase():
    def __init__(self, test_module, test_suite, test_name, instances, steps, testsims):
        self.test_module = test_module
        self.test_name = test_name
        self.test_suite = test_suite
        self.instances = instances
        self.steps = steps
        self.testsims = testsims
        self.test_module = test_module

class BenchSimulation(Process):
    def __init__(self, args, ev, dir_path):
        super(BenchSimulation, self).__init__()
        self.args = args
        self.event = ev
        self.dir_path = dir_path
        self.daemon = True

    def run(self):
        testcases = []
        args = self.args
        dir_path = self.dir_path
        if args.mode and args.mode in MODES:
            self.mode = args.mode
        else:
            if args.mode:
                print "# WARNING #: COULD NOT FIND MODE %s, DEFAULTING TO FULL" % args.mode
            self.mode = "FULL"

        if args.testfile:
            filename = os.path.basename(args.testfile.split('.')[0])
            with open(args.testfile) as f:
                for line in f.readlines():
                    if not line.strip().startswith('#'):
                        test_suite, test_name, instances, steps = line.split(' ')
                        module = importlib.import_module("benchmark.%s.%s" % (test_suite, test_name))
                        testcases.append(TestCase(module, test_suite, test_name, int(instances), int(steps), args.testsims))

        else:
            module = importlib.import_module("benchmark.%s" % args.test)
            testcases.append(TestCase(module, args.test.split('.')[0], args.test.split('.')[1], args.instances, args.steps, args.testsims))
            filename = ''

        for testcase in testcases:
            # Replace for argument based choice
            #import benchmark.subset.subset_01 as test_case
            reload(testcase.test_module)
            bs = importlib.import_module("benchmark.benchmark")
            reload(bs)

            if hasattr(testcase.test_module, "TIMESTEP"):
                ts = testcase.test_module.TIMESTEP
            else:
                ts = args.timestep

            framebench = BenchmarkFrame(address=args.address, time_step=ts, instrument=True, profiling=True)
            if self.mode:
                framebench.set_benchmode(self.mode)
            framebench.attach_app(bs.BenchmarkSimulation(framebench, testcase.instances, testcase.steps, testcase.test_module.initialize, testcase.test_module.update))

            test_options = "%sn %si %ss" % (testcase.test_name, testcase.instances, testcase.steps)

            filenames = [os.path.join(dir_path, "%s %s %s.csv" % (filename, "producer", test_options))]
            si.setup_instruments([framebench], filenames=filenames,
                                 options={'instances' : testcase.instances,
                                            'steps': testcase.steps,
                                            'type':'%s.%s' % (testcase.test_suite, testcase.test_name),
                                            'sims':testcase.testsims,
                                            'mode':self.mode})
            # Synchronize to start together
            self.event.set()
            framebench.run()
            # Test is over
            self.event.set()
            time.sleep(15)
        self.event.set()
            #frame.loop()

class TestSimulation(Process):
    def __init__(self, args, ev, dir_path):
        super(TestSimulation, self).__init__()
        self.args = args
        self.event = ev
        self.dir_path = dir_path
        self.daemon = True

    def run(self):
        testcases = []
        args = self.args
        dir_path = self.dir_path
        if args.mode and args.mode in MODES:
            self.mode = args.mode
        else:
            if args.mode:
                print "# WARNING #: COULD NOT FIND MODE %s, DEFAULTING TO FULL" % args.mode
            self.mode = "FULL"

        if args.testfile:
            filename = os.path.basename(args.testfile.split('.')[0])
            with open(args.testfile) as f:
                for line in f.readlines():
                    if not line.strip().startswith('#'):
                        test_suite, test_name, instances, steps = line.split(' ')
                        module = importlib.import_module("benchmark.%s.%s" % (test_suite, test_name))
                        testcases.append(TestCase(module, test_suite, test_name, int(instances), int(steps), args.testsims))

        else:
            module = importlib.import_module("benchmark.%s" % args.test)
            testcases.append(TestCase(module, args.test.split('.')[0], args.test.split('.')[1], args.instances, args.steps, args.testsims))
            filename = ''

        for testcase in testcases:
            reload(testcase.test_module)
            bt = importlib.import_module("benchmark.benchtest")
            reload(bt)

            if hasattr(testcase.test_module, "TIMESTEP"):
                ts = testcase.test_module.TIMESTEP
            else:
                ts = args.timestep

            test_options = "%sn %si %ss" % (testcase.test_name, testcase.instances, testcase.steps)
            frametest = BenchmarkFrame(address=args.address, time_step=ts, instrument=True, profiling=True)
            if self.mode:
                frametest.set_benchmode(self.mode)
            frametest.attach_app(bt.BenchmarkTestSimulation(frametest, self.event, testcase.test_module.initialize_test, testcase.test_module.update_test))
            filenames = [os.path.join(dir_path, "%s %s %s.csv" % (filename, "consumer", test_options))]
            si.setup_instruments([frametest], filenames=filenames,
                                 options={'instances' : testcase.instances,
                                            'steps': testcase.steps,
                                            'type':'%s.%s' % (testcase.test_suite, testcase.test_name),
                                            'sims':testcase.testsims,
                                            'mode':self.mode})
            # Wait for BenchSimulation to give the go
            self.event.wait()
            # Clear event, so update loop can wait for event to signal end
            self.event.clear()
            frametest.run()
            # Wait until benchmark is ready
            self.event.wait()
            #frame.loop()

def setupLoggers():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    clog = logging.StreamHandler()
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    clog.setLevel(logging.DEBUG)
    logger.addHandler(clog)

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

def get_dirpath(args):
    if args.mode and args.mode in MODES:
        mode = args.mode
    else:
        if args.mode:
            print "# WARNING #: COULD NOT FIND MODE %s, DEFAULTING TO FULL" % args.mode
        mode = "FULL"
    if not os.path.exists('stats'):
        os.mkdir('stats')
    strtime = time.strftime("%Y-%m-%d_%H-%M-%S")
    dir_path = os.path.join('stats', "%s %s" % (mode, strtime))
    os.mkdir(dir_path)
    return dir_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--instances', type=int, default=1000, help='Number of object instances to be instantiated.')
    parser.add_argument('-ts', '--testsims', type=int, default=1, help='Number of test simulations subscribing to types <UNSUPPORTED>.')
    parser.add_argument('-s', '--steps', type=int, default=100, help='Number of simulation steps.')
    parser.add_argument('-t', '--test', default='subset.subset_01', help='Name of tests to be run')
    parser.add_argument('-a', '--address', default='http://127.0.0.1:12000')
    parser.add_argument('-tf', '--testfile', help='File contanining list of tests in the form of <test_suite> <test_name> <instances> <steps> <testsims>.')
    parser.add_argument('-tsp', '--timestep', type=int, default=500, help='Time interval for each simulation step. Default is 500.')
    parser.add_argument('-m', '--mode', default=None, help='Testbench mode, current options are: <BASELINE>, <DIFF_SENT>, <DIFF_RECEIVED>, <FULL>')
    #parser.add_argument('-r', '--remote', help='Remote frame server location, in the form of <user>@<server>:/<path>.')
    setupLoggers()
    args = parser.parse_args()
    e = Event()
    dirpath = get_dirpath(args)
    sim = BenchSimulation(args, e, dirpath)
    sim2 = TestSimulation(args, e, dirpath)
    sim.start()
    sim2.start()
    sim.join()
    sim2.join()
