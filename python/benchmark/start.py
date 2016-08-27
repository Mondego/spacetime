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
filepath = os.path.realpath(os.path.dirname(__file__))
while filepath in sys.path:
    sys.path.remove(filepath)
sys.path.append(os.path.realpath(os.path.join(filepath, "../")))

from common.instrument import SpacetimeInstruments as si
#from spacetime_local.frame import frame
from benchmark.bframe import BenchmarkFrame

class TestCase():
    def __init__(self, test_module, test_suite, test_name, instances, steps, testsims):
        self.test_module = test_module
        self.test_name = test_name
        self.test_suite = test_suite
        self.instances = instances
        self.steps = steps
        self.testsims = testsims
        self.test_module = test_module

class Simulation(object):
    def __init__(self, args):
        testcases = []
        if not os.path.exists('stats'):
            os.mkdir('stats')
        strtime = time.strftime("%Y-%m-%d_%H-%M-%S")
        dir_path = os.path.join('stats', strtime)
        os.mkdir(dir_path)

        if args.testfile:
            filename = os.path.basename(args.testfile.split('.')[0])
            with open(args.testfile) as f:
                for line in f.readlines():
                    if not line.strip().startswith('#'):
                        test_suite, test_name, instances, steps, testsims = line.split(' ')
                        module = importlib.import_module("benchmark.%s.%s" % (test_suite, test_name))
                        testcases.append(TestCase(module, test_suite, test_name, int(instances), int(steps), int(testsims)))

        else:
            module = importlib.import_module("benchmark.%s" % args.test)
            testcases.append(TestCase(module, args.test.split('.')[0], args.test.split('.')[1], args.instances, args.steps, args.testsims))
            filename = ''

        for testcase in testcases:
            # Replace for argument based choice
            #import benchmark.subset.subset_01 as test_case

            reload(testcase.test_module)
            bs = importlib.import_module("benchmark.benchmark")
            bt = importlib.import_module("benchmark.benchtest")
            reload(bs)
            reload(bt)

            framebench = BenchmarkFrame(address=args.address, time_step=500, instrument=True, profiling=True)
            framebench.set_benchmode("BASELINE")
            framebench.attach_app(bs.BenchmarkSimulation(framebench, testcase.instances, testcase.steps, testcase.test_module.initialize, testcase.test_module.update))

            test_options = "%sn %si %ss" % (testcase.test_name, testcase.instances, testcase.steps)

            frametest = BenchmarkFrame(address=args.address, time_step=500, instrument=True, profiling=True)
            frametest.set_benchmode("BASELINE")
            frametest.attach_app(bt.BenchmarkTestSimulation(frametest, framebench, testcase.test_module.initialize_test, testcase.test_module.update_test))
            filenames = [os.path.join(dir_path, "%s %s %s.csv" % (filename, "benchmark", test_options)),
                         os.path.join(dir_path, "%s %s %s.csv" % (filename, "test", test_options))]
            si.setup_instruments([framebench, frametest], filenames=filenames, options={'instances' : testcase.instances, 'steps': testcase.steps, 'type':'%s.%s' % (testcase.test_suite, testcase.test_name), 'sims':testcase.testsims})

            frametest.run_async()
            framebench.run()

            time.sleep(3)
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--instances', type=int, default=1000, help='Number of object instances to be instantiated.')
    parser.add_argument('-ts', '--testsims', type=int, default=1, help='Number of test simulations subscribing to types.')
    parser.add_argument('-s', '--steps', type=int, default=100, help='Number of simulation steps.')
    parser.add_argument('-t', '--test', default='subset.subset_01', help='Name of tests to be run')
    parser.add_argument('-a', '--address', default='http://127.0.0.1:12000')
    parser.add_argument('-tf', '--testfile', help='File contanining list of tests in the form of <test_suite> <test_name> <instances> <steps> <testsims>.')
    #parser.add_argument('-r', '--remote', help='Remote frame server location, in the form of <user>@<server>:/<path>.')
    setupLoggers()
    args = parser.parse_args()
    sim = Simulation(args)