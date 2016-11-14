#!/usr/bin/python
'''
Created on Oct 21, 2016

@author: Rohan Achar
'''

import logging
import logging.handlers
import os
import sys
import argparse
import uuid

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))

from spacetime_local.frame import frame
from applications.search.crawler_frame import CrawlerFrame

logger = None

class Simulation(object):
    '''
    classdocs
    '''
    def __init__(self, name, timeout, useragent):
        '''
        Constructor
        '''
        frame_c = frame(time_step = timeout)
        frame_c.attach_app(CrawlerFrame(frame_c, name, useragent))

        frame_c.run_async()
        frame.loop()

def SetupLoggers():
    global logger
    logger = logging.getLogger()
    logging.info("testing before")
    logger.setLevel(logging.DEBUG)

    #logfile = os.path.join(os.path.dirname(__file__), "../../logs/CADIS.log")
    #flog = logging.handlers.RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=50, mode='w')
    #flog.setFormatter(logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
    #logger.addHandler(flog)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    clog = logging.StreamHandler()
    clog.addFilter(logging.Filter(name='CRAWLER'))
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    clog.setLevel(logging.DEBUG)
    logger.addHandler(clog)

if __name__== "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name', type=str, default=str(uuid.uuid4()), help='Unique name to avoid conflict with other crawlers. (Default: uuid4 str)')
    parser.add_argument('-t', '--timeout', type=int, default=1000, help='Politeness delay of the crawler.')
    parser.add_argument('-u', '--useragent', type=str, default="Mondego Spacetime Test Crawler", help='UserAgentString for the crawler')
    args = parser.parse_args()
    SetupLoggers()
    sim = Simulation(args.name, args.timeout, args.useragent)
