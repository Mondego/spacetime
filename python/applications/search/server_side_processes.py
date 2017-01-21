#!/usr/bin/python
'''
Created on Jan 17, 2016

@author: Rohan Achar
'''

import logging
import logging.handlers
import os
import sys

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))

from spacetime_local.frame import frame
from applications.search.load_balancer import LoadBalancer
from applications.search.link_validator import LinkValidator
from applications.search.downloaded_saver import DownloadedSaver

logger = None

class Simulation(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        frame_lb = frame(time_step = 1000)
        frame_lb.attach_app(LoadBalancer(frame_lb))
        frame_lv = frame(time_step = 1000)
        frame_lv.attach_app(LinkValidator(frame_lv))
        frame_ds = frame(time_step = 1000)
        frame_ds.attach_app(DownloadedSaver(frame_ds))

        frame_lb.run_async()
        frame_lv.run_async()
        frame_ds.run_async()
        frame.loop()

if __name__== "__main__":
    sim = Simulation()
