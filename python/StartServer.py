#!/usr/bin/python
'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import sys
from spacetime.store_server import FrameServer
import logging
log_level = sys.argv[1] if len(sys.argv) > 1 else "debug"
FrameServer(log_level)

