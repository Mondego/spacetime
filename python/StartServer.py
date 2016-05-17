#!/usr/bin/python
'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import sys
from spacetime.store_server import FrameServer

log_level = sys.argv[1] if len(sys.argv) > 1 else None
FrameServer(log_level)

