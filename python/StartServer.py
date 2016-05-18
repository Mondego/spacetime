#!/usr/bin/python
'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import sys
import argparse
from spacetime.store_server import FrameServer
import logging

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', type=int, default=12000, help='Port where the server will listen (default: 12000)')
parser.add_argument('-d', '--debug', action='store_true', help='Debug on')
args = parser.parse_args()

FrameServer(args.port, args.debug)

