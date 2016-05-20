#!/usr/bin/python
'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import sys
import argparse
from spacetime.store_server import FrameServer
import cmd
from flask import request
from threading import Thread as Parallel

class SpacetimeConsole(cmd.Cmd):
    prompt = 'Spacetime> '

    """Command console interpreter for frame."""
    def do_exit(self, line):
        """ exit
        Exits all applications by calling their shutdown methods.
        """
        shutdown()

    def emptyline(self):
        pass

    def do_EOF(self, line):
        shutdown()

def shutdown():
    print "Shutting down ..."
    sys.exit(0)

if __name__== "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=12000, help='Port where the server will listen (default: 12000)')
    parser.add_argument('-d', '--debug', action='store_true', help='Debug on')
    parser.add_argument('-e', '--external', action='store_true', help='Make this server externally accessible')
    args = parser.parse_args()

    global fs 
    fs = FrameServer(args.port, args.debug, args.external)
    p = Parallel(target = fs.run)
    p.daemon = True
    p.start()
    
    SpacetimeConsole().cmdloop()




