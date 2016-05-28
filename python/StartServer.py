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
    def do_quit(self, line):
        """ quit
        Exits all applications by calling their shutdown methods.
        """
        shutdown()

    def do_exit(self, line):
        """ exit
        Exits all applications by calling their shutdown methods.
        """
        shutdown()

    def do_objsin(self, type_text):
        """ objsin <type>
        Prints the primary key of all objects of a type (accepts auto-complete)
        """
        if type_text in fs.name2class:
            objs = fs.Store.get(fs.name2class[type_text])
            if objs:
                print "{0:20s}".format("ids")
                print "============="
                for oid in objs:
                    print "{0:20s}".format(oid)
                print ""
        else:
            print "could not find type %s" % type_text

    def complete_objsin(self, text, line, begidx, endidx):
        if not text:
            completions = [t.__realname__ for t in fs.DATAMODEL_TYPES]
        else:
            completions = [t.__realname__ for t in fs.DATAMODEL_TYPES if t.__realname__.startswith(text)]
        return completions

    def complete_list(self, text, line, begidx, endidx):
        return ['sets','apps']

    def do_list(self, line):
        """ list ['sets','apps']
        list accepts one of two arguments:
        * 'sets' prints all pcc sets tracked by the server
        * 'apps' prints the name of all applications registered with the server
        """
        if line == "sets":
            for t in fs.DATAMODEL_TYPES:
                print "{0:60s}{1:s}".format(t.__realname__, t.Class().__module__)
        elif line == "apps":
            all_apps = fs.Store.get_app_list()
            for app in all_apps:
                print app
        else:
            print line

    def do_clear(self, type_text):
        """ clear [<type>, '!all']
        Deletes all objects of the type passed.

        If '!all' is passed, all objects of all types are cleared.
        """
        if type_text:
            if type_text == "!all":
                fs.Store.clear()
                print "cleared all objects in store..."
            else:
                try:
                    fs.Store.clear(fs.name2class[type_text])
                    print "cleared all objects of type %s" % type_text
                except:
                    print "could not clear objects of type %s" % type_text


    def emptyline(self):
        pass

    def do_EOF(self, line):
        shutdown()

    # TODO: do_pause. Will require telling the applications to pause, to avoid
    # issues.

def shutdown():
    print "Shutting down ..."
    sys.exit(0)

if __name__== "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=12000, help='Port where the server will listen (default: 12000)')
    parser.add_argument('-d', '--debug', action='store_true', help='Debug on')
    parser.add_argument('-e', '--external', action='store_true', help='Make this server externally accessible')
    parser.add_argument('-w', '--watchdog', action='store_true', help='Starts the server with thes slack/github watchdog')
    args = parser.parse_args()

    global fs
    fs = FrameServer(args.port, args.debug, args.external)
    p = Parallel(target = fs.run)
    p.daemon = True
    p.start()

    if args.watchdog:
        try:
            from slack_watchdog import start_watchdog
            start_watchdog(fs)
        except:
            print "error starting watchdog."
            raise
    SpacetimeConsole().cmdloop()




