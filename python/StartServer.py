#!/usr/bin/python
'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import sys
import argparse
from spacetime.store_server import FrameServer
import cmd
import shlex
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

    def do_findobjs(self, line):
        """ findobjs
        Looks for objects where a given dimension matches a given value for a
        given set.
        """
        tokens = shlex.split(line)
        if len(tokens) == 3:
            type_text = tokens[0]
            dim = tokens[1]
            val = tokens[2]
            if type_text in fs.name2class:
                tp = fs.name2class[type_text]
                if hasattr(tp, dim):
                    objs = fs.Store.get(tp)
                    for obj in objs:
                        try:
                            v = getattr(obj, dim)
                        except Exception:
                            continue
                        if str(v) == val:
                            for d in obj:
                                print "%s: %s" % (d, obj[d])
                else:
                    print "type %s does not have dimension %s" % (type_text, dim)
            else:
                print "could not find type %s" % type_text
        else:
            print "usage: findobjs <type> <dimension> <value>"

    def do_descobj(self, line):
        """ descobj <type> <id>
        Given a type and an id, prints all the dimensions and values.
        Has auto-complete.
        """
        tokens = shlex.split(line)
        if len(tokens) ==  2:
            type_text = tokens[0]
            oid = tokens[1]
            if type_text in fs.name2class:
                obj = {}
                try:
                    obj = fs.Store.get_object_state(fs.name2class[type_text], oid)
                except:
                    print "could not find object with id %s" % oid
                for dim in obj:
                    print "%s: %s" % (dim, obj[dim])
            else:
                print "could not find type %s" % type_text


    def complete_descobj(self, text, line, begidx, endidx):
        tokens = shlex.split(line)
        if len(tokens) == 1:
            completions = [t.__realname__ for t in fs.DATAMODEL_TYPES]
        elif len(tokens) == 2 and text:
            completions = [t.__realname__ for t in fs.DATAMODEL_TYPES if t.__realname__.startswith(text)]
        else:
            if tokens[1] in fs.name2class:
                if len(tokens) == 2 and not text:
                    completions = [oid for oid in fs.Store.get_ids(fs.name2class[tokens[1]])]
                elif len(tokens) == 3 and text:
                    completions = [oid for oid in fs.Store.get_ids(fs.name2class[tokens[1]]) if oid.startswith(text)]
            else:
                print "\n%s is not a valid type." % tokens[1]
        return completions

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
                print "{0:60s}{1:s}".format(t.__realname__, t.__module__)
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
    global fs
    fs.shutdown()
    sys.exit(0)

if __name__== "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=12000, help='Port where the server will listen (default: 12000)')
    parser.add_argument('-P', '--profile', action='store_true', help='Enable profiling on store server.')
    parser.add_argument('-d', '--debug', action='store_true', help='Debug on')
    parser.add_argument('-e', '--external', action='store_true', help='Make this server externally accessible')
    parser.add_argument('-w', '--watchdog', action='store_true', help='Starts the server with thes slack/github watchdog')
    parser.add_argument('-t', '--timeout', type=int, default=0, help='Timeout in seconds for the server to consider a client disconnected.')
    parser.add_argument('-c', '--clearempty', action='store_true', default=False, help='Clears the dataframes when all simulations leave.')
    args = parser.parse_args()
    global fs
    fs = FrameServer(args.port, args.debug, args.external, args.timeout, args.clearempty)
    p = Parallel(target = fs.run, args = (args.profile,))
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




