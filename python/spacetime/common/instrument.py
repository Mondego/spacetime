from functools import wraps
from spacetime.client.IApplication import IApplication
from spacetime.client.IFrame import IFrame
import csv
import datetime
INSTRUMENT_HEADERS = {}
import time
import os
import re
from .util import get_os

private = re.compile(r'(_frame__)(.)*')

class ApplicationInstruments:
    def __init__(self, frame, filename=None, options=None):
        self.appname = frame.get_app().__class__.__name__
        self.frame = frame
        strtime = time.strftime("%Y-%m-%d_%H-%M-%S")
        if not os.path.exists('stats'):
            os.mkdir('stats')
        if not filename:
            self.filename = os.path.join('stats', "%s_frame_%s.csv" % (strtime, self.appname))
        else:
            self.filename = filename
        if not get_os().startswith("Windows"):  # 'Windows CYGWIN' has symlink
            linkname = os.path.join('stats', "latest_%s" % self.appname)
            if os.path.lexists(linkname):
                os.remove(linkname)
            os.symlink(os.path.abspath(self.filename), linkname) # @UndefinedVariable only in Linux!
        with open(self.filename, 'w', 0) as csvfile:
            if not options:
                csvfile.write("########\n")
                csvfile.write("Options, Interval : %s\n" % frame.get_timestep())
                csvfile.write("########\n\n")
            else:
                opt_headers = options.keys()
                csvfile.write("########\n")
                csvfile.write("Interval,%s\n" % ",".join(opt_headers))
                values = [options[h] for h in opt_headers]
                csvfile.write("%s,%s\n" % (frame.get_timestep(),",".join(map(str, values))))
                csvfile.write("########\n\n")

            # Base headers
            headers = ['time', 'update_delta']
            # Annotated headers
            if self.frame.__module__ in INSTRUMENT_HEADERS:
                headers.extend(INSTRUMENT_HEADERS[self.frame.__module__])
            if self.frame.get_app().__module__ in INSTRUMENT_HEADERS:
                headers.extend(INSTRUMENT_HEADERS[self.frame.get_app().__module__])
            if hasattr(self.frame, '_instrument_headers'):
                headers.extend(self.frame._instrument_headers)

            self.fieldnames = list(set(headers))
            writer = csv.DictWriter(csvfile, delimiter=',', restval='-1', lineterminator='\n', fieldnames=self.fieldnames)
            writer.writeheader()
        self.start_time = datetime.datetime.now()

# static class
class SpacetimeInstruments(object):
    @classmethod
    def setup_instruments(cls, frame_list, options=None, filenames=None):
        cls.instruments = {}
        if not filenames or len(filenames) != len(frame_list):
            filenames = [None] * len(frame_list)
        for frame, filename in zip(frame_list,filenames):
            inst = ApplicationInstruments(frame, filename=filename, options=options)
            cls.instruments[inst.appname] = inst

    @classmethod
    def record_instruments(cls, delta_secs, frame):
        appname = frame.get_app().__class__.__name__
        inst = cls.instruments[appname]
        with open(inst.filename, 'a', 0) as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=',', restval='-1', lineterminator='\n', fieldnames=inst.fieldnames)
            # picks up custom instruments
            d = frame._instruments
            # adds time and update delta
            d['time'] = str(datetime.datetime.now() - inst.start_time)
            d['update_delta'] = delta_secs * 1000
            writer.writerow(d)
            frame._instruments = {}

def timethis(f):
    if not f.__module__ in INSTRUMENT_HEADERS:
        INSTRUMENT_HEADERS[f.__module__] = []
    fname = f.func_name
    if private.match(f.func_name):
        fname = fname.replace('_frame','')
    INSTRUMENT_HEADERS[f.__module__].append(fname)
    @wraps(f)
    def instrument(*args, **kwds):
        obj = args[0]
        if isinstance(obj, IApplication):
            obj = obj.frame
        if not isinstance(obj, IFrame):
            raise TypeError("Instrumentation is only supported for IFrame and IApplication objects.")

        start = time.time()
        ret = f(*args, **kwds)
        end = time.time()
        fname = f.__name__
        if private.match(f.func_name):
            fname = fname.replace('_frame','')
        if not hasattr(obj, '_instruments'):
            obj._instruments = {}
        obj._instruments[fname] = (end - start) * 1000
        return ret
    return instrument
