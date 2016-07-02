from functools import wraps
from spacetime_local.IApplication import IApplication
from spacetime_local.IFrame import IFrame
import platform
import csv
import datetime
INSTRUMENT_HEADERS = {}
import time
import os

class ApplicationInstruments:
    def __init__(self, frame, filename=None):
        self.appname = frame.get_app().__class__.__name__
        self.frame = frame
        strtime = time.strftime("%Y-%m-%d_%H-%M-%S")
        if not os.path.exists('stats'):
            os.mkdir('stats')
        if not filename:
            self.filename = os.path.join('stats', "%s_frame_%s.csv" % (strtime, self.appname))
        else:
            self.filename = filename
        if platform.system() != "Windows":
            linkname = os.path.join('stats', "latest_%s" % self.appname)
            if os.path.exists(linkname):
                os.remove(linkname)
            os.symlink(os.path.abspath(self.filename), linkname) # @UndefinedVariable only in Linux!
        with open(self.filename, 'w', 0) as csvfile:
            csvfile.write("########\n")
            csvfile.write("Options, Interval : %s\n" % frame.get_timestep())
            csvfile.write("########\n\n")

            # Base headers
            headers = ['time', 'update_delta']
            # Annotated headers
            if self.frame.__module__ in INSTRUMENT_HEADERS:
                headers.extend(INSTRUMENT_HEADERS[self.frame.__module__])
            if self.frame.get_app().__module__ in INSTRUMENT_HEADERS:
                headers.extend(INSTRUMENT_HEADERS[self.frame.get_app().__module__])

            self.fieldnames = headers
            writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=self.fieldnames)
            writer.writeheader()
        self.start_time = datetime.datetime.now()

# static class
class SpacetimeInstruments(object):
    @classmethod
    def setup_instruments(cls, frame_list):
        cls.instruments = {}
        for frame in frame_list:
            inst = ApplicationInstruments(frame)
            cls.instruments[inst.appname] = inst

    @classmethod
    def record_instruments(cls, delta_secs, frame):
        appname = frame.get_app().__class__.__name__
        inst = cls.instruments[appname]
        with open(inst.filename, 'a', 0) as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=inst.fieldnames)
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
    INSTRUMENT_HEADERS[f.__module__].append(f.func_name)
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
        if not hasattr(obj, '_instruments'):
            obj._instruments = {}
        obj._instruments[f.__name__] = (end - start) * 1000
        return ret
    return instrument
