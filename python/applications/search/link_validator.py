import sys, os

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))

from spacetime_local.frame import frame
import logging
from datamodel.search.datamodel import NewLink
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import GetterSetter, Deleter
try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[LINKVALIDATOR]"


@Deleter(NewLink)
@GetterSetter(NewLink)
class LinkValidator(IApplication):

    def __init__(self, frame):
        self.frame = frame

    def initialize(self):
        pass

    def update(self):
        new_links = self.frame.get_new(NewLink)
        for l in new_links:
            if self.is_valid(l):
                l.valid = True
            else:
                self.frame.delete(NewLink, l)

    def shutdown(self):
        pass

    def is_valid(self, l):
        parsed = urlparse(l.full_url)
        return parsed.scheme != "" and parsed.hostname != "" and ".ics.uci.edu" in parsed.hostname

class Simulation(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        frame_c = frame(time_step = 1000)
        frame_c.attach_app(LinkValidator(frame_c))

        frame_c.run_async()
        frame.loop()

if __name__== "__main__":
    sim = Simulation()
   