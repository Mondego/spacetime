import sys, os, json

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "../..")))

from spacetime_local.frame import frame
import logging
from datamodel.search.datamodel import DownloadedLink
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Getter
import re, chardet


logger = logging.getLogger(__name__)
LOG_HEADER = "[DOWNLOADEDSAVER]"
FINAL_FOLDER = "DOWNLOAD"

@Getter(DownloadedLink)
class DownloadedSaver(IApplication):
    def __init__(self, frame):
        self.frame = frame

    def initialize(self):
        pass

    def update(self):
        links = self.frame.get_new(DownloadedLink)
        for l in links:
            domain = l.domain
            folder = os.path.join(FINAL_FOLDER, domain)
            filename = os.path.join(folder, '_'.join(re.findall(r'[a-zA-Z0-9]+', l.url)))
            if os.path.exists(filename):
                print "File name exists"
                continue
            if not os.path.exists(folder):
                
                try:
                    os.makedirs(folder)
                except Exception, e:
                    print "Something went wrong", e.message
                    # Something went wrong trying to create the folder
                    continue
            with open(filename, "w") as wfile:
                try:
                    output = json.dumps({
                        "url": l.url,
                        "content": l.raw_content if type(l.raw_content) == str else l.raw_content.encode("utf-8"),
                        "downloaded_by": l.downloaded_by},
                                        sort_keys = True,
                                        indent = 4,
                                        separators = (",",": "))

                    wfile.write(output)
                except Exception, e1:
                    print "Encoding error maybe? ", e1.message
                    try:
                        detected_encoding = chardet.detect(l.raw_content)['encoding']
                        if detected_encoding:
                            wfile.write(json.dumps({
                                "url": l.url,
                                "content": l.raw_content if type(l.raw_content) == str else l.raw_content.encode(detected_encoding),
                                "downloaded_by": l.downloaded_by},
                                        sort_keys = True,
                                        indent = 4,
                                        separators = (",",": "))
                            )
                    except Exception, e2:
                        print "Final error ", e2.message
                        # Can't do anything more
                        continue

    def shutdown(self):
        pass

class Simulation(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        frame_c = frame(time_step = 1000)
        frame_c.attach_app(DownloadedSaver(frame_c))

        frame_c.run_async()
        frame.loop()

if __name__== "__main__":
    sim = Simulation()
