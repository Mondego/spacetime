import logging
from datamodel.search.datamodel import Link, OneUnProcessedGroup, JustLink
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re
from time import time
from Robot import Robot
try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"


@Producer(Link)
@Getter(JustLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame, app_name, useragent):
        self.app_id = app_name
        self.frame = frame
        self.robot_manager = Robot()
        self.UserAgentString = useragent

    def initialize(self):
        self.starttime = time()
        self.count = 0
        l = Link("http://www.ics.uci.edu")
        print l.full_url
        self.frame.add(l)

    def update(self):
        outputLinks = []
        for g in self.frame.get(OneUnProcessedGroup):
            rawDatas = g.download(self.UserAgentString)
            self.count += 1
            for url, rawData in rawDatas:
                if not rawData:
                    print ("Error downloading " + url)
                    continue
                try:
                    htmlParse = html.document_fromstring(rawData)
                    htmlParse.make_links_absolute(url)
                except etree.ParserError:
                    print("ParserError: Could not extract the links from the url")
                    continue
                except etree.XMLSyntaxError:
                    print("XMLError: Could not extract the links from the url")
                    continue
    
                for element, attribute, link, pos in htmlParse.iterlinks():
                    if link != url:
                        outputLinks.append(link)
            for l in outputLinks:
                if self.is_valid(l):
                    lObj = Link(l)
                    if self.frame.get(JustLink, lObj.url) == None:
                        self.frame.add(lObj)

    def is_valid(self, url):
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        if not self.robot_manager.Allowed(url, self.UserAgentString):
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
                + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                + "|thmx|mso|arff|rtf|jar|csv"\
                + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

        except TypeError:
            print ("TypeError for ", parsed)

    def shutdown(self):
        print "downloaded ", self.count, " in ", time() - self.starttime, " seconds."
        pass

def ExtractNextLinks(self, url, rawData, outputLinks):
        '''Function to extract the next links to iterate over. No need to validate the links. They get validated at the ValudUrl function when added to the frontier
        Add the output links to the outputLinks parameter (has to be a list). Return Bool signifying success of extracting the links.
        rawData for url will not be stored if this function returns False. If there are no links but the rawData is still valid and has to be saved return True
        Keep this default implementation if you need all the html links from rawData'''

        

        