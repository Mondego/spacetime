import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re
from time import time

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"


@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame, app_name, useragent):
        self.app_id = app_name
        self.frame = frame
        self.UserAgentString = useragent

    def initialize(self):
        self.starttime = time()
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu")
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks = process_url_group(g, self.UserAgentString)
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l)
                    self.frame.add(lObj)

    def shutdown(self):
        print "downloaded ", self.count, " in ", time() - self.starttime, " seconds."
        pass

def process_url_group(group, useragentstr):
    rawDatas = group.download(useragentstr, is_valid)
    return extract_next_links(rawDatas)
    
def extract_next_links(rawDatas):
    outputLinks = []
    for url, rawData in rawDatas:
        if not rawData:
            try:
                print ("Error downloading " + url)
            except Exception:
                continue
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
    return outputLinks

def is_valid(url):
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
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
