import logging
from datamodel.search.datamodel import Link, OneUnProcessedLink
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter
from lxml import html,etree
import re
try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"


@Producer(Link)
@GetterSetter(OneUnProcessedLink)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.frame = frame

    def initialize(self):
        self.frame.add(Link("http://www.ics.uci.edu"))

    def update(self):
        outputLinks = []
        for l in self.frame.get_new(OneUnProcessedLink):
            if not l.isprocessed:
                l.isprocessed = True
            rawData = l.download("Mondego Spacetime crawler Test")
            if not rawData:
                print ("Error downloading " + l.full_url)
                continue
            try:
                htmlParse = html.document_fromstring(rawData)
                htmlParse.make_links_absolute(l.full_url)
            except etree.ParserError:
                print("ParserError: Could not extract the links from the url")
                continue
            except etree.XMLSyntaxError:
                print("XMLError: Could not extract the links from the url")
                continue
    
            for element, attribute, link, pos in htmlParse.iterlinks():
                outputLinks.append(link)
        for l in outputLinks:
            if self.is_valid(l):
                lObj = Link(l)
                if self.frame.get(Link, lObj.url) == None:
                    self.frame.add(lObj)

    def is_valid(self, url):
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

    def shutdown(self):
        pass

def ExtractNextLinks(self, url, rawData, outputLinks):
        '''Function to extract the next links to iterate over. No need to validate the links. They get validated at the ValudUrl function when added to the frontier
        Add the output links to the outputLinks parameter (has to be a list). Return Bool signifying success of extracting the links.
        rawData for url will not be stored if this function returns False. If there are no links but the rawData is still valid and has to be saved return True
        Keep this default implementation if you need all the html links from rawData'''

        

        