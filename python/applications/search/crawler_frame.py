import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
import re, os
from time import time
from collections import defaultdict
from analytics import Analytics
import lxml.html
from urlparse import urljoin
import tldextract
from lxml import etree

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000
CRAWLER_TRAP_THRESHOLD = 5000

@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id =  "14644574_87723335_18368632"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 Grad 14644574, 87723335, 18368632"
		
        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        pass

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas
    
#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''
def extract_next_links(raw_data):
    analytics = Analytics()

    # Storing the output URLs on every html page
    output_links = list()

    # Getting the dictionaries for the domains, sub-domains, and path counts
    domains = analytics.get('DOMAINS')
    sub_domains = analytics.get('SUB_DOMAINS')
    paths = analytics.get('PATHS')

    # Initializing the members needed to record the MAX_OUT_LINKS and the INVALID_LINKS
    invalid_links = list()
    max_links = -1
    max_links_url = None

    for data in raw_data:
        if data.http_code < 400:
            # All processing in case of success happens here

            # Finding the paths and storing them with respect to domain, and subdomain
            parsed_url = urlparse(data.url)
            if paths[parsed_url.path] > CRAWLER_TRAP_THRESHOLD:
                invalid_links.append(data.url)
                data.bad_url = True
                data.out_links = None
                continue

            # Reading the HTML string page data and parsing it to find all URLs
            try:
                html = lxml.html.fromstring(data.content)
                links = html.xpath('//a/@href')
            except etree.XMLSyntaxError:
                print '[EXCEPTION CAUGHT]'
                print data.content
                invalid_links.append(data.url)
                data.bad_url = True
                data.out_links = None
                continue

            # Converting the relative links to absolute links
            links = [urljoin(data.url, link) for link in links]

            # Storing the links extracted from the HTML page, and the page's URL
            output_links += links

            # Finding the domain and subdomain for the URL and updating the statistics
            ext = tldextract.extract(data.url)
            sub_domains[ext.subdomain] += 1
            domains[ext.domain] += 1
            paths[parsed_url.path] += 1

            # Updating the URL with the most number of outgoing links
            if len(links) > max_links:
                max_links = len(links)
                max_links_url = data.url

            data.bad_url = False
            data.out_links = links
        else:
            invalid_links.append(data.url)
            data.bad_url = True
            data.out_links = None

    analytics.set('DOMAINS', domains)
    analytics.set('SUB_DOMAINS', sub_domains)
    analytics.set('PATHS', paths)
    analytics.merge('MAX_OUT_LINKS', (max_links, max_links_url))
    analytics.merge('INVALID_LINKS', invalid_links)
    analytics.write_to_file()
    return output_links

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''

    # Getting the record analytics as needed
    analytics = Analytics()
    paths = analytics.get('PATHS')

    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        if paths[parsed.path] > CRAWLER_TRAP_THRESHOLD:
            print '[ERROR] Invalid Crawler Trap URL found'
            return False
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)

