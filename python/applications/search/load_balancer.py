import logging
from datamodel.search.datamodel import DownloadLinkGroup, DistinctDomainUnprocessedLink
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
LOG_HEADER = "[LOADBALANCER]"


@Producer(DownloadLinkGroup)
@GetterSetter(DistinctDomainUnprocessedLink)
class LoadBalancer(IApplication):

    def __init__(self, frame):
        self.frame = frame

    def initialize(self):
        pass

    def update(self):
        groups = self.frame.get_new(DistinctDomainUnprocessedLink)[:5]
        links = []
        if len(groups) == 0:
            return
        print len(groups)
        self.frame.add(DownloadLinkGroup(groups))

    def shutdown(self):
        pass

        