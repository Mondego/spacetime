'''
Created on Oct 20, 2016

@author: Rohan Achar
'''
from __future__ import absolute_import
import logging
from pcc.subset import subset
from pcc.parameter import parameter, ParameterMode
from pcc.set import pcc_set
from pcc.projection import projection
from pcc.attributes import dimension, primarykey
from pcc.impure import impure
import socket, base64
try:
    from urllib2 import Request, urlopen, HTTPError, URLError
    from urlparse import urlparse, parse_qs
    import httplib
except ImportError:
    from urllib.request import Request, urlopen, HTTPError, URLError
    from urllib.parse import urlparse, parse_qs
    from http import client as httplib

@pcc_set
class Link(object):
    @primarykey(str)
    def url(self): return self._url

    @url.setter
    def url(self, value): self._url = value

    @dimension(bool)
    def underprocess(self): return self._up

    @underprocess.setter
    def underprocess(self, value): self._up = value

    @dimension(bool)
    def isprocessed(self): return self._isp

    @isprocessed.setter
    def isprocessed(self, value): self._isp = value

    @dimension(str)
    def raw_content(self): return self._rc

    @raw_content.setter
    def raw_content(self, value): self._rc = value

    @dimension(str)
    def scheme(self): return self._scheme

    @scheme.setter
    def scheme(self, value): self._scheme = value

    @dimension(str)
    def domain(self): return self._domain

    @domain.setter
    def domain(self, value): self._domain = value

    @property
    def full_url(self): return self.scheme + "://" + self.url

    def __init__(self, url):
        pd = urlparse(url)
        if pd.path:
            path = pd.path[:-1] if pd.path[-1] == "/" else pd.path
        else:
            path = ""
        self.url = pd.netloc + path + (("?" + pd.query) if pd.query else "")
        self.scheme = pd.scheme
        self.domain = pd.hostname
        self.underprocess = False
        self.isprocessed = False    
        self.raw_content = None    

    def __ProcessUrlData(self, raw_content):
        self.raw_content = raw_content
        return self.raw_content

    def download(self, useragentstring, timeout = 2, MaxPageSize = 1048576, MaxRetryDownloadOnFail = 5, retry_count = 0):
        self.isprocessed = True
        url = self.full_url
        if self.raw_content != None:
            print ("Downloading " + url + " from cache.")
            return self.raw_content
        else:
            print ("Downloading " + url + " from source.")
            try:
                urlreq = Request(url, None, {"User-Agent" : useragentstring})
                urldata = urlopen(urlreq, timeout = timeout)
                try:
                    size = int(urldata.info().getheaders("Content-Length")[0])
                except AttributeError:
                    failobj = None
                    sizestr = urldata.info().get("Content-Length", failobj)
                    if sizestr:
                        size = int(sizestr)
                    else:
                        size = -1
                except IndexError:
                    size = -1

                if size < MaxPageSize and urldata.code > 199 and urldata.code < 300:
                    return self.__ProcessUrlData(urldata.read())
            except HTTPError:
                return ""
            except URLError:
                return ""
            except httplib.HTTPException:
                return ""
            except socket.error:
                if (retry_count == MaxRetryDownloadOnFail):
                    return ""
                print ("Retrying " + url + " " + str(retry_count + 1) + " time")
                return self.download(useragentstring, timeout, MaxPageSize, MaxRetryDownloadOnFail, retry_count + 1)
            #except Exception as e:
            #    # Can throw unicode errors and others... don't halt the thread
            #    print(type(e).__name__ + " occurred during URL Fetching.")
        return ""

@projection(Link, Link.url, Link.scheme)
class JustLink(object):
    @property
    def full_url(self): return self.scheme + "://" + self.url
    
@subset(Link)
class UnProcessedLink(Link):
    @staticmethod
    def __predicate__(l):
        return l.isprocessed == False

@impure
@subset(UnProcessedLink)
class DistinctDomainUnprocessedLink(Link):
    @staticmethod
    def __predicate__(l): return l.isprocessed == False

    @property
    def __distinct__(self): return self.domain

    __limit__ = 5


@pcc_set
class DownloadLinkGroup(object):
    @primarykey(str)
    def ID(self): return self._id

    @ID.setter
    def ID(self, v): self._id = v

    @dimension(list)
    def link_group(self): return self._lg

    @link_group.setter
    def link_group(self, v): self._lg = v

    @dimension(bool)
    def underprocess(self): return self._up

    @underprocess.setter
    def underprocess(self, v): self._up = v

    def __init__(self, links):
        self.ID = None
        self.link_group = links
        self.underprocess = False

@impure
@subset(DownloadLinkGroup)
class OneUnProcessedGroup(object):
    @staticmethod
    def __query__(upls):
        for upl in upls:
            if OneUnProcessedGroup.__predicate__(upl):
                upl.underprocess = True
                return [upl]
        return []

    @staticmethod
    def __predicate__(upl):
        return not upl.underprocess 

    def download(self, UserAgentString, timeout = 2, MaxPageSize = 1048576, MaxRetryDownloadOnFail = 5, retry_count = 0):
        return [(l.full_url, 
                 l.download(
                    UserAgentString,
                    timeout,
                    MaxPageSize,
                    MaxRetryDownloadOnFail,
                    retry_count)
                 ) for l in self.link_group]
