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

from datamodel.search.Robot import Robot

robot_manager = Robot()

@pcc_set
class Link(object):
    @primarykey(str)
    def url(self): return self._url

    @url.setter
    def url(self, value): self._url = value

    @dimension(bool)
    def underprocess(self): 
        try:
            return self._up
        except AttributeError:
            return False

    @underprocess.setter
    def underprocess(self, value): self._up = value

    @dimension(bool)
    def grouped(self): 
        try:
            return self._gpd
        except AttributeError:
            return False

    @grouped.setter
    def grouped(self, value): self._gpd = value

    @dimension(bool)
    def isprocessed(self): 
        try:
            return self._isp
        except AttributeError:
            return False

    @isprocessed.setter
    def isprocessed(self, value): self._isp = value

    @dimension(str)
    def raw_content(self): 
        try:
            return self._rc
        except AttributeError:
            return None

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

    @dimension(str)
    def downloaded_by(self): return self._downloaded_by

    @downloaded_by.setter
    def downloaded_by(self, value): self._downloaded_by = value

    @dimension(int)
    def http_code(self): return self._http_code

    @http_code.setter
    def http_code(self, value): self._http_code = value

    @dimension(bool)
    def valid(self): 
        try:
            return self._valid
        except AttributeError:
            return False

    @valid.setter
    def valid(self, v): self._valid = v

    @dimension(bool)
    def download_complete(self): 
        try:
            return self._dc
        except AttributeError:
            return False

    @download_complete.setter
    def download_complete(self, v):
        self._dc = v

    @property
    def full_url(self): return self.scheme + "://" + self.url

    def __ProcessUrlData(self, raw_content, useragentstr):
        self.raw_content = raw_content
        self.downloaded_by = useragentstr
        self.download_complete = True
        return self.raw_content

    def download(self, useragentstring, timeout = 2, MaxPageSize = 1048576, MaxRetryDownloadOnFail = 5, retry_count = 0):
        self.isprocessed = True
        url = self.full_url
        if self.raw_content != None:
            print ("Downloading " + url + " from cache.")
            return self.raw_content
        else:
            try:
                print ("Downloading " + url + " from source.")
            except Exception:
                pass
            try:
                urlreq = Request(url, None, {"User-Agent" : useragentstring})
                urldata = urlopen(urlreq, timeout = timeout)
                self.http_code = urldata.code
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
                    return self.__ProcessUrlData(urldata.read(), useragentstring)
            except HTTPError:
                self.http_code = 400
                return ""
            except URLError:
                self.http_code = 400
                return ""
            except httplib.HTTPException:
                self.http_code = 400
                return ""
            except socket.error:
                if (retry_count == MaxRetryDownloadOnFail):
                    self.http_code = 400
                    return ""
                try:
                    print ("Retrying " + url + " " + str(retry_count + 1) + " time")
                except Exception:
                    pass
                return self.download(useragentstring, timeout, MaxPageSize, MaxRetryDownloadOnFail, retry_count + 1)
            except Exception as e:
                # Can throw unicode errors and others... don't halt the thread
                self.http_code = 499
                print(type(e).__name__ + " occurred during URL Fetching.")
        return ""

@projection(Link, Link.url, Link.scheme, Link.domain)
class ProducedLink(object):
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
    
@subset(Link)
class NewLink(object):
    @property
    def full_url(self): return self.scheme + "://" + self.url
    
    @staticmethod
    def __predicate__(l):
        return l.valid == False

@subset(Link)
class UnProcessedLink(object):
    @staticmethod
    def __predicate__(l):
        return l.isprocessed == False and l.valid == True

@subset(Link)
class DownloadedLink(object):
    @staticmethod
    def __predicate__(l):
        return l.download_complete == True and l.valid == True

@impure
@subset(UnProcessedLink)
class DistinctDomainUnprocessedLink(object):
    @staticmethod
    def __post_process__(l):
        l.grouped = True
        return l

    @staticmethod
    def __predicate__(l): return (l.isprocessed == False and l.grouped == False and l.valid == True)

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
    def __post_process__(lg):
        lg.underprocess = True
        return lg

    @staticmethod
    def __query__(upls):
        for upl in upls:
            if OneUnProcessedGroup.__predicate__(upl):
                return [upl]
        return []

    @staticmethod
    def __predicate__(upl):
        return not upl.underprocess 

    def download(self, UserAgentString, is_valid, timeout = 2, MaxPageSize = 1048576, MaxRetryDownloadOnFail = 5, retry_count = 0):
        try:
            return [(l.full_url, 
                 l.download(
                    UserAgentString,
                    timeout,
                    MaxPageSize,
                    MaxRetryDownloadOnFail,
                    retry_count)
                 ) for l in self.link_group
                    if is_valid(l.full_url) and robot_manager.Allowed(l.full_url, UserAgentString)]
        except AttributeError:
            pass

