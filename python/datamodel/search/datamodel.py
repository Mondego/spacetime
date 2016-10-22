'''
Created on Oct 20, 2016

@author: Rohan Achar
'''
from __future__ import absolute_import
import logging
from pcc.subset import subset
from pcc.parameter import parameter, ParameterMode
from pcc.set import pcc_set
from pcc.attributes import dimension, primarykey
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
        self.underprocess = False
        self.isprocessed = False    
        self.raw_content = None    

@subset(Link)
class UnProcessedLink(object):
    @staticmethod
    def __predicate__(l):
        return l.isprocessed == False

@subset(UnProcessedLink)
class OneUnProcessedLink(Link.Class()):
    @staticmethod
    def __query__(upls):
        for upl in upls:
            if OneUnProcessedLink.__predicate__(upl):
                upl.underprocess = True
                return [upl]
        return []

    @staticmethod
    def __predicate__(upl):
        return not upl.underprocess 

    def __ProcessUrlData(self, raw_content):
        self.raw_content = raw_content
        return self.raw_content

    def download(self, useragentstring, timeout = 2, MaxPageSize = 1048576, MaxRetryDownloadOnFail = 5, retry_count = 0):
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
                return None
            except URLError:
                return None
            except httplib.HTTPException:
                return None
            except socket.error:
                if (retry == MaxRetryDownloadOnFail):
                    return None
                print ("Retrying " + url + " " + str(retry + 1) + " time")
                return self.download(useragentstring, timeout, MaxPageSize, MaxRetryDownloadOnFail, retry + 1)
            #except Exception as e:
            #    # Can throw unicode errors and others... don't halt the thread
            #    print(type(e).__name__ + " occurred during URL Fetching.")
        return None

@pcc_set
class Document(object):
    @primarykey(str)
    def ID(self): return self._ID

    @ID.setter
    def ID(self, value): self._ID = value
    
    @dimension(str)
    def raw_content(self): return self._rc

    @raw_content.setter
    def raw_content(self, value): self._rc = value

    @dimension(bool)
    def underprocess(self): return self._up

    @underprocess.setter
    def underprocess(self, value): self._up = value

    @dimension(bool)
    def isprocessed(self): return self._isp

    @isprocessed.setter
    def isprocessed(self, value): self._isp = value

    def __init__(self, url):
        self.url = url
        self.underprocess = False
        self.isprocessed = False        


