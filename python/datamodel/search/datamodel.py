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
from pcc.attributes import dimension, primarykey, count
from pcc.impure import impure
import socket, base64, requests
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

class UrlResponse(object):
    def __init__(self, dataframe_obj, url, content, error_message, http_code, headers, is_redirected, final_url = None):
        self.dataframe_obj = dataframe_obj # do not change
        
        self.url = url
        self.content = content
        self.error_message = error_message
        self.headers = headers
        self.http_code = http_code
        self.is_redirected = is_redirected
        self.final_url = final_url
        
        # Things that have to be set later by crawlers
        self.bad_url = False
        self.out_links = set()

@pcc_set
class Link(object):
    @primarykey(str)
    def url(self): return self._url

    @url.setter
    def url(self, value): self._url = value

    @dimension(int)
    def underprocess(self): 
        try:
            return self._up
        except AttributeError:
            return 0

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

    @dimension(list)
    def first_detected_by(self): return self._fdb

    @first_detected_by.setter
    def first_detected_by(self, value): self._fdb = value

    @dimension(str)
    def http_code(self): return self._http_code

    @http_code.setter
    def http_code(self, value): self._http_code = str(value)

    @dimension(str)
    def error_reason(self): return self._error_reason

    @error_reason.setter
    def error_reason(self, value): self._error_reason = str(value)

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

    @dimension(list)
    def bad_url(self):
        try:
            return self._bu
        except AttributeError:
            return list()

    @bad_url.setter
    def bad_url(self, v):
        self._bu = v

    @dimension(dict)
    def http_headers(self):
        try:
            return self._http_headers
        except AttributeError:
            return dict()

    @http_headers.setter
    def http_headers(self, v):
        self._http_headers = dict(v)

    @dimension(bool)
    def is_redirected(self):
        try:
            return self._is_redirect
        except AttributeError:
            return False

    @is_redirected.setter
    def is_redirected(self, v):
        self._is_redirect = v

    @dimension(str)
    def final_url(self):
        try:
            return self._final_url
        except AttributeError:
            return ""

    @final_url.setter
    def final_url(self, v):
        self._final_url = v

    @dimension(list)
    def marked_invalid_by(self):
        try:
            return self._mib
        except AttributeError:
            return list()

    @marked_invalid_by.setter
    def marked_invalid_by(self, v):
        self._mib = v

    @property
    def full_url(self): return self.scheme + "://" + self.url

    def __ProcessUrlData(self, raw_content, useragentstr):
        self.raw_content = raw_content
        self.download_complete = True
        return UrlResponse(self, self.full_url, self.raw_content, "", self.http_code, self.http_headers, self.is_redirected, self.final_url), True

    def download(self, useragentstring, timeout = 2, MaxPageSize = 1048576, MaxRetryDownloadOnFail = 5, retry_count = 0):
        self.isprocessed = True
        self.downloaded_by = useragentstring
        url = self.full_url
        if self.raw_content != None:
            print ("Downloading " + url + " from cache.")
            return UrlResponse(self, url, self.raw_content, "", self.http_headers, self.is_redirected, self.final_url), True
        else:
            try:
                print ("Downloading " + url + " from source.")
            except Exception:
                pass
            try:
                urlresp = requests.get(url,
                                       timeout = timeout, 
                                       headers = {"user-agent" : useragentstring})
                
                self.http_code = urlresp.status_code
                self.is_redirected = len(urlresp.history) > 0
                self.final_url = urlresp.url if self.is_redirected else None
                urlresp.raise_for_status()
                self.http_headers = dict(urlresp.headers)
                try:
                    size = int(urlresp.headers.get("Content-Length"))
                except TypeError:
                    size = -1
                except AttributeError:
                    size = -1
                except IndexError:
                    size = -1
                try:
                    content_type = urlresp.headers.get("Content-Type")
                    mime = content_type.strip().split(";")[0].strip().lower()
                    if mime not in [ "text/plain", "text/html", "application/xml" ]:
                        self.error_reason = "Mime does not match"
                        return UrlResponse(self, url, "", self.error_reason, self.http_code, self.http_headers, self.is_redirected, self.final_url), False
                except Exception:
                    pass
                if size < MaxPageSize and urlresp.status_code > 199 and urlresp.status_code < 300:
                    return self.__ProcessUrlData(urlresp.text.encode("utf-8"), useragentstring)
                elif size >= MaxPageSize:
                    self.error_reason = "Size too large."
                    return UrlResponse(self, url, "", self.error_reason, self.http_code, self.http_headers, self.is_redirected, self.final_url), False

            except requests.HTTPError, e:
                self.http_code = 400
                self.error_reason = str(urlresp.reason)
                return UrlResponse(self, url, "", self.error_reason, self.http_code, self.http_headers, self.is_redirected, self.final_url), False
            except socket.error:
                if (retry_count == MaxRetryDownloadOnFail):
                    self.http_code = 400
                    self.error_reason = "Socket error. Retries failed."
                    return UrlResponse(self, url, "", self.error_reason, self.http_code, self.http_headers, self.is_redirected, self.final_url), False
                try:
                    print ("Retrying " + url + " " + str(retry_count + 1) + " time")
                except Exception:
                    pass
                return self.download(useragentstring, timeout, MaxPageSize, MaxRetryDownloadOnFail, retry_count + 1)
            except requests.ConnectionError, e:
                self.http_code = 499
                self.error_reason = str(e.message)
            except requests.RequestException, e:
                self.http_code = 499
                self.error_reason = str(e.message)
            #except Exception, e:
            #    # Can throw unicode errors and others... don't halt the thread
            #    self.error_reason = "Unknown error: " + str(e.message)
            #    self.http_code = 499
            #    print(type(e).__name__ + " occurred during URL Fetching.")
        return UrlResponse(self, url, "", self.error_reason, self.http_code, self.http_headers, self.is_redirected, self.final_url), False

@subset(Link)
class LinkMarkedBad(object):
    @staticmethod
    def __predicate__(l):
        return len(l.bad_url) > 0

@projection(Link, Link.url, Link.scheme, Link.domain, Link.first_detected_by)
class ProducedLink(object):
    @property
    def full_url(self): return self.scheme + "://" + self.url
    
    def __init__(self, url, first_detected_by):
        pd = urlparse(url)
        if pd.path:
            path = pd.path[:-1] if pd.path[-1] == "/" else pd.path
        else:
            path = ""
        self.url = pd.netloc + path + (("?" + pd.query) if pd.query else "")
        self.scheme = pd.scheme
        self.domain = pd.hostname
        self.first_detected_by = first_detected_by
    
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
        #return True

@subset(Link)
class AllLink(object):
    @staticmethod
    def __predicate__(l): return True

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
        self.underprocess = 0

@impure
@subset(DownloadLinkGroup)
class OneUnProcessedGroup(object):
    @staticmethod
    def __post_process__(lg):
        lg.underprocess += 1
        #print "count: ", lg.ID, lg.underprocess
        #for l in lg.link_group:
        #  l.underprocess += 1
        return lg

    #@staticmethod
    #def __query__(upls):
    #    for upl in upls:
    #        if OneUnProcessedGroup.__predicate__(upl):
    #            return [upl]
    #    return []

    #@staticmethod
    #def __orderby__(l): return l.underprocess

    #__limit__ = 1
    
    @staticmethod
    def __query__(upls):
        for upl in upls:
            if upl.underprocess == 0:
                return [upl]
        for upl in upls:
            if upl.underprocess == 1:
                return [upl]
        for item in sorted([upl for upl in upls if OneUnProcessedGroup.__predicate__(upl)], key = lambda x: x.underprocess):
            return [item]
        return list()

    @staticmethod
    def __predicate__(upl):
        return upl.underprocess <= 10000

    def download(self, UserAgentString, is_valid, timeout = 2, MaxPageSize = 1048576, MaxRetryDownloadOnFail = 5, retry_count = 0):
        try:
            success_urls = list()
            result = list()
            for l in self.link_group:
                if is_valid(l.full_url):
                    if robot_manager.Allowed(l.full_url, UserAgentString):
                        content, success = l.download(
                            UserAgentString,
                            timeout,
                            MaxPageSize,
                            MaxRetryDownloadOnFail,
                            retry_count)
                        if success:
                            success_urls.append(l.full_url)
                        result.append(content)
                    else:
                        l.marked_invalid_by += ["Robot Rule"]
                else:
                    if UserAgentString not in set(l.marked_invalid_by):
                        l.marked_invalid_by += [UserAgentString]
                    if UserAgentString not in set(l.bad_url):
                        l.bad_url += [UserAgentString]
            return result, success_urls
        except AttributeError:
            return list(), list()

@subset(Link)
class DomainCount(object):
    __groupby__ = Link.domain
    @count(Link.url)
    def link_count(self): return self._lc
    @link_count.setter
    def link_count(self, v): self._lc = v

    @staticmethod
    def __predicate__(l): return l.isprocessed == True

@subset(Link)
class BadLink(object):
    @property
    def full_url(self): return self.scheme + "://" + self.url

    @staticmethod
    def __predicate__(l):
        return len(l.bad_url) > 0

@pcc_set
class BadUrlPattern(object):
    @primarykey(str)
    def pattern(self): return self._bup

    @pattern.setter
    def pattern(self, v): self._bup = v

    def __init__(self, pattern):
        self.pattern = pattern

@pcc_set
class Release(object):
    @primarykey(str)
    def oid(self): return self._oid

    @oid.setter
    def oid(self, v): self._oid = v

