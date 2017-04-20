"""
Elias Wood (owns13927@yahoo.com)
2015-04-06
Jython (Java) HTTPAdapter for Python's Requests module for skipping SSL
verification in Java.
This doesn't completely implement Requests' HTTPAdapter send method -
not all arguments are used.  This is just a quick fix for development.
"""
try:
    import java.net.Proxy as Proxy # @UnresolvedImport
    import java.net.InetSocketAddress as InetSocketAddress # @UnresolvedImport
    import java.net.URL as URL # @UnresolvedImport
    import java.net.ConnectException # @UnresolvedImport
except: pass

from requests.adapters import HTTPAdapter
from requests.utils import select_proxy,get_encoding_from_headers
from requests.exceptions import ConnectionError
from requests.models import Response
from requests.packages.urllib3.response import DeflateDecoder,GzipDecoder

class MyJavaHTTPAdapter(HTTPAdapter):
    """
    A HTTP Adapter that makes the request using java libraries.  This
    is so it uses the socket factory setup in MyJavaHTTPAdapter.ignoreJavaSSL()
    """
    # I'm assuming the proxy doesn't change
    _my_proxy_obj = None
    #def __init__(self,*args,**keys):
    #    super(MyJavaHTTPAdapter,self).__init__(*args,**keys)
    #def close(self,*args,**keys):
    #    super(MyHTTPAdapter,self).close(*args,**keys)
    #    self._my_proxy_obj = None

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        """Sends PreparedRequest object. Returns Response object.

        :param request: The :class:`PreparedRequest <PreparedRequest>` being sent.
        :param stream: (optional) Whether to stream the request content.
        :param timeout: (optional) How long to wait for the server to send
            data before giving up, as a float, or a :ref:`(connect timeout,
            read timeout) <timeouts>` tuple.
        :type timeout: float or tuple
        :param verify: (optional) Whether to verify SSL certificates.
        :param cert: (optional) Any user-provided SSL certificate to be trusted.
        :param proxies: (optional) The proxies dictionary to apply to the request.
        """

        # setup proxy object
        proxy = select_proxy(request.url,proxies)
        if proxy:
            if not self._my_proxy_obj:
                proxy = proxy[proxy.find('//')+2:]
                host,port = proxy.split(':')
                self._my_proxy_obj = Proxy(Proxy.Type.HTTP,InetSocketAddress(str(host),int(port)))
        else:
            self._my_proxy_obj = None

        # build the request
        #print 'request.method',request.method -- ignored
        #print 'request.body',request.body -- ignored
        #url = self.request_url(request, proxies) -- ignored - just the params

        u = URL(request.url)
        conn = u.openConnection(self._my_proxy_obj) if self._my_proxy_obj else u.openConnection()
        conn.setAllowUserInteraction(False)
        conn.setDoInput(True);

        self.add_headers(request)
        for k,v in request.headers.iteritems():
            conn.addRequestProperty(k,v)

        # make the request!
        try: conn.connect()
        except java.net.ConnectException, e:
            raise ConnectionError(e.getMessage())

        # start building the request
        response = Response()
        #response.status_code = getattr(resp, 'status', None)
        response.status_code = conn.getResponseCode()
        #response.headers = CaseInsensitiveDict(getattr(resp, 'headers', {}))
        i = 1 # 0 -->[None] = u'HTTP/1.1 200 OK'
        k = conn.getHeaderFieldKey(i)
        while k != None:
            response.headers[k] = conn.getHeaderField(i)
            i += 1
            k = conn.getHeaderFieldKey(i)
        # encoding
        response.encoding = get_encoding_from_headers(response.headers)
        #response.url = req.url.decode('utf-8') if isinstance(req.url, bytes) else req.url
        response.url = conn.getURL()
        #response.raw = resp
        response.raw = MyJavaInputStreamReader(conn.getInputStream()
            if 200<=conn.getResponseCode() and conn.getResponseCode()<300
            else conn.getErrorStream(),conn.getContentEncoding())
        #response.reason = response.raw.reason
        response.reason = conn.getHeaderField(0).split(' ',2)[2] #u'HTTP/1.1 200 OK'
        return response

    #===========================================================================
    # add ignore SSL
    #===========================================================================
    @staticmethod
    def ignoreJavaSSL():
        """
        Creates a dummy socket factory that doesn't verify connections.
            HttpsURLConnection.setDefaultSSLSocketFactory(...)
        This code was taken from multiple sources.
        Only makes since in jython (java).  otherwise, just use verify=False!
        """
        import sys
        if not 'java' in sys.platform:
            raise RuntimeError('only use if platform (sys.platform) is java!')
        else:
            #===================================================================
            # set default SSL socket to ignore verification
            #===================================================================
            import javax.net.ssl.X509TrustManager as X509TrustManager # @UnresolvedImport
            class MyTrustManager(X509TrustManager):
                def getAcceptedIssuers(self,*args,**keys):
                    return None
                def checkServerTrusted(self,*args,**keys):
                    pass
                def checkClientTrusted(self,*args,**keys):
                    pass

            import com.sun.net.ssl.internal.ssl.Provider # @UnresolvedImport
            from java.security import Security # @UnresolvedImport

            Security.addProvider(com.sun.net.ssl.internal.ssl.Provider())
            trustAllCerts = [MyTrustManager()]

            import javax.net.ssl.SSLContext as SSLContext # @UnresolvedImport
            sc = SSLContext.getInstance("SSL");

            import java.security.SecureRandom as SecureRandom # @UnresolvedImport
            sc.init(None, trustAllCerts,SecureRandom())

            import javax.net.ssl.HttpsURLConnection as HttpsURLConnection # @UnresolvedImport
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory())
            #===================================================================
            # Do a test!
            #===================================================================
            '''
            # setup proxy
            import java.net.Proxy as Proxy
            import java.net.InetSocketAddress as InetSocketAddress
            p = Proxy(Proxy.Type.HTTP,InetSocketAddress("host",port))

            import java.net.URL as URL
            u = URL("https://www.google.com/")
            conn = u.openConnection(p)
            print 'server response: %r',conn.getResponseCode()
            '''
            #===================================================================
            # ignore requests's error logging - this is for dev
            #===================================================================
            try:
                import requests.packages.urllib3 as urllib3
                urllib3.disable_warnings()
            except: pass

            return 'SSL verification in Java is disabled!'

#===============================================================================
# ignoreJavaSSL outside class
#===============================================================================
def ignoreJavaSSL():
    MyJavaHTTPAdapter.ignoreJavaSSL()

#===============================================================================
# Basic class to read the java input stream
#===============================================================================
class MyJavaInputStreamReader(object):
    __is = None
    _decoder = None
    def __init__(self,input_stream,encoding='gzip'):
        super(MyJavaInputStreamReader,self).__init__()
        self.__is = input_stream
        #self._decoder = requests.packages.urllib3.response._get_decoder(encoding.lower()) # @UndefinedVariable
        if encoding is None:
            self._decoder = None # no decoder!
        elif encoding.lower() == 'gzip':
            self._decoder = GzipDecoder()
        elif encoding.lower() == 'deflate':
            self._decoder = DeflateDecoder()

    def read(self,size=None):
        # content - #for chunk in .raw.read(chunk_size): str
        #.close(): None
        #.release_conn(): None
        buff = []
        if True or size is None: # checking is hard... all at once!
            b = self.__is.read()
            while b != -1:
                buff.append(b)
                b = self.__is.read()
            flush = True
        else:
            # doesn't check properly... no idea how... rip
            for i in xrange(size): # @UnusedVariable
                b = self.__is.read()
                if b == -1: break
                else: buff.append(b)
            flush = len(buff) < size

        if self._decoder:
            if flush:
                # all done - close up!
                s = self._decoder.decompress(buff) if buff else ''
                s += self._decoder.decompress(b'')
                return s + self._decoder.flush()
            else: return self._decoder.decompress(buff)
        else: return ''.join(chr(i) for i in buff)
        '''
        decoder = requests.packages.urllib3.response._get_decoder(conn.getContentEncoding())
        print 'reading....',conn.getContentEncoding(),conn.getContentType()
        br = conn.getInputStream() #BufferedReader(InputStreamReader(conn.getInputStream()))
        c = br.read()
        buff = []
        content = ""
        while c != -1:
            buff.append(c)
            c = br.read()
        br.close()
        content = decoder.decompress(buff)
        content += decoder.decompress(b'')
        content += decoder.flush()'''
    #===========================================================================
    # used in response to to close the connection after reading
    #===========================================================================
    def close(self):
        if self.__is:
            self.__is.close()
            self.__is = None
        print 'closed stream'
    def close_connection(self):
        self.close()


#===============================================================================
# Test - without MyJavaHTTPAdapter
#===============================================================================
def test_pure_java_request():
    """
    Test making a get request without using Python's requests module
    you'll need to set the proxy host and port if needed
    returns the status code. May raise errors.
    """
    ignoreJavaSSL() # will raise error if not 'java'
    # setup proxy
    proxyHost = ""
    proxyPort = 0
    if proxyHost:
        #import java.net.Proxy as Proxy # @UnresolvedImport
        #import java.net.InetSocketAddress as InetSocketAddress # @UnresolvedImport
        p = Proxy(Proxy.Type.HTTP,InetSocketAddress(proxyHost,proxyPort))
    else: p = None

    #import java.net.URL as URL # @UnresolvedImport
    u = URL("https://www.google.com/")
    conn = u.openConnection(p) if p else u.openConnection()
    return 'server response: '+repr(conn.getResponseCode())

#===============================================================================
# Test - with MyJavaHTTPAdapter
#===============================================================================
def test_java_requests_http_adapter():
    """
    Test making a get request using Python's requests module.
    you'll need to set the proxy if needed
    returns the status code. May raise errors.
    """
    ignoreJavaSSL() # will raise error if not 'java'

    # setup proxy
    proxies = dict()

    # make a dummy request
    import requests
    s = requests.Session()
    s.mount('https://',MyJavaHTTPAdapter())
    s.proxies=proxies
    s.verify=False # is ignored
    r = s.get('https://www.google.com')
    c = r.content # @UnusedVariable
    s.close()
    return 'get request successful! '+repr(r)

#===============================================================================
# main - Test
#===============================================================================
if __name__ == '__main__':
    print 'running test_pure_java_request...'
    print test_pure_java_request()
    print 'running test_java_requests_http_adapter...'
    print test_java_requests_http_adapter()

