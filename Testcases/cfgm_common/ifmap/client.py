#!/usr/bin/python
#
# Copyright 2011, Infoblox, All Rights Reserved
#
# Open Source, see LICENSE
#

from _ssl import PROTOCOL_SSLv3, PROTOCOL_SSLv23, PROTOCOL_TLSv1
import gevent
import geventhttpclient
from geventhttpclient import HTTPClient

import urllib

import base64
import cStringIO
import sys


from logging import getLogger

log = getLogger(__name__) # when imported, the logger will be named "ifmap.client"

# Import either httplib2 or urllib2 and map to same name
try:
	import httplib2 as http_client_lib
	Http = http_client_lib.Http
	HttpException = http_client_lib.HttpLib2Error
except ImportError:
	import urllib2 as http_client_lib
	HttpException = (http_client_lib.URLError, http_client_lib.HTTPError)
	class Http(): # wrapper to use when httplib2 not available
		def request(self, url, method, body, headers):
			f = http_client_lib.urlopen(http_client_lib.Request(url, body, headers))
			return f.info(), f.read()

#import urllib2 as http_client_lib
#class Http(): # wrapper to use when httplib2 not available
#	def request(self, url, method, body, headers):
#		f = http_client_lib.urlopen(http_client_lib.Request(url, body, headers))
#		return f.info(), f.read()

namespaces = {
	'env'   :   "http://www.w3.org/2003/05/soap-envelope",
	'ifmap' :   "http://www.trustedcomputinggroup.org/2010/IFMAP/2",
	'meta'  :   "http://www.trustedcomputinggroup.org/2010/IFMAP-METADATA/2"
}

# NOTE(sahid): It seems that the geventhttpclient uses gevent.queue.LifoQueue
# to maintain a pool of connections and according to the doc it is possible
# to configure the maxsize of the queue with None or a value less than 0 to
# set the number of connections ulimited otherwise It is actually not possible
# to set it to None or less than 0 since lock.BoundedSemaphore will return an
# exception. https://github.com/gwik/geventhttpclient/blob/master/src/geventhttpclient/connectionpool.py#L37
concurrency = 1 # arbitrary value since it is not possible to use ulimited.

class AsyncReadWrapper(object):
    """ Perform the socket read in a separate greenlet """
    def __init__(self, request):
        self._greenlet = gevent.spawn(self.AsyncRead, request)
	self._content = None

    def AsyncRead(self, request):
        self._content = request.read()

    def __str__(self, *args, **kwargs):
        self._greenlet.join()
        return self._content

    def __repr__(self, *args, **kwargs):
        self._greenlet.join()
        return self._content

class client:
	"""
	IF-MAP client
	"""
    
	__url = None
	__session_id = None
	__publisher_id = None
	__last_sent = None
	__last_received = None
	__namespaces = None
	__ssl_options = {
		'cert_reqs'   : gevent.ssl.CERT_NONE,
		'ssl_version' : PROTOCOL_SSLv23,
	}
	if sys.version_info >= (2,7):
		__ssl_options['ciphers'] = "RC4-SHA"

	__envelope ="""<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope" %(ns)s>
  <env:Body>
		%(body)s
  </env:Body>
</env:Envelope>
"""

	def __init__(self, url, user=None, password=None, namespaces={}, ssl_opts=None):
		if user and password:
#			self.__password_mgr=http_client_lib.HTTPPasswordMgrWithDefaultRealm()
#			self.__password_mgr.add_password(None, url, user, password)
#			handler = http_client_lib.HTTPBasicAuthHandler(self.__password_mgr)
#			opener = http_client_lib.build_opener(handler)
#			http_client_lib.install_opener(opener)

                        #pycurl.global_init(pycurl.GLOBAL_SSL)

			pass

		#if namespaces:
		self.__namespaces = namespaces
		if ssl_opts:
			self.__ssl_options.update(ssl_opts)

		self.__url = url
                self.__username = user
                self.__password = password
                try:
                    self._http = HTTPClient(*self.__url, ssl = True,
                                        connection_timeout = None,
                                        network_timeout = None,
                                        ssl_options = self.__ssl_options,
                                        insecure = True,
                                        concurrency = concurrency)
                except TypeError:
                    self._http = HTTPClient(*self.__url, ssl = True,
                                        connection_timeout = None,
                                        network_timeout = None,
                                        ssl_options = self.__ssl_options,
                                        concurrency = concurrency)


	def last_sent(self):
		return self.__last_sent

	def last_received(self):
		return self.__last_received

	def envelope(self, body) :
		_ns = ""
		for ns_prefix, ns_uri in self.__namespaces.items():
			#if ns_prefix == "env": break # don't add the envelope namespace again
			if ns_prefix == "env": continue # don't add the envelope namespace again
			_ns += "xmlns:"+ns_prefix+'="'+ns_uri+'" '
		return str(self.__envelope % {'body':body, 'ns': _ns})

	def call(self, method, body):
		xml = self.envelope(body)
		#headers={
		#  'Content-type': 'text/xml; charset="UTF-8"',
		#  'Content-length': str(len(xml)),
		#  "SOAPAction": '"%s"' % (method),
		#}

                base64string = base64.encodestring('%s:%s' % (self.__username, self.__password)).replace('\n', '')
		# pycurl
		#headers=[
		#  'Content-type: text/xml; charset="UTF-8"',
		#  'Content-length: %s' %(str(len(xml))),
                #  'Authorization : Basic %s' %(base64string),
		#  'SOAPAction: %s' % (method),
		#]

                # geventhttp
		headers={
		  'Content-type': 'text/xml; charset="UTF-8"',
		  'Content-length': '%s' %(str(len(xml))),
                  'Authorization': 'Basic %s' %(base64string),
		  'SOAPAction': '%s' % (method),
		}

		try:
				log.info("sending IF-MAP message to server")
				log.debug("========  sending IF-MAP message ========")
				log.debug("\n%s\n", xml)
				log.debug("========  /sending IF-MAP message ========")

				#response, content = self.http.request(self.__url,"POST", body=xml, headers=headers )

                                #self.http = pycurl.Curl()
                                #self.http.setopt(pycurl.URL, self.__url)
                                #self.http.setopt(pycurl.HTTPHEADER, headers)
                                #self.http.setopt(pycurl.POSTFIELDS, xml)
                                #self.http.setopt(pycurl.VERBOSE, True)
                                #self.http.setopt(pycurl.SSL_VERIFYPEER, 0)   
                                #self.http.setopt(pycurl.SSL_VERIFYHOST, 0)
                                #content = cStringIO.StringIO()
                                #self.http.setopt(pycurl.WRITEFUNCTION,
                                #                 content.write)
                                #self.http.perform()

				#self.http = HTTPClient(*self.__url, ssl = True,
				#                       ssl_options = {'cert_reqs': gevent.ssl.CERT_NONE,
				#		                      'ssl_version': PROTOCOL_SSLv3})
				#response = self.http.post('/', body = xml, headers = headers)
				response = self._http.post('/', body = xml, headers = headers)
				content = response.read()

				self.__last_sent = xml

				#self.__last_received = content
				#pycurl self.__last_received = content.getvalue()
				self.__last_received = content

				log.debug("========  received IF-MAP response ========")
				#log.debug("\n%s\n", content)
				#pycurl log.debug("\n%s\n", content.getvalue())
				log.debug("\n%s\n", content)
				log.debug("========  /receive IF-MAP response ========")

				#return content
				#pycurl return content.getvalue()
				return content

		except	HttpException, e:
				log.error("HTTP Connection error in IF-MAP client: %s", e.reason)
		except Exception as e:
				log.error("Uknown error sending IF-MAP message to server %s", str(e))
				raise

	def call_async_result(self, method, body):
		xml = self.envelope(body)
                base64string = base64.encodestring('%s:%s' % (self.__username, self.__password)).replace('\n', '')

                # geventhttp
		headers={
		  'Content-type': 'text/xml; charset="UTF-8"',
		  'Content-length': '%s' %(str(len(xml))),
                  'Authorization': 'Basic %s' %(base64string),
		  'SOAPAction': '%s' % (method),
		}

		try:
				response = self._http.post('/', body = xml, headers = headers)
				content = AsyncReadWrapper(response)

				return content

		except	HttpException, e:
				log.error("HTTP Connection error in IF-MAP client: %s", e.reason)
		except:
				log.error("Uknown error sending IF-MAP message to server")
				raise

	def set_session_id(self, session_id):
		self.__session_id = session_id

	def set_publisher_id(self, publisher_id):
		self.__publisher_id = publisher_id

	def get_session_id(self):
		return self.__session_id

	def get_publisher_id(self):
		return self.__publisher_id


if __name__ == "__main__":
	print """The ifmap client library is not meant to be run from the command line or python interpreter
- you should use it by including it in your python software. See testmap.py for an example.
Hint: add this line to use the library - 'from ifmap import ifmapClient' """
