"""
A library for functional testing of the server API
"""

import unittest
import httplib
import sys
import os
import json


class RestTestCase(unittest.TestCase):
    """
    Superclass for our functional tests. Defines the boilerplate for the tests
    """

    SUCCESS =              1     # : a success
    ERR_BAD_CREDENTIALS = -1     # : (for login only) cannot find the user/password pair in the database
    ERR_USER_EXISTS     = -2     #: (for add only) trying to add a user that already exists
    ERR_BAD_USERNAME    = -3     #: (for add, or login) invalid user name (only empty string is invalid for now)
    ERR_BAD_PASSWORD    = -4
    
    # Lookup the name of the server to test
    serverToTest = "localhost:5000"
    if "TEST_SERVER" in os.environ:
        serverToTest = os.environ["TEST_SERVER"]

    def makeRequest(self, url, method="GET", data=None):
        """
        Make a request to the server.
        @param url is the relative url (no hostname)
        @param method is either "GET" or "POST"
        @param data is an optional dictionary of data to be send using JSON
        @result is a dictionary of key-value pairs
        """
        
        headers = { }
        body = ""  
        if data is not None:
            headers = { "content-type": "application/json" }
            body = json.dumps(data)

        try:
            self.conn.request(method, url, body, headers)
        except Exception, e:
            if str(e).find("Connection refused") >= 0:
                print "Cannot connect to the server "+RestTestCase.serverToTest+". You should start the server first, or pass the proper TEST_SERVER environment variable"
                sys.exit(1)
            raise

        self.conn.sock.settimeout(100.0) # Give time to the remote server to start and respond
        resp = self.conn.getresponse()
        data_string = "<unknown"
        try:
            if resp.status == 200:
                data_string = resp.read()
                # The response must be a JSON request
                # Note: Python (at least) nicely tacks UTF8 information on this,
                #   we need to tease apart the two pieces.
                self.assertTrue(resp.getheader('content-type') is not None, "content-type header must be present in the response")
                self.assertTrue(resp.getheader('content-type').find('application/json') == 0, "content-type header must be application/json")


                data = json.loads(data_string)
                return data
            else:
                self.assertEquals(200, resp.status)
        except:
            # In case of errors dump the whole response,to simplify debugging
            print "Got exception when processing response to url="+url+" method="+method+" data="+str(data)
            print "  Response status = "+str(resp.status)
            print "  Resonse headers: "
            for h, hv in resp.getheaders():
                print "    "+h+"  =  "+hv
            print "  Data string: "+data_string
            raise

        
    def setUp(self):
        self.conn = httplib.HTTPConnection(RestTestCase.serverToTest, timeout=1)
        self.makeRequest("/TESTAPI/resetFixture", method="POST")
        
    def tearDown(self):
        self.conn.close ()
    
