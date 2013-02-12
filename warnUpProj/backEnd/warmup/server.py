# Sample server for testing of the User Counter warm-up app for CS169

# Note: initial code copied from http://www.codeproject.com/Articles/462525/Simple-HTTP-Server-and-Client-in-Python.
# Will be modified extensively.

#!/usr/bin/env python

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import os
import json
import sys
import tempfile
import traceback
import re


SUCCESS               =   1  # : a success
ERR_BAD_CREDENTIALS   =  -1  # : (for login only) cannot find the user/password pair in the database
ERR_USER_EXISTS       =  -2  # : (for add only) trying to add a user that already exists
ERR_BAD_USERNAME      =  -3  # : (for add, or login) invalid user name (only empty string is invalid for now)
ERR_BAD_PASSWORD      =  -4

# We do not use a database in this sample implementation. Instead we store
# the table of users in memory, in a Python dictionary

class UserData:
    """
    If we were to use a database, this class provides the interface to a record.
    This would be an ActiveRecord for Ruby-on-Rails, or a Model class for Django
    """
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.count    = 1

class UsersModel:
    """
    This is essentially the Model in a MVC architecture. It encapsulates the data,
    along with the main invariants
    """
    MAX_USERNAME_LENGTH = 128
    MAX_PASSWORD_LENGTH = 128
    
    def __init__(self):
        # username -> UserData
        self.reset()

    # Used from constructor and self test
    def reset(self):
        # username -> UserData
        self.users = dict()

    
    # int login(string user, string password); 
    #   This function checks the user/password in the database. 
    #   On success, the function updates the count of logins in the database.
    #   On success the result is either the number of logins (including this one) (>= 1)
    #   On failure the result is an error code (< 0) from the list below
    def login(self, user, password):
        if user not in self.users:
            return ERR_BAD_CREDENTIALS

        data = self.users[user]
        if data.password != password:
            return ERR_BAD_CREDENTIALS
        data.count += 1
        return data.count;

        

#     int add(string user, string password);
#         This function checks that the user does not exists, the user name is not empty. (the password may be empty). 
#         On success the function adds a row to the DB, with the count initialized to 1
#         On success the result is the count of logins
#         On failure the result is an error code (<0) from the list below
    def add(self, user, password):
        if user in self.users:
            return ERR_USER_EXISTS
        def valid_username(username):
            return username != "" and len(username) <= UsersModel.MAX_USERNAME_LENGTH

        def valid_password(password):
            return len(password) <= UsersModel.MAX_PASSWORD_LENGTH
        
        if not valid_username(user):
            return ERR_BAD_USERNAME
        if not valid_password(password):
            return ERR_BAD_PASSWORD
        
        self.users[user] = UserData(user, password)
        assert self.users[user].count == 1
        return self.users[user].count

#    int TESTAPI_resetFixture();
#        Reset the database to the empty state.
#        Used for testing
    def TESTAPI_resetFixture(self):
        self.reset ()

# We keep a global instance of the UsersModel
g_users = UsersModel ()

class UsersController:
    """This is a controller for the main /users requests"""
    
    def do_POST(self, request):
        if request.path == "/users/login" or request.path == "/users/add":
            # Most of the code for "login" and "add" is the same
            rdata = request.getRequestData()
            if not rdata: return
            
            username = rdata["user"]
            password = rdata["password"]
            if request.path == "/users/login":
                rval = g_users.login(username, password)
            else:
                rval = g_users.add(username, password)
                
            if rval < 0:
                resp = {"errCode" : rval}
            else:
                resp = {"errCode" : SUCCESS, "count" : rval}
            request.sendResponse(data = json.dumps(resp))
        else:
            return request.send_error(404, "Unrecognized request")


#Create custom HTTPRequestHandler class
class UserCounter_HTTPRequestHandler(BaseHTTPRequestHandler):

    # We serve the static files (client.html, etc.). This enables us to load the HTML from the
    # same domain (e.g., localhost:8000) that we send the requests to
    def do_GET(self):
        try:
            if self.path not in ["/client.html", "/client.css", "/client.js"]:
                self.send_error(404, 'file not found')
                return

            if self.path.endswith(".html"):
                mimetype='text/html'
            elif self.path.endswith(".css"):
                mimetype='text/css'
            elif self.path.endswith(".js"):
                mimetype='text/javascript'
            else:
                assert False
                
            from os import curdir, sep
            f = open(curdir + sep + self.path)
            self.sendResponse(status=200, contentType=mimetype, data = f.read())
            f.close()
        except:
            self.send_error(500, 'unknown error')
            pass


    def do_POST(self):
        
        # A simple dispatcher based on the url path
        if self.path.find("/users/") == 0:
            UsersController().do_POST(self)
        elif self.path.find("/TESTAPI/") == 0:
            TESTAPI_Controller().do_POST(self)
        else:
            self.send_error(404, 'file not found')
            return


    ### Some generic HTTP processing functions
    # A generic function for sending a HTTP response
    def sendResponse(self, status=200, contentType='application/json', data=""):
        self.send_response(status)
        self.send_header('Content-type', contentType)
        self.end_headers()
        if data:
            self.wfile.write(data)
    
    
    # Return the JSON data from the request, as a dictionary
    def getRequestData(self):
        # We need to know how many bytes to read
        length = int(self.headers.getheader('content-length'))
        req = self.rfile.read(length)
    
        # The request must be a JSON request
        # Note: Python (at least) nicely tacks UTF8 information on this,
        #   we need to tease apart the two pieces.
        if not 'application/json' in self.headers.getheader('content-type').split(";"):
            self.send_error(500, 'wrong content-type on request')
            return { }
        return json.loads( req ) # throws on malformed JSON
    

class TESTAPI_Controller:
    """This is a controller for the special TESTAPI_ interface to the server."""
    
    def do_POST(self, request):
        # Note: This is added functionality to make unit testing easier
        if request.path == "/TESTAPI/resetFixture":
            g_users.TESTAPI_resetFixture()
            request.sendResponse(data=json.dumps( {"errCode" : SUCCESS} ))   # To simplify the testing, make this be a JSON object
            return
        
        elif request.path == "/TESTAPI/unitTests":
            # We run the unit tests and collect the output into a temporary file
            # Conveniently, we have a Makefile target for all unit_tests
            # There are better ways of doing this in Python, but this is a more portable example

            (ofile, ofileName) = tempfile.mkstemp(prefix="userCounter")
            try:
                errMsg = ""     # We accumulate here error messages
                output = ""     # Some default values
                totalTests = 0
                nrFailed   = 0
                while True:  # Give us a way to break
                    # Find the path to the server installation
                    thisDir = os.path.dirname(os.path.abspath(__file__))
                    cmd = "make -C "+thisDir+" unit_tests >"+ofileName+" 2>&1"
                    print "Executing "+cmd
                    code = os.system(cmd)
                    if code != 0:
                        # There was some error running the tests.
                        # This happens even if we just have some failing tests
                        errMsg = "Error running command (code="+str(code)+"): "+cmd+"\n"
                        # Continue to get the output, and to parse it
                        
                    # Now get the output
                    try:
                        ofileFile = open(ofileName, "r")
                        output = ofileFile.read()
                        ofileFile.close ()
                    except:
                        errMsg += "Error reading the output "+traceback.format_exc()
                        # No point in continuing
                        break
                    
                    print "Got "+output
                    # Python unittest prints a line like the following line at the end
                    # Ran 4 tests in 0.001s
                    m = re.search(r'Ran (\d+) tests', output)
                    if not m:
                        errMsg += "Cannot extract the number of tests\n"
                        break
                    totalTests = int(m.group(1))
                    # If there are failures, we will see a line like the following
                    # FAILED (failures=1)
                    m = re.search('rFAILED.*\(failures=(\d+)\)', output)
                    if m:
                        nrFailures = int(m.group(1))
                    break # Exit while

                # End while
                resp = { 'output' : errMsg + output,
                         'totalTests' : totalTests,
                         'nrFailed' : nrFailed }
                request.sendResponse(data = json.dumps(resp))
                            
            finally:
                os.unlink(ofileName)
                
            
        else:
            return request.send_error(404, "Unrecognized request")
            


def run():
    port = int(os.environ.get("PORT", 5000))
    # We use port 5000 to please Heroku 
    sys.stderr.write('http server is starting on 0.0.0.0:'+str(port)+'...\n')

    #ip and port of servr
    #by default http server port is 80
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, UserCounter_HTTPRequestHandler)
    sys.stderr.write('http server is running...\n')
    httpd.serve_forever()
    assert False #unreachable
    
if __name__ == '__main__':
    run()
