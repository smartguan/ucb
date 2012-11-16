import sys
import getopt
import socket
import random
import time

import Checksum
import BasicSender

'''
This is a skeleton sender class. Create a fantastic transport protocol here.
'''
class Sender2(BasicSender.BasicSender):
    #redefine the constructor
    #adding members for sliding window 
    #and time for retransmission
    def __init__(self,dest,port,filename,debug=False):
        self.debug = debug
        self.dest = dest
        self.dport = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(None) # blocking
        self.sock.bind(('',random.randint(10000,40000)))
        if filename == None:
            self.infile = sys.stdin
        else:
            self.infile = open(filename,"r")
        
        #for the retransmission of packet
        self.update = time.time()
        self.max_wait_time = 0.2

        #for the sliding window
        self.max_buf_size = 5
        self.wd_bottom = 0 #first sent seqno in the window
        self.seqnums = {}

        #for packet handler
        self.MESSAGE_HANDLER = {
            'ack' : self._handle_ack
        }

    def _handle_ack(self, seqno) :
        for n in sorted(self.seqnums.keys()):
            if n < int(seqno):
                del self.seqnums[n]
                #print "deleting %d %d" % (n, int(seqno))
                self.wd_bottom += 1

    def _handle_other(self, seqno) :
        pass

    # Handles a response from the receiver.
    def handle_response(self,response_packet):
        #print "Handling Response"
        if Checksum.validate_checksum(response_packet):
            print "recv: %s" % response_packet
            #call ack handler
            m = response_packet.split("|")
            msg_type, seqno = m[0:2]
            self.MESSAGE_HANDLER.get(msg_type, self._handle_other)(seqno)

        else:
            pass
            #print "recv: %s <--- CHECKSUM FAILED" % response_packet

    # Main sending loop.
    def start(self):
        seqno = 0
        time_current = 0

        msg = self.infile.read(500)
        msg_type = None
        while not msg_type == 'end' or self.seqnums:
            time_current = time.time()
            seqno_old = 0

            '''---------------Sending algorithm------------'''
            #if the window not fully opened, send the packet
            #also store the packet into the buffer
            if seqno - self.wd_bottom < self.max_buf_size and msg_type != 'end':
                next_msg = self.infile.read(500)
                #change package type
                if seqno == 0:
                    msg_type = 'start'
                elif next_msg == "":
                    msg_type = 'end'
                else:
                    msg_type = 'data'
                #make packet and send
                packet = self.make_packet(msg_type, seqno, msg)
                self.send(packet)
                self.seqnums[seqno] = packet
                self.update = time.time()
                seqno += 1
                msg = next_msg
                
            #if window opened fully, only send when having waited for 500ms
            #send the bottom packet
            elif time_current - self.update >= self.max_wait_time:
                self.send(self.seqnums[self.wd_bottom])
                self.update = time.time()
            #else:
                #print "Packet Lost"

            '''---------------Receiving algorithm------------'''
            
            response = self.receive(self.max_wait_time)

            #modify the round trip time
            #-0.11 if receive data
            #+0.11 if None
            if not response is None:
                #print "Handling response"
                self.handle_response(response)
                self.max_buf_size += 1
                if self.max_wait_time > 0.01:
                    self.max_wait_time -= 0.01
            
            else:
                if self.max_buf_size > 1 and seqno - self.wd_bottom != self.max_buf_size:
                    self.max_buf_size -= 1
            
                if self.max_wait_time < 0.8:
                    self.max_wait_time += 0.01
            #print "max_wait_time: %d" % self.max_wait_time
            print "self.max_buf_size: %d" % self.max_buf_size
            '''---------------Next loop algorithm------------'''
            #deal with packet loss
            if seqno_old == self.wd_bottom:
                #print "Packet %d got dropped" % seqno_old
                if self.seqnums:
                    #print "resending packet %d %d" % (self.wd_bottom, seqno)
                    self.send(self.seqnums[self.wd_bottom])
            else:
                seqno_old = self.wd_bottom

        self.infile.close()

'''
This will be run if you run this script from the command line. You should not
change any of this; the grader may rely on the behavior here to test your
submission.
'''
if __name__ == "__main__":
    def usage():
        print "BEARS-TP Sender"
        print "-f FILE | --file=FILE The file to transfer; if empty reads from STDIN"
        print "-p PORT | --port=PORT The destination port, defaults to 33122"
        print "-a ADDRESS | --address=ADDRESS The receiver address or hostname, defaults to localhost"
        print "-d | --debug Print debug messages"
        print "-h | --help Print this usage message"

    try:
        opts, args = getopt.getopt(sys.argv[1:],
                               "f:p:a:d", ["file=", "port=", "address=", "debug="])
    except:
        usage()
        exit()

    port = 33122
    dest = "localhost"
    filename = None
    debug = False

    for o,a in opts:
        if o in ("-f", "--file="):
            filename = a
        elif o in ("-p", "--port="):
            port = int(a)
        elif o in ("-a", "--address="):
            dest = a
        elif o in ("-d", "--debug="):
            debug = True

    s = Sender(dest,port,filename,debug)
    try:
        s.start()
    except (KeyboardInterrupt, SystemExit):
        exit()
