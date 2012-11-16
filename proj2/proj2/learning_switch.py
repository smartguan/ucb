from sim.api import *
from sim.basics import *
import time

'''
Create your learning switch in this file.
'''
class LearningSwitch(Entity):
    def __init__(self):
        # Add your code here!
        self.learnedSrcDict = dict(); #store the learned addresses
        self.learnedTTLDict = dict(); #store the ttl info
        pass

    def handle_rx (self, packet, port):
        # Add your code here!
        
        #flag for already having the address of not
        flagFlood = 1;
        
        #if contains the address, and the dest port is not the same as the src port,
        #send it with that port
        for addr in (self.learnedSrcDict):
            if addr is packet.dst:
                if not packet.src in self.learnedSrcDict or (packet.src in self.learnedSrcDict and not self.learnedSrcDict[addr] is self.learnedSrcDict[packet.src]):
                    self.send(packet, self.learnedSrcDict[addr], flood=False)
                
                #set tyhe flag
                flagFlood = 0;
      
        #flood the packet
        if flagFlood == 1:
            self.send(packet, port, flood=True);
        
        #deal with learning
        if not packet.src in self.learnedSrcDict or (packet.src in self.learnedSrcDict and self.learnedTTLDict[packet.src] < packet.ttl):
            self.learnedSrcDict[packet.src] = port;
            self.learnedTTLDict[packet.src] = packet.ttl;

