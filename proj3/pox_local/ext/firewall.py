from pox.core import core
from pox.lib.recoco.recoco import Timer
from pox.lib.addresses import * 
from pox.lib.packet import *
import re

# Get a logger
log = core.getLogger("fw")

class Firewall (object):
  """
  Firewall class.
  Extend this to implement some firewall functionality.
  Don't change the name or anything -- the eecore component
  expects it to be firewall.Firewall.
  """
  
  def __init__(self):
    """
    Constructor.
    Put your initialization code here.
    """
    log.debug("------------Firewall initialized.---------")


    self.banned_ports = []
    self.banned_domains = []
    self.msdict = {}
    self.mscountdict = dict()      # {ip: {{kw1:{{in1: {sum0:count, edge1:count}, out0: same}, ...}
    self.msdata_edgedict = dict()   # store the edge data
    self.timerdict = dict()   #timer for ms, {ip: time}
    
    self.outfile = "/root/pox/ext/count.txt"
    
    f = open(self.outfile, "w")
    f.close()


    #initialise the members
    monitored_strings = []
    self.parsefile("/root/pox/ext/banned-ports.txt", self.banned_ports)
    self.parsefile("/root/pox/ext/banned-domains.txt", self.banned_domains)
    self.parsefile("/root/pox/ext/monitored-strings.txt", monitored_strings)
   
    #init the msdict for monitoring string ref
    splist = []
    for item in monitored_strings:
      splist = item.split(":")
      if not splist[0] in self.msdict:
        self.msdict[splist[0]] = [splist[1]]
      else:
        self.msdict[splist[0]].append(splist[1])

    #log.debug(self.banned_ports)
    #log.debug(self.banned_domains)
    #log.debug(self.msdict)


  def _add_ms(self, msip, msport, srcip, srcport):
    mskey = msip + "," + msport + ":" + srcip + "," + srcport
    if not mskey in self.mscountdict:
      for keyword in self.msdict[msip]:
        #first time
        if not mskey in self.mscountdict:
          self.mscountdict[mskey] = {keyword : {0:{0:0, 1:0} , 1:{0:0, 1:0}}}  #0;1 -> reverse, 0/1->sum/edge
          self.msdata_edgedict[mskey] = {keyword : {0:None , 1:None}}  #0;1 -> reverse
        #except the first time
        else:
          self.mscountdict[mskey][keyword] = {0:{0:0, 1:0}, 1:{0:0, 1:0}}
          self.msdata_edgedict[mskey][keyword] = {0:None , 1:None}  #0;1 -> reverse
      #set the timer
      self.timerdict[mskey] = Timer(30, self.writeout, args = [mskey])
    #if ms ip port is handling
    #reset all ms dict's
    else:
      self._reset_ms(mskey)
      
    #test output
    #log.debug(self.mscountdict)
    #log.debug(self.msdata_edgedict)
    #log.debug(self.timerdict)



  #reset specific msipport for all the ms dict's 
  #except msdict(used for parsed ms file)
  #self.mscountdict[msipport] should have been initialized
  def _reset_ms(self, mskey):
    try:
      for keyword in self.mscountdict[mskey]:
        #reset mscountdict
        self.mscountdict[mskey][keyword][0][0] = 0
        self.mscountdict[mskey][keyword][0][1] = 0
        self.mscountdict[mskey][keyword][1][0] = 0
        self.mscountdict[mskey][keyword][1][1] = 0

        #reset msdata_edgedict
        self.msdata_edgedict[mskey][keyword][0] = None
        self.msdata_edgedict[mskey][keyword][1] = None

        #reset timerdict
        self.timerdict[mskey].cancel()
        self.timerdict[mskey] = Timer(30, self.writeout, args = [mskey])
        
    except KeyError:
        log.debug("KeyError on reset!!")
  
  
  
  def _handle_ConnectionIn (self, event, flow, packet):
    """
    New connection event handler.
    You can alter what happens with the connection by altering the
    action property of the event.
    """
    #take care of the  banned_ports 
    if str(flow.dstport) in (self.banned_ports):
      log.debug("Deny connection [" + str(flow.src) + ":" + str(flow.srcport) + "," + str(flow.dst) + ":" + str(flow.dstport) + "]" )
      event.actoin.deny = True
    
    #take care of the banned_domains
    else:  # str(flow.dstport) == "80":
      #log.debug("Deffered connection [" + str(flow.src) + ":" + str(flow.srcport) + "," + str(flow.dst) + ":" + str(flow.dstport) + "]" )
      event.action.defer = True
    '''    
    #take care of the monitered_strings
    elif str(flow.dst) in self.msdict:
      event.action.monitor_forward = True
      event.action.monitor_backward = True
      #add to ms-dict's
      self._add_ms(str(flow.dst), str(flow.dstport))
      log.debug("Monitoring connection [" + str(flow.src) + ":" + str(flow.srcport) + "," + str(flow.dst) + ":" + str(flow.dstport) + "]" )
    
    else:
      event.action.forward = True
    '''


  def _handle_DeferredConnectionIn (self, event, flow, packet):
    """
    Deferred connection event handler.
    If the initial connection handler defers its decision, this
    handler will be called when the first actual payload data
    comes across the connection.
    """
    #get the domain and put it into a string 
    domain = str(self.gethost(packet.payload.payload.payload))

    #check the domain against the banned file
    if self.checkDomain(domain) == 1:
      log.debug("-----ip: " + str(flow.dst))
      log.debug("Blocked: "+ domain)
      event.action.deny = True
    
    #take care of the monitered_strings
    elif (str(flow.dst) in self.msdict) or (str(flow.src) in self.msdict):
      event.action.monitor_forward = True
      event.action.monitor_backward = True
      #add to ms dict's
      if str(flow.dst) in self.msdict:
        self._add_ms(str(flow.dst), str(flow.dstport), str(flow.src), str(flow.srcport))
      else:
        self._add_ms(str(flow.src), str(flow.srcport), str(flow.dst), str(flow.dstport)) 
      log.debug("Monitoring connection [" + str(flow.src) + ":" + str(flow.srcport) + "," + str(flow.dst) + ":" + str(flow.dstport) + "]" )
        
    else:
      event.action.forward = True

        
  def _handle_MonitorData (self, event, packet, reverse):
    """
    Monitoring event handler.
    Called when data passes over the connection if monitoring
    has been enabled by a prior event handler.
    """
    
    #fine the type of flow
    if reverse:
      msip = str(packet.payload.srcip)
      msport = str(packet.payload.payload.srcport)
      srcip = str(packet.payload.dstip)
      srcport = str(packet.payload.payload.dstport)
    else:
      msip = str(packet.payload.dstip)
      msport = str(packet.payload.payload.dstport)
      srcip = str(packet.payload.srcip)
      srcport = str(packet.payload.payload.srcport)

    #get the http raw data
    msdata = str(packet.payload.payload.payload)

    #update accordingly
    self.update_ms(msip, msport, srcip, srcport, msdata, reverse)
    

    

  #---- method to pasre a file ----
  #result - list
  def parsefile(self, filestr, result_list):
    f = open(filestr, "r").readlines()
    for line in f:
        result_list.append(line.split("\n")[0])

  #---- method to get the host of an HTTP header ----
  def gethost(self, http_header):
    dom = None
    #search the http header for host name
    for line in http_header.split("\n"):
      word = line.split(":")
      if word[0] == "Host":
        dom = word[1][1:-1]
        if "www." in dom:
          dom = dom[3:]
        else:
          dom = "." + dom;
        #log.debug("domain: "+word[1])
    return dom
  

  #---- check the domain against the banned list ----
  def checkDomain(self, dom):
    result = 0
    for banned_d in (self.banned_domains):
      bd = banned_d[3:] if "www." in banned_d else "."+banned_d
      if (bd in dom) and (not dom == str(None)) and not (("com." in dom) and (not "com." in bd)):
        result = 1
        break
    return result


  #---- method to update ms dict's ----
  def update_ms(self, msip, msport, srcip, srcport, msdata, reverse):

    mskey = msip + "," + msport + ":" + srcip + "," + srcport
    #reset timer for the ip
    self.timerdict[mskey].cancel()
    self.timerdict[mskey] = Timer(30, self.writeout, args = [mskey])

    data_edge = str(None)

    for target in self.mscountdict[mskey]:
      data_edge = str(self.msdata_edgedict[mskey][target][reverse])
      #sum(ABC) += count(B+C) - count(B)
      self.mscountdict[mskey][target][reverse][0] += len(re.findall(target, data_edge+msdata)) - self.mscountdict[mskey][target][reverse][1]

      #store the new edge count & data
      self.mscountdict[mskey][target][reverse][1] = len(re.findall(target, msdata))
      self.msdata_edgedict[mskey][target][reverse] = msdata
      



  #---- method to writeout to a file ----
  #call only by the timmer when it elapses
  #filename: /root/pox/ext/counts.txt
  def writeout(self, mskey):
    log.debug("Outputing to file......")

    f = open(self.outfile, "a")

    mssum = 0
    #main loop to write out
    #for key in self.mscountdict:
    for s in self.mscountdict[mskey]:
      #mssum = count(incoming) + count(outgoing)
      mssum = self.mscountdict[mskey][s][0][0] + self.mscountdict[mskey][s][1][0]
      f.write(mskey.split(":")[0]+","+s+","+str(mssum)+"\n")

    f.flush()
    f.close()

    #clean up
    del self.mscountdict[mskey]
    del self.msdata_edgedict[mskey]
    del self.timerdict[mskey]

    log.debug("Done!")


