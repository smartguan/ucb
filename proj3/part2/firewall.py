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
    
    #members declarations
    self.temp_ports_dict = dict()  #ports that get temperally allowed


    log.debug("------------Firewall initialized.---------")


  def _handle_ConnectionIn (self, event, flow, packet):
    """
    New connection event handler.
    You can alter what happens with the connection by altering the
    action property of the event.
    """
    curkey = str(flow.src) + "||" + str(flow.dst) + ":" + str(flow.dstport)

    if flow.dstport == 21:
      event.action.defer = True
    elif flow.dstport < 1024:
      event.action.forward = True
    else:
      if curkey in self.temp_ports_dict: 
        event.action.monitor_forward = True
        event.action.monitor_backward = True
      else:
        log.debug("Deny : " + curkey)
        event.action.deny = True

  def _handle_DeferredConnectionIn (self, event, flow, packet):
    """
    Deferred connection event handler.
    If the initial connection handler defers its decision, this
    handler will be called when the first actual payload data
    comes across the connection.
    """
    log.debug("Defferred In: "+str(flow.src) + ":"+str(flow.srcport)+"||"+str(flow.dst) + ":"+str(flow.dstport))
    event.action.monitor_backward = True

        
  def _handle_MonitorData (self, event, packet, reverse):
    """
    Monitoring event handler.
    Called when data passes over the connection if monitoring
    has been enabled by a prior event handler.
    """
    
    srcip = packet.payload.srcip
    dstip = packet.payload.dstip
    srcport = packet.payload.payload.srcport
    dstport = packet.payload.payload.dstport
    if reverse:
      curkey = str(dstip) + "||" + str(srcip) + ":" + str(srcport)
    else:
      curkey = str(srcip) + "||" + str(dstip) + ":" + str(dstport)
    ftp_payload = str(packet.payload.payload.payload)
    ftp_type = ftp_payload.split(" ")[0]

    #calculate the server port if packet from server cmd port (21)
    if (str(srcport) == "21") and ((ftp_type == "227") or (ftp_type == "229")):
      #227 Entered Passive Mode (63,245,215,56,204,223)
      #port = 204*256+223
      if ftp_type == "227":
        rg = re.search(r"227 .+ \(\d+,\d+,\d+,\d+,(\d+),(\d+)\)", ftp_payload)
        server_port = int(rg.group(1))*256+int(rg.group(2))
      #229 Extended Passive Mode (|||55674|)
      else:
        rg = re.search(r"229 .+ \(\|\|\|(\d+)\|\)", ftp_payload)
        server_port = int(rg.group(1))
      
      #construct and add the tmp key   
      tmpkey = str(dstip) + "||" + str(srcip) + ":" + str(server_port)
      self.open_port(tmpkey)
    
    #for ports in self.temp_ports_dict 
    elif (curkey in self.temp_ports_dict) and (ftp_payload != str(None)):
      self.temp_ports_dict[curkey].cancel()
      self.temp_ports_dict[curkey] = Timer(10, self.close_port, args = [curkey])

    else: 
      pass

      
  #method to add a temp open port for ftp
  def open_port(self, tmpkey):
    self.temp_ports_dict[tmpkey] = Timer(10, self.close_port, args = [tmpkey])
    log.debug("connnection opened: " + tmpkey)

  #to close a port for ftp
  def close_port(self, tmpkey):
    if tmpkey in self.temp_ports_dict:
      del self.temp_ports_dict[tmpkey]
    log.debug("connection closed: " +tmpkey)

