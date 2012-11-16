from sim.api import *
from sim.basics import *

'''
Create your RIP router in this file.
'''
class RIPRouter (Entity):
    def __init__(self):
        #RT = {dst : {port : distance}}
        self.RT = dict()
        #self.RT_ports = dict();  #{dst: shortest_port}
        self.RUpacket = RoutingUpdate()
        self.RUpacket.src = self

    #method to calculate the minimun distance
    def min_dist_to_dest(self, dest):
        minDist = 100
        if dest in self.RT:
            for p in (self.RT[dest].keys()):
                if self.RT[dest][p] < minDist:
                    minDist = self.RT[dest][p]
        return minDist

    #method to get the port to dest with minDist
    def min_port_to_dest(self, dest):
        port = -1
        minDist = 100
        if dest in self.RT:
            for p in sorted (self.RT[dest].keys()):
                if self.RT[dest][p] < minDist:
                    minDist = self.RT[dest][p]
                    port = p
        return port

    #method to update RoutingUpdate packets
    def update_RUP(self, dest, minDist):
        if minDist == 100 and (dest in self.RUpacket.all_dests()):
            del self.RUpacket.paths[dest]
            #self.RUpacket.add_destination(dest, minDist)
        elif dest is self:
            self.RUpacket.add_destination(dest, 0);
        else:
            self.RUpacket.add_destination(dest, minDist)

    #method to send RoutingUpdates packets
    def flood_RUP(self, port):
        portlist = dict()
        portlist[port] = 1
        destlist = self.RUpacket.all_dests()
        for dst in destlist:
            for p in (self.RT[dst].keys()):
                if not p in portlist:
                    portlist[p] = 1
                    tempd = self.RUpacket.get_distance(dst)
                    del self.RUpacket.paths[dst]
                    self.send(self.RUpacket, p, flood=False)
                    self.RUpacket.add_destination(dst, tempd)

 
        #self.send(self.RUpacket, port, flood=True)
    #methond to remove a port when link down
    def remove_port(self, port):
        for dst in (self.RT.keys()):
            if port in self.RT[dst]:
                self.RT[dst][port] = 100
                minDist = self.min_dist_to_dest(dst)
            self.update_RUP(dst, minDist)

    #copy method
    def copy_rup(self, rup):
        out_rup = RoutingUpdate()
        for dst in (rup.all_dests()):
            out_rup.add_destination(dst, rup.get_distance(dst))
        return out_rup


    def handle_rx (self, packet, port):
        #RUpacket = RoutingUpdate()
        minDist = 100
        out_port = -1
        flag_RUP = 0;
        flag_RU = 0;
        flag_PV = 0;

        print "-----------%s---------" % self
        print "recieving packet: %s, port: %s" % (packet,port)

        #if it's a discorvery packet
        #if packet.dst is NullAddress and not packet.src is NullAddress:
        try:
            if packet.is_link_up:
                if not packet.src in self.RT:
                    self.RT[packet.src] = dict()
                self.RT[packet.src][port] = 1
                #minDist = 1
                self.update_RUP(packet.src, 1)
            else:
                self.remove_port(port)
                #self.RT[packet.src][port] = 100
                #find the min distance to src
                #minDist = self.min_dist_to_dest(packet.src)

            #make the update packet
            #RUpacket.add_destination(packet.src, minDist)
            #self.update_RUP(packet.src, minDist)
            #flood the RUpacket except for the incoming port
            #self.send(RUpacket, port, flood=True)
            self.flood_RUP(port)
            print "RUP: %s" % self.RUpacket.str_routing_table()
            print "RT: %s" % self.RT

        except AttributeError:
            try:
            #if it's a RoutingUpdate packet
            #elif packet.dst is NullAddress and packet.src is NullAddress:
                for dst in (packet.all_dests()):
                    print "Incoming RUP: %s" % packet.str_routing_table()
                    print "RT_pre: %s" % self.RT
                    #deal with ports that can't connect to dst
                    #if packet.get_distance(dst) == -1:
                    #    del self.RT[packet.dst][port]
                    #    minDist = self.min_dist_to_dest(dst)
                    minDist = 100
                    flag_RU = 0
                    #poison reversed
                    if packet.get_distance(dst) == 100:
                        if not dst in self.RT:
                            self.RT[dst] = dict()
                        self.RT[dst][port] = 100
                        #minDist = self.min_dist_to_dest(dst)
                        flag_PV = 1

                    #if a new dest
                    elif not dst in self.RT:
                        minDist = packet.get_distance(dst) + 1
                        self.RT[dst] = dict()
                        self.RT[dst][port] = minDist
                        flag_RU = 1
                        #print "RT: %s" % self.RT
                    #if dst already exits but the port DNE
                    elif dst in self.RT and not port in self.RT[dst]:
                        self.RT[dst][port] = packet.get_distance(dst) + 1
                        minDist = self.min_Dist_to_Dest(dst);    
                        flag_RU = 1
                    #if dst and port
                    elif dst in self.RT and port in self.RT[dst] and self.RT[dst][port] != packet.get_distance(dst) + 1:
                        self.RT[dst][port] = packet.get_distance(dst)+1
                        minDist = self.min_Dist_to_Dest(dst)
                        flag_RU = 1
                    else:
                        flag_RU = 0


                    #make the update packet
                    #RUpacket.add_destination(dst, minDist)
                    if flag_RU == 1 and flag_PV == 0:
                        self.update_RUP(dst, minDist)
                        flag_RUP = 1;

                #deal with implicitly withdrawn
                if flag_PV == 0:
                    for dst in (self.RT.keys()):
                        if not dst is packet.src and port in self.RT[dst] and not dst in packet.all_dests():
                            self.RT[dst][port] = 100
                            minDist = self.min_dist_to_dest(dst)
                            self.update_RUP(dst, minDist)
                            flag_RUP = 1

                #flood the RUpacket except for the incoming port
                #self.send(RUpacket, port, flood=True)
                if flag_RUP == 1:
                    self.flood_RUP(port);
                    print "RUpacket: %s" % self.RUpacket.str_routing_table()
                print "RT: %s" % self.RT

                flag_RUP = 0

                #perform poison reverse
                #poisonRUP = self.copy_rup(self.RUpacket)
                poisonRUP = RoutingUpdate()
                for dst in (packet.all_dests()):
                    if (not dst is self) and (self.min_port_to_dest(dst) == port) and (not dst is packet.src):
                        #poisonRUP = self.RUpacket
                        poisonRUP.add_destination(dst, 100)
                        flag_RUP = 1
                if packet.src in poisonRUP.all_dests():
                    del poisonRUP.paths[packet.src]
                if flag_RUP == 1:
                    self.send(poisonRUP, port, flood=False);
                    print "RUpacket: %s" % self.RUpacket.str_routing_table()
                    print "poisonRUP: %s" % poisonRUP.str_routing_table()


            except AttributeError:

                #if it's a data packet
                #else:
                #find the port with min_distance
                out_port = self.min_port_to_dest(packet.dst)

                #send the packet
                if out_port != -1: 
                    self.send(packet, out_port, flood=False)
