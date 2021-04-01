from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
from ryu.lib.packet import ipv4
from ryu.lib.packet import in_proto
from ryu.lib.packet import icmp
from ryu.lib.packet import tcp
from ryu.lib.packet import udp
import threading

video_server='00:00:00:00:00:04'
vidoe_client='00:00:00:00:00:01'
 
class SimpleSwitch13(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    def __init__(self, *args, **kwargs):
        super(SimpleSwitch13, self).__init__(*args, **kwargs)
        self.mac_to_port = {}

 
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):

      datapath = ev.msg.datapath
      ofproto = datapath.ofproto
      parser = datapath.ofproto_parser
      dpid=datapath.id
      


      match = parser.OFPMatch()
      actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                        ofproto.OFPCML_NO_BUFFER)]

      self.add_flow(datapath, 0, match, actions, meter=None)

      

      print("Creating Meters")

      if 1==1:
        bands=[]
        dropband=parser.OFPMeterBandDrop(rate=5000, burst_size=0)
        bands.append(dropband)
        meter_id=99
        request=parser.OFPMeterMod(datapath=datapath,
                                  command=ofproto.OFPMC_ADD,
                                  flags=ofproto.OFPMF_KBPS,
                                  meter_id=meter_id,
                                  bands=bands)
        datapath.send_msg(request)

        self.logger.info("New Meter Created on Switch: %s with ID: %s", dpid,meter_id)
      

        

 
    def add_flow(self, datapath, priority, match, actions, buffer_id=None, meter=None):
      ofproto = datapath.ofproto
      parser = datapath.ofproto_parser

      if meter==99:
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,actions),parser.OFPInstructionMeter(99)]
      else:
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                           actions)]
      
      if buffer_id:
          mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                  priority=priority, match=match,
                                  instructions=inst)
      else:
          mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                  match=match, instructions=inst)

      datapath.send_msg(mod)



 
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):

      # If you hit this you might want to increase
      # the "miss_send_length" of your switch
      if ev.msg.msg_len < ev.msg.total_len:
          self.logger.debug("packet truncated: only %s of %s bytes",
                            ev.msg.msg_len, ev.msg.total_len)

      msg = ev.msg
      datapath = msg.datapath
      ofproto = datapath.ofproto
      parser = datapath.ofproto_parser
      in_port = msg.match['in_port']



      pkt = packet.Packet(msg.data)
      eth = pkt.get_protocols(ethernet.ethernet)[0]

      if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
        return


      dst = eth.dst
      src = eth.src

      dpid = datapath.id
      self.mac_to_port.setdefault(dpid, {})

      self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port) 

      # learn a mac address to avoid FLOOD next time.

      self.mac_to_port[dpid][src] = in_port


      if dst in self.mac_to_port[dpid]:
          out_port = self.mac_to_port[dpid][dst]

      else:
          out_port = ofproto.OFPP_FLOOD



      actions = [parser.OFPActionOutput(out_port)]
      priority=1
      # install a flow to avoid packet_in next time
      if out_port != ofproto.OFPP_FLOOD:

        if (dst==video_server or src==vidoe_client):
        
          priority=10
          match = parser.OFPMatch(in_port=in_port, eth_dst=dst,eth_src=src)
          if msg.buffer_id != ofproto.OFP_NO_BUFFER:
            self.add_flow(datapath, priority, match, actions, msg.buffer_id,meter=99)
            return
          else:
            self.add_flow(datapath, priority, match, actions,meter=99)

        

         
        else:
          match = parser.OFPMatch(in_port=in_port, eth_dst=dst,eth_src=src)
          if msg.buffer_id != ofproto.OFP_NO_BUFFER:
            self.add_flow(datapath, priority, match, actions, msg.buffer_id)
            return

          else:
            self.add_flow(datapath, priority, match, actions)

      data = None
      if msg.buffer_id == ofproto.OFP_NO_BUFFER:
        data = msg.data



      out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                in_port=in_port, actions=actions, data=data)
      datapath.send_msg(out)
