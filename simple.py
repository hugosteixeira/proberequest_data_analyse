#!/usr/bin/env python
from collections import defaultdict
from kafka import KafkaProducer
import dpkt
import binascii
from manuf import manuf
import json
import time

class Simulator:
    def __init__(self, host, user, passwd, vhost):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.vhost = vhost

        self.arquivos =['probes-2013-03-17.pcap0','probes-2013-03-17.pcap1']
        self.p = manuf.MacParser(update=True)
        self.devices = defaultdict(list)
        self.ssids = defaultdict(list)
        self.timeStamps = defaultdict(list)
        self.pnls = defaultdict(list)

    def generateDatas(self):
        for arquivo in self.arquivos:
            pcap = self.getPackets(arquivo)
            for ts, buf in pcap:
                try:
                    rtap = dpkt.radiotap.Radiotap(buf)
                except:
                    continue
                wifi = rtap.data
                if wifi.type == 0 and wifi.subtype == 4:
                    src = binascii.hexlify(wifi.mgmt.src).decode("utf-8")
                    try:
                        ssid = wifi.ies[0].info.decode("utf-8")
                    except:
                        print("SID Invalido")
                    mv = self.p.get_manuf(src)
                    self.populateLists(mv, src, ssid, ts)

    def getPackets(self, arquivo):
        f = open(arquivo, 'rb')
        pcap = dpkt.pcap.Reader(f)
        return pcap

    def populateLists(self, mv, src, ssid, ts):
        if ssid != "":
            self.pnls[src].append(ssid)
        else:
            ssid = 'BROADCAST'    
        self.timeStamps[ts].append(ssid)
        self.timeStamps[ts].append(mv)
        self.timeStamps[ts].append(src)
        self.devices[src].append([ts, ssid, mv])
        self.ssids[ssid].append([ts, src])

    def simulatePackets(self, speed):
        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))
        times = list(self.timeStamps.keys())
        times.sort()
    
        for counter in range(len(times)):
            try:
                waitTime = (times[counter+1]-times[counter])/speed
            except:
                waitTime = 0
            message = ''
            message += str(self.timeStamps.keys()[counter])+','
            message += str(self.timeStamps[times[counter]][0])+","      
            message+= str(self.timeStamps[times[counter]][1])+ ','
            message+=str(self.timeStamps[times[counter]][2])


            print(message)
            producer.send('meu-topico-legal', message)
            time.sleep(waitTime)

simulador = Simulator("localhost",'hugo','041296','hugo')
simulador.generateDatas()
simulador.simulatePackets(10)
