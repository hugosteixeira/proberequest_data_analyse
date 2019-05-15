#!/usr/bin/env python
from collections import defaultdict
import dpkt
import binascii
from manuf import manuf
import json
import pika


class Simulator:
    def __init__(self, host, user, passwd, vhost):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.vhost = vhost

        self.arquivos = ['probes-2013-03-17.pcap0', 'probes-2013-03-17.pcap1', 'probes-2013-03-17.pcap2',
                         'probes-2013-03-17.pcap3', 'probes-2013-03-17.pcap4', 'probes-2013-03-19.pcap0',
                         'probes-2013-03-19.pcap1']
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
        self.timeStamps[ts].append([ssid, mv, src])
        self.devices[src].append([ts, ssid, mv])
        self.ssids[ssid].append([ts, src])

    def simulatePackets(self, speed):
        credentials = pika.PlainCredentials(self.user, self.passwd)
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.host, 5672, self.vhost, credentials))
        channel = connection.channel()
        channel.exchange_declare(exchange='topic_logs',
                         exchange_type='topic')

        times = self.timeStamps.keys().sort()
        for counter in len(times):
            try:
                waitTime = (times[counter+1]-times[counter])/speed
            except:
                waitTime = 0
            message = json.dumps(self.timeStamps[times[counter]])
            channel.basic_publish(exchange='topic_logs', routing_key=times[counter], body=message)
            time.sleep(waitTime)

simulador = Simulator("host",'user','passwd','vhost')
simulador.generateDatas()
simulador.simulatePackets(10)
