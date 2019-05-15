#!/usr/bin/env python
import dpkt
import binascii
import time
import requests
from manuf import manuf

arquivos =['probes-2013-03-17.pcap0','probes-2013-03-19.pcap0','probes-2013-03-19.pcap1','probes-2013-03-17.pcap1','probes-2013-03-17.pcap2','probes-2013-03-17.pcap3','probes-2013-03-17.pcap4']
devices = {}
ssids = {}
marcas ={}
contApple = 0
contSans = 0
contNok = 0
contOutros = 0
mac = []
p = manuf.MacParser(update=True)

for arquivo in arquivos:
    f = open(arquivo,'rb')
    npkt = 0
    pcap = dpkt.pcap.Reader(f)
    for ts, buf in pcap:
        npkt +=1
        try:
            rtap = dpkt.radiotap.Radiotap(buf)
        except:
            continue
        wifi = rtap.data
        if wifi.type == 0 and wifi.subtype == 4:
            src = binascii.hexlify(wifi.mgmt.src).encode("ascii", 'ignore')#MAC
            try:
                ssid = wifi.ies[0].info.encode("ascii", 'ignore')
                try:
                    devices[src].append([ts,ssid])
                    
                except:
                    devices[src] = [ts,ssid]
                try:
                    ssids[ssid].append([ts,src])
                except:
                    ssids[ssid] = [ts,src]
            except:
                print("SSID Invalido")

for mac in devices.keys():
    mv=p.get_manuf(mac)
    try:
        marcas[mv] +=1
    except:
        marcas[mv] =1

    

def othersCouner():
    quant = 0
    for marca in marcas:
        if marca != 'Apple' and marca != 'SamsungE' and marca != 'Nokia' and marca != 'HTC' and marca != 'Sony' and marca != 'Rim':
            quant += marcas[marca]
            print(quant)
f.close()
