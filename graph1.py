#!/usr/bin/env python
import dpkt
import binascii
import time


arquivos =['probes-2013-03-17.pcap0','probes-2013-03-17.pcap1','probes-2013-03-17.pcap2','probes-2013-03-17.pcap3','probes-2013-03-17.pcap4','probes-2013-03-19.pcap0','probes-2013-03-19.pcap1']
devices = {}
ssids = {}

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
            src = binascii.hexlify(wifi.mgmt.src).decode("utf-8")#MAC
            
            try:
                ssid = wifi.ies[0].info.decode("utf-8")
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
tamanhos = []
for ssid in ssids.keys():
    tamanho = len(ssids[ssid])
    tamanhos.append((tamanho, ssid))
tamanhos.sort()
tamanhos.reverse()
maxSsid = tamanhos[1:5]
valor = []
for i in maxSsid:
    valor.append(i[0])
print(valor)
f.close()
