#!/usr/bin/env python2.7
"""
Elasticsearch Brain Surgeon
Detects a split brain condition in an elastic search cluster, and acts on the
violating host, if it does not agree with the majority of nodes within the
cluster as to who the master is.

Depends on Pythoin Elasticsearch Library (install with pip install elasticsearch).
"""
from elasticsearch import Elasticsearch
from operator import itemgetter
import re

def masterWinner(masters):
    sorted_masters = sorted(masters.items(), key=itemgetter(1))
    sorted_masters.reverse()
    return sorted_masters[0]

def hariKari():
    print "Committing harikari!"

masters = {}
myhost = 'localhost:9200'
myEs = Elasticsearch(myhost)
nodes = myEs.nodes.stats()['nodes']
ipregex = r"""inet\[\/(?P<ipaddress>[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+).*"""

for x in nodes.keys():
    try:
        match_obj = re.search(ipregex, nodes[x]['ip'][0])
        groups = match_obj.groupdict()
        ipaddress = groups['ipaddress']
        es = Elasticsearch('%s:9200' % ipaddress)
        master = es.cat.master()
        if master not in masters:
            q = {master: 1}
            masters.update(q)
        else:
            masters[master] += 1
    except Exception,exc:
        print exc.message

if len(masters) > 1:
    myMaster = masterWinner(masters)
    if myMaster[0] != myEs.cat.master():
        # If I have the wrong master, commit hariKari.
        hariKari()
