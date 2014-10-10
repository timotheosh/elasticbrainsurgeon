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
from subprocess import check_call, CalledProcessError
import re

class ElasticBrainSurgeon:
    def __init__(self, host=None, port=None):
        """
        Initializes ElasticBrainSurgeon Class
        """
        self.masters = {}
        if not host:
            host = 'localhost'
        if not port:
            port = 9200
        myhost = '%s:%s' % (host, port)
        myEs = Elasticsearch(myhost)
        self.nodes = myEs.nodes.stats()['nodes'] # List of nodes in this ES cluster.
        self.myMaster = myEs.cat.master() # Who I think the master is.
        self.masters = {}                 # Who other nodes think the master is.
        self.msg = "ElasticBrainSurgeon LEVEL MESG"
        self.ipregex = r"""inet\[\/(?P<ipaddress>[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+).*"""

    def __masterWinner__(self):
        """
        Determines the master by the number of votes.
        """
        sorted_masters = sorted(self.masters.items(), key=itemgetter(1))
        sorted_masters.reverse()
        return sorted_masters[0]

    def hariKari(self):
        """
        Killing (and restarting) the local ElasticSearch instance.
        """
        try:
            check_call("service stop elasticsearch")
        except CalledProcessError,exc:
            print exc.message
        try:
            check_call("service start elasticsearch")
        except CalledProcessError,exc:
            print exc.message
        print "Committing harikari!"

    def checkMyMaster(self):
        """
        Who is the master. Who do I think it is? Who does everyone
        else think it is?
        """
        iamok = True
        for x in self.nodes.keys():
            try:
                match_obj = re.search(self.ipregex, self.nodes[x]['ip'][0])
                groups = match_obj.groupdict()
                ipaddress = groups['ipaddress']
                es = Elasticsearch('%s:9200' % ipaddress)
                master = es.cat.master()
                if master not in self.masters:
                    q = {master: 1}
                    self.masters.update(q)
                else:
                    self.masters[master] += 1
            except Exception,exc:
                print exc.message

        if len(self.masters) > 1:
            if self.myMaster != self.__masterWinner__():
                # If I have the wrong master, return False.
                iamok = False
        return iamok

if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--harikari',
                        action='store_true',
                        dest='harikari',
                        help='Tells the surgeon to restart the local elasticsearch')
    args = parser.parse_args()
    bes = ElasticBrainSurgeon('localhost', 9200)
    if not bes.checkMyMaster():
        print bes.msg.replace('LEVEL','CRITICAL').replace('MESG','I have caused a split brain!')
        if args.harikari:
            bes.hariKari()
    else:
        print bes.msg.replace('LEVEL','OK').replace('MESG','Only one master.')
