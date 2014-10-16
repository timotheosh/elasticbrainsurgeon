#!/usr/bin/env python
"""
Developed with python2.7
Elasticsearch Brain Surgeon
Detects a split brain condition in an elastic search cluster, and acts on the
violating host, if it does not agree with the majority of nodes within the
cluster as to who the master is.

Depends on Python Elasticsearch Library (install with pip install elasticsearch).
"""
from elasticsearch import Elasticsearch
from operator import itemgetter
from subprocess import check_call, CalledProcessError
from psutil import get_process_list
from sys import exit
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
        self.initd = ['service','elasticsearch']
        self.process = 'org.elasticsearch.bootstrap.Elasticsearch'
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

    def __restart__(self):
        print "This method does not do anything.... yet."

    def checkProcess(self):
        rtn = []
        for x in get_process_list():
            # If the process is java, and the commandline args included elastic search...
            if x.name() == 'java' and self.process in x.cmdline():
                # ...add pid to list
                rtn.append(x.pid)
        return rtn

    def hariKari(self, restart=False):
        """
        Killing (and restarting) the local ElasticSearch instance.
        """
        try:
            attempts = 0
            self.initd.append('stop')
            check_call(self.initd)
        except CalledProcessError,exc:
            print exc.message

        while len(self.checkProcess()) > 0:
            if attempts == 3:
                check_call(['/sbin/halt','-p'])
            else:
                attempts += 1
                for pid in self.checkProcess():
                    check_call(['kill','-9', "%s" % pid])
        print "Committing harikari!"
        if restart:
            self.__restart__()


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

def run(host, port, harikari):
    rc = 3
    try:
        bes = ElasticBrainSurgeon(host, port)
        if not bes.checkMyMaster():
            print bes.msg.replace('LEVEL','CRITICAL').replace('MESG','I have caused a split brain!')
            rc = 2
            if harikari:
                bes.hariKari()
        else:
            print bes.msg.replace('LEVEL','OK').replace('MESG','Only one master.')
            rc = 0
    except Exception,e:
        print e.error
        rc = 2
    exit(rc)

def test(host, port):
    bes = ElasticBrainSurgeon(host, port)
    bes.hariKari()

if __name__ == "__main__":
    host = 'localhost'
    port = 9200
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--harikari',
                        action='store_true',
                        dest='harikari',
                        help='Tells the surgeon to restart the local elasticsearch')
    parser.add_argument('-t',
                        action='store_true',
                        dest='test',
                        help='Runs a harikari test on local elasticsearch')
    args = parser.parse_args()
    if args.test:
        test(host, port)
    else:
        run(host, port, args.harikari)
