from node import Node
from fabric.api import *
from subprocess import Popen, PIPE
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy

class Cluster(object):
    
    def __init__(self, nodelist):
        self.nodelist = []
        self.hosts = nodelist
        for item in nodelist:
            self.nodelist.append(Node(item))
            
    def clean_bootstrap(self, version):
        local("cstar_perf_bootstrap -v " + version)

    def get_nodes(self):
        return self.nodelist

    def get_nodestring(self):
        nodestring = ''
        for node in self.nodelist:
            nodestring.append(node.get_address() + ',')
        return nodestring[:-1]

    def get_session(self, nodes, keyspace=None, exclusive=False, cons_level=None):
        node_ips = []
        for node in nodes:
            node_ips.append(node.get_address())

        if exclusive is True:
            wlrr = WhiteListRoundRobinPolicy(node_ips)
            cluster = Cluster(node_ips, load_balancing_policy=wlrr)
            session = cluster.connect()
        else:
            cluster = Cluster(node_ips)
            session = cluster.connect()

        if keyspace is not None:
            session.execute('USE %s;' % keyspace)

        if cons_level is not None:
            session.default_consistency_level = cons_level
        return session

    def stress(self, command, nodes=None, parallel=False):
        base = 'JAVA_HOME=~/fab/java ~/fab/stress/cassandra-2.1/tools/bin/cassandra-stress'
        hosts = ""
        if nodes != None:
            for node in nodes:
                if hosts != '':
                    hosts += "," + node.get_address()
                else:
                    hosts = node.get_address()
        else:
            for node in self.nodelist:
                if hosts != '':
                    hosts += "," + node.get_address()
                else:
                    hosts = node.get_address()
        hosts = "-node " + hosts
        line = "{base} {cmd} {hosts}".format(base=base, cmd=command, hosts=hosts)
        if parallel == True:
            p = Popen(line, shell=True)
        else:
            local(line)

    def create_ks(self, keyspace, settings):
        session = self.get_session(self.nodelist)
        querytemplate = "CREATE KEYSPACE {name} WITH {options};".format(name=keyspace, options=settings)
        try:
            session.execute(querytemplate)
            return keyspace
        except:
            print "Unable to create keyspace, may already exist"

    def nodetool(self, cmd, nodes=None, capture_output=False, parallel=False):
        base = 'JAVA_HOME=~/fab/java ~/fab/stress/cassandra-2.1/bin/nodetool'
        hosts = ""
        if nodes != None:
            for node in nodes:
                if hosts != '':
                    hosts += "," + node.get_address()
                else:
                    hosts = node.get_address()
        else:
            for node in self.nodelist:
                if hosts != '':
                    hosts += "," + node.get_address()
                else:
                    hosts = node.get_address()

        hostnodes = "-h " + hosts

        command = "{base} {nodes} {cmds}".format(base=base, nodes=hostnodes, cmds=cmd)
        if parallel:
            p = Popen(command, shell=True)
        elif capture_output:
            p = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
            return p.communicate()
        else:
            p = Popen(command, shell=True)
            p.wait()

    def round_robin_update(self, version):
        for node in self.nodelist:
            self.nodetool('drain', nodes=[node])
            node.stop()
            node.update(version)
            node.start()