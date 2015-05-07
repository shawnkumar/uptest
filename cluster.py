from node import ValNode
from fabric.api import *
from subprocess import Popen
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy

class Cluster(object):
    
    def __init__(self, nodelist):
        self.nodelist = []
        self.hosts = nodelist
        for item in nodelist:
            self.nodelist.append(ValNode(item))
            
    def clean_bootstrap(self, version):
        local("cstar_perf_bootstrap -v " + version)

    def get_nodes(self):
        return self.nodelist

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

    def stress(self, command, nodes, parallel=False):
        base = 'JAVA_HOME=~/fab/java ~/fab/stress/cassandra-2.1/tools/bin/cassandra-stress'
        hosts = ""
        for node in nodes:
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

    def nodetool(self, cmd, nodes, capture_output=False, parallel=False):
        base = 'JAVA_HOME=~/fab/java ~/fab/stress/cassandra-2.1/bin/nodetool'
        hosts = ""
        for node in nodes:
            if hosts != '':
                hosts += "," + node.get_address()
            else:
                hosts = node.get_address()
        command = "{base} {nodes} {cmds}".format(base=base, nodes=hosts, cmds=cmd)
        if parallel:
            p = Popen(command, shell=True)
        elif capture_output:
            p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            return p.communicate()
        else:
            p = subprocess.Popen(command, shell=True)
            p.wait()

    def update(self, version, nodes=None):
        if nodes != None:
            for node in nodes:
                node.update(version)
        else:
            for node in self.hosts:
                node.update(version)