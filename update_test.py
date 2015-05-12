from cluster import Cluster
from node import Node
from utils import *
import re

class TestUpdate():

    def update_test(self):
        cluster = Cluster(get_ctool_nodes())
        cluster.clean_bootstrap('apache/cassandra-2.1')

        [node1, node2] = cluster.get_nodes()

        lastkey = self.fillData(10000000000, cluster)

        cluster.round_robin_update('apache/trunk')

        repout_node1 = cluster.nodetool('repair -hosts ' + node1.get_address(), nodes=[node1], capture_output=True)
        repout_node2 = cluster.nodetool('repair -hosts ' + node2.get_address(), nodes=[node2], capture_output=True)

        self.

    def checkDataSize(self, cluster):
        (output, error) = cluster.nodetool("cfstats", capture_output=True)
        abort = False
        if output.find("Standard1") == -1:
            abort = True
        output = output[output.find("Standard1"):]
        output = output[output.find("Space used (total), bytes:"):]
        value = output[output.find(":")+1:output.find("\n")].strip()
        if value == '':
            return 0
        elif value == ' ':
            return 0
        elif value == '   ':
            return 0
        elif abort == True:
            return 0
        else:
            return int(value)

    def fillData(self, target, cluster):
        firstkey = 0
        print "Commencing data adding phase"
        nodestring = cluster.get_nodestring()
        checking = True
        numWrites = 0
        #write to initialize tables
        cluster.stress("write n=10000 -schema replication\(factor=2\)")
        while checking:
            datasize = self.checkDataSize(cluster)
            print "data size: " + datsize
            numWrites = int(float(target - datasize)/float(340))
            if datasize == 0:
                print "zero"
            else:
                checking = False
        #bigwrite should take close to value we want, note if negative stress won't do anything
        with open(os.devnull, 'w') as nl:
            lastkey = firstkey + numWrites
            print "Starting big stress write"
            cluster.stress("write n={numWrites} -pop seq={firstkey}..{lastkey} no-wrap -schema replication\(factor=2\)".format(numWrites=str(numWrites), firstkey=str(firstkey), lastkey=str(lastkey)))
            firstkey = lastkey + 1
            print "Flushing"
            cluster.nodetool('flush')
            sleep(10)
            self.waitForCompactions(cluster)
        underTarget = True
        #fine-tune here
        while underTarget:
            checking = True
            while checking:
                currentsize = self.checkDataSize(cluster)
                if currentsize == 0:
                    print "zero"
                else:
                    checking = False
            print "Data Size: " + str(currentsize)
            if (currentsize < (target - 1000000)):
                print "Fine data addition, current value: " + str(currentsize)
                keysneeded = int(float(target - currentsize)/float(270))
                with open(os.devnull, 'w') as nl:
                    lastkey = firstkey + keysneeded
                    cluster.stress("write n={keysneeded} -pop seq={firstkey}..{lastkey} no-wrap -schema replication\(factor=2\)".format(keysneeded=str(keysneeded), firstkey=str(firstkey), lastkey=str(lastkey)))
                    firstkey = lastkey + 1
                    print "Flushing"
                    cluster.nodetool('flush')
                    sleep(10)
                    self.waitForCompactions(cluster)
            else:
                underTarget = False
        return lastkey

    def waitForCompactions(self, cluster):
        """Check for compactions via nodetool compactionstats"""
        pattern = re.compile("^pending tasks: 0\n")
        print "Waiting for compactions to finish"
        nodes = cluster.get_nodes()
        for node in nodes:
            while True: 
                output = cluster.nodetool('compactionstats', nodes=[node], capture_output=True)
                if pattern.match(output):
                    break