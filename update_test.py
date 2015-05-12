from cluster import Cluster
from node import Node
from utils import *
import re
from fabric.api import get

class TestUpdate():

    def update_test(self):
        cluster = Cluster(get_ctool_nodes())
        cluster.clean_bootstrap('apache/cassandra-2.1')

        [node1, node2] = cluster.get_nodes()

        lastkey = self.fillData(10000000000, cluster)

        cluster.round_robin_update('apache/trunk')

        repout_n1 = cluster.nodetool('repair -hosts ' + node1.get_address(), nodes=[node1], capture_output=True)
        repout_n2 = cluster.nodetool('repair -hosts ' + node2.get_address(), nodes=[node2], capture_output=True)

        (output1, error1) = repout_n1[0]
        (output2, error2) = repout_n1[0]

        #check return values of repair is succesful
        self.assertEqual(repout_n1[1], 1, str(error1))
        self.assertEqual(repout_n2[1], 1, str(error2))

        #perform some basic validation to check querying values works
        (info, rc)= cluster.stress("read n={numWrites} -pop seq=1..{lastkey} no-wrap".format(numWrites=lastkey, lastkey=lastkey))

        #check validation error-free
        self.assertEqual(rc, 1)

        #check that there are no errors in logs:
        self.check_logs(cluster)

    def check_logs(self, cluster):
        nodes = cluster.get_nodes()
        for node in nodes:
            path = node.get_log(node.get_address().replace('.', ''))
            errors = self.grep_log_for_errors(path)
            if len(errors) is not 0:
                raise AssertionError('Unexpected error in %s node log: %s' % (node.get_address(), errors))
    
    def grep_log_for_errors(self, path):
        """
        Returns a list of errors with stack traces
        in the Cassandra log of this node
        """
        expr = "ERROR"
        matchings = []
        pattern = re.compile(expr)
        with open(path) as f:
            for line in f:
                m = pattern.search(line)
                if m:
                    matchings.append([line])
                    try:
                        while line.find("INFO") < 0:
                            line = f.next()
                            matchings[-1].append(line)
                    except StopIteration:
                        break
        return matchings

    def checkDataSize(self, cluster):
        (output, error) = cluster.nodetool("cfstats", capture_output=True)[0]
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
                (output,_) = cluster.nodetool('compactionstats', nodes=[node], capture_output=True)[0]
                if pattern.match(output):
                    break