from cluster import Cluster
from node import Node
from utils import *

class TestUpdate():

    def update_test(self):
        cluster = Cluster(get_ctool_nodes())
        cluster.clean_bootstrap('apache/cassandra-2.1')

        [node1, node2] = cluster.get_nodes()
        cluster.stress("write n=50000", [node1,node2])

        cluster.update('apache/trunk', node1)

