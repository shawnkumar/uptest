from fabric.api import *
from fab_node import *

class Node:
    
    def __init__(self, address):
        self.hosts = [address]

    def start(self):
        execute(start, hosts=self.hosts)

    def stop(self):
        execute(stop, hosts=self.hosts)

    def update(self,version):
        execute(update, version, hosts=self.hosts)

    def get_address(self):
        return self.hosts[0]