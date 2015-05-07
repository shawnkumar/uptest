def get_ctool_nodes():
    with open('/etc/hosts', 'r') as f:
        nodes = []
        for line in f:
            if " node" in line and "node0" not in line:
                nodes.append(line[0: line.find(' ')])
        return nodes
