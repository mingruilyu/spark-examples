class Node(object):
    def __init__(self, connections, status, distance):
        self.connections = connections
        self.status = status
        self.distance = distance

    def merge(self, other):
        self.connections.union(other.connections)
        self.distance = min(self.distance, other.distance)
        self.status = max(self.status, other.status)
        return self

    def generate(self):
        newNodes = list()
        if self.status == 1:
            self.status = 2
            for connection in self.connections:
                newNodes.append(
                    (connection,
                     Node(set(), 1,
                          self.distance + 1)))
        return newNodes
