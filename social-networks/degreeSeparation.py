from pyspark import SparkConf, SparkContext
from node import Node

def processNode(data):
    newNodes = list()
    ID, node = data[0], data[1]
    # if the node is being processed and node ID is the target
    if ID == targetCharacterID and node.status == 1:
        hitCounter.add(1)
        print("Find the target, degree of seperation is " \
            + str(node.distance))
    else:
        # Generate new nodes marked as 'being processed'
        newNodes = node.generate()
        # Emit the current node so that when we meet the
        # same ID, we will know that we have processed it.
        newNodes.append(data)
    return newNodes

def createNode(line):
    parts = line.split()
    ID = int(parts[0])
    connections = set()
    for connection in parts[1 : ]:
        connections.add(int(connection))
    # 0 not start
    # 1 pending
    # 2 done
    if ID == startCharacterID:
        status, distance = 1, 0
    else:
        status, distance = 0, 10000
    return (ID, Node(connections, status, distance))

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf, pyFiles=['node.py'])

# The characters we wish to find the degree of separation between:
startCharacterID = 5306 #SpiderMan
targetCharacterID = 14  #ADAM 3,031 (who?)

hitCounter = sc.accumulator(0)
# Load the initial RDD
metadata = sc.textFile("file:///home/hduser/Marvel-Graph.txt")
graph = metadata.map(createNode)

iterationCount = 1
while True:
    print("iteration" + str(iterationCount))
    iterationCount += 1
    # x is (id, Node)
    graph = graph.flatMap(processNode)
    # we need to call terminal action here so that the mapreduce
    # process will start.
    print("Processing " + str(graph.count()) + " values.")
    if hitCounter.value != 0:
        print("Hit the target!")
        break
    graph = graph.reduceByKey(lambda x, y: x.merge(y))
