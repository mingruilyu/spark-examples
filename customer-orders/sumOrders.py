from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    parts = line.split(',')
    return (int(parts[0]), float(parts[2]))

conf = SparkConf().setMaster("local").setAppName("SumCustomerOrders")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///home/hduser/customer-orders.csv")
#orderById = lines.map(parseLine).reduceByKey(lambda x, y: x + y).collect()
#result = collections.OrderedDict(sorted(orderById))
orderById = lines.map(parseLine).reduceByKey(lambda x, y: x + y)
result = orderById.map(lambda x: (x[1], x[0])).sortByKey(False).collect()
for key, value in result:
    print("{0}: {1}".format(value, key))
