from pyspark import SparkConf, SparkContext

def getFriendCountAndAge(line):
    parts = line.split(',')
    age, count = int(parts[2]), int(parts[3])
    return (age, count)

conf = SparkConf().setMaster("local").setAppName("FriendCountByAge")
sc = SparkContext(conf = conf)

friendCounts = sc.textFile("file:///home/hduser/fakefriends.csv")
rdd = friendCounts.map(getFriendCountAndAge)
countByAge = rdd.mapValues(lambda count: (count, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
aveCountByAge = countByAge.mapValues(lambda x: x[0] / x[1])
results = aveCountByAge.collect()
print(type(results))
