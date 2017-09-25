from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    parts = line.split(',')
    station, tag, temp = parts[0], parts[2], parts[3]
    return (station, tag, temp)

conf = SparkConf().setMaster("local").setAppName("MinimumTemprature")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///home/hduser/1800.csv")
temps = lines.map(parseLine)
minTemps = temps.filter(lambda x: "TMIN" in x[1]).map(lambda x: (x[0], x[2]))
result = minTemps.reduceByKey(lambda x, y: min(x, y)).collect()
print(result)
