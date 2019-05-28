from pyspark import SparkConf, SparkContext
import math

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///media/monir/Software/Development/Data Science/DataSceince/PySparkPractice/02_Friends_By_Age/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

averagesByAge = totalsByAge.mapValues(lambda x: math.floor(x[0] / x[1]))
results = averagesByAge.collect()

for result in results:
    print(result)