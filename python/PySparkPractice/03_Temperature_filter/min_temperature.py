from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("TemperatureFilter")
sc = SparkContext(conf = conf)

def ParseLine(line):
    fields = line.split(',')
    stationId = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationId, entryType, temperature)

lines = sc.textFile("file:///media/monir/Software/Development/Data "
                    "Science/DataSceince/PySparkPractice/03_Temperature_filter/1800.csv")
parseLine = lines.map(ParseLine)

minTemps = parseLine.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

results = minTemps.collect()
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
