import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def NormalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("file:///media/monir/Software/Development/Data "
                    "Science/DataSceince/PySparkPractice/04_Word_Count/book.txt")
words = input.flatMap(NormalizeWords)
wordCounts = words\
            .map(lambda x: (x,1))\
            .reduceByKey(lambda x, y: x + y)\
            .map(lambda x: (x[1], x[0]))\
            .sortByKey()

results = wordCounts.collect()

for result in results:
    count = str(result[0]).encode('ascii', 'ignore')
    word = str(result[1]).encode('ascii', 'ignore')
    if(word):
        print(word + " : ".encode('ascii', 'ignore') + count)