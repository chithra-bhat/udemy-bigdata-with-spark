from pyspark import SparkContext, SparkConf
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)


def stripSpecialChars(word):
    cleanedWord = re.sub(r'^[^\w]+|[^\w]+$', '', word)
    return cleanedWord


data = sc.textFile("data/book.txt")

words = data.flatMap(lambda x: x.lower().split(" ")).map(stripSpecialChars)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).sortByKey()

result = wordCounts.collect()

for word, count in result:
    cleanWord = word.encode('ascii', 'ignore').decode() 
    if cleanWord:
        print(f"{cleanWord} : {count}")
