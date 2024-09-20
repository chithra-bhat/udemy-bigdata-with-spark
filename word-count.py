from pyspark import SparkContext, SparkConf
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


data = sc.textFile("data/book.txt")

words = data.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

sortedWordCounts = wordCounts.map(lambda x: (x[1], x[0])) \
    .sortByKey(ascending=False)

result = sortedWordCounts.collect()

for count, word in result:
    word = word.encode('ascii', 'ignore')
    if word:
        print(f"{word.decode()} : {count}")
