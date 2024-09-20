from pyspark import SparkContext, SparkConf


def process_lines(line):
    info = line.split(",")
    age = int(info[2])
    friends = int(info[3])
    return age, friends


conf = SparkConf().setMaster("local").setAppName("AverageFriendsByAge")
sc = SparkContext(conf=conf)

lines = sc.textFile("data/fakefriends.csv")
rdd = lines.map(process_lines)

count_friends = rdd.mapValues(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

average_friends = count_friends.mapValues(lambda x: round(x[0] / x[1], 2)) \
    .sortByKey()

result = average_friends.collect()

for key, val in result:
    print(f"{key} {val}")
