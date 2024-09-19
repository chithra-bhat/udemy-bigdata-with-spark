from pyspark import SparkConf, SparkContext


def parseLines(line):
    info = line.split(",")
    stationID = info[0]
    celsiusTemp = float(info[3]) * 0.1 * (9/5) + 32
    return (stationID, celsiusTemp)


conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)

lines = sc.textFile("data/1800.csv")

rdd = lines.filter(lambda x: "TMIN" in x)

minTemps = rdd.map(parseLines)

minTempByStation = minTemps.reduceByKey(lambda x, y: round(min(x, y), 2))

result = minTempByStation.collect()

print("\nStation ID \t Min Temperature in F")
for key, val in result:
    print(f"{key} \t {val}")
