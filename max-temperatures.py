from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, temperature)


data = sc.textFile("data/1800.csv")

maxTempRDD = data.filter(lambda x: 'TMAX' in x)

cleanedTemp = maxTempRDD.map(parseLine)

maxTemps = cleanedTemp.reduceByKey(lambda x, y: round(max(x, y), 2)).collect()

print("Station ID \t Max Temperature in F")
for key, val in maxTemps:
    print(f"{key} \t {val}")
