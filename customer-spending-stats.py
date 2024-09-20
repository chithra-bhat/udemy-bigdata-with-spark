from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerStats")
sc = SparkContext(conf=conf)


def preprocess(line):
    fields = line.split(",")
    return (int(fields[0]), float(fields[2]))


data = sc.textFile("data/customer-orders.csv")

customerData = data.map(preprocess)

totalAmountSpent = customerData.reduceByKey(lambda x, y: round(x + y, 2))

sortedResult = totalAmountSpent.map(lambda x: (x[1], x[0])).sortByKey(False)

result = sortedResult.collect()

print("Customer ID \t Total Amount Spent in $")
for amount, custID in result:
    print(f"{custID} \t\t {amount}")
