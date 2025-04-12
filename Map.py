from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

fileRdd = sc.textFile("file draft.txt")

allCapRdd = fileRdd.map(lambda line: line.upper())
print(allCapRdd.collect())

for line in allCapRdd.collect():
    print(line)
