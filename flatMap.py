from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

fileRdd = sc.textFile("file draft.txt")

wordRdd = fileRdd.flatMap(lambda line: line.split(" "))
print(wordRdd.collect())

# Ghi nhớ: map => áp dụng toàn bộ dòng | flatMap => tách từng phần tử
