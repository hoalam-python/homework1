from pyspark import SparkContext, SparkConf
import time
from random import Random

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

data = ["Vnontop", "Quynh", "Dung", "Khanh", "Dat"]
rdd = sc.parallelize(data, numSlices=2)
print(rdd.glom().collect())

def process_partition(iterator):
    rand = Random(int(time.time() * 1000) + Random().randint(0, 1000))
    return [f"{name}:{rand.randint(0, 1000)}" for name in iterator]

result = rdd.mapPartitions(process_partition)
print(result.collect())
