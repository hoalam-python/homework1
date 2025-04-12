from pyspark import SparkContext

sc = SparkContext(master="local", appName="103-Spark")

data = [{"id": 1, "name": "Vnontop"}, {"id": 2, "name": "Nam"}, {"id": 3, "name": "Dat"}]
rdd = sc.parallelize(data)

print(rdd.collect())       # Trả về tất cả dữ liệu trong RDD dưới dạng list
print(rdd.count())         # Số phần tử trong RDD
print(rdd.first())         # Xem bản ghi đầu tiên
