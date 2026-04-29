from pyspark import SparkContext, SparkConf
from datetime import datetime

conf = SparkConf() \
    .setAppName("Assignment_Bai6") \
    .setMaster("yarn") \
    .set("spark.yarn.jars", "file:///D:/Install/Spark/spark-3.5.8-bin-hadoop3/jars/*.jar")
sc = SparkContext(conf=conf)

ratings = sc.textFile("/Lab3/ratings_*.txt").map(lambda l: l.split(','))

def to_year(ts):
    return datetime.fromtimestamp(int(ts)).year

result = ratings.map(lambda x: (to_year(x[3]), (float(x[2]), 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: (v[0] / v[1], v[1])) \
    .sortByKey()

result.saveAsTextFile("/Lab3/output/Bai6")
sc.stop()