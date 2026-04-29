from pyspark import SparkContext, SparkConf

conf = SparkConf() \
    .setAppName("Assignment_Bai3") \
    .setMaster("yarn") \
    .set("spark.yarn.jars", "file:///D:/Install/Spark/spark-3.5.8-bin-hadoop3/jars/*.jar")
sc = SparkContext(conf=conf)

users = sc.textFile("/Lab3/users.txt").map(lambda l: l.split(','))
ratings = sc.textFile("/Lab3/ratings_*.txt").map(lambda l: l.split(','))

user_gender = users.map(lambda x: (x[0], x[1]))
rating_user = ratings.map(lambda x: (x[0], (x[1], float(x[2]))))

# Join: ((MovieID, Gender), (Rating, 1))
result = user_gender.join(rating_user) \
    .map(lambda x: ((x[1][1][0], x[1][0]), (x[1][1][1], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: v[0] / v[1])

result.saveAsTextFile("/Lab3/output/Bai3")
sc.stop()