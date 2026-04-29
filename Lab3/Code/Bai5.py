from pyspark import SparkContext, SparkConf

conf = SparkConf() \
    .setAppName("Assignment_Bai5") \
    .setMaster("yarn") \
    .set("spark.yarn.jars", "file:///D:/Install/Spark/spark-3.5.8-bin-hadoop3/jars/*.jar")
sc = SparkContext(conf=conf)

users = sc.textFile("/Lab3/users.txt").map(lambda l: l.split(','))
occupations = sc.textFile("/Lab3/occupation.txt").map(lambda l: l.split(','))
ratings = sc.textFile("/Lab3/ratings_*.txt").map(lambda l: l.split(','))

occ_names = occupations.map(lambda x: (x[0], x[1]))
user_occ = users.map(lambda x: (x[0], x[3]))
rating_user = ratings.map(lambda x: (x[0], float(x[2])))

# Join User-Rating sau đó Join với Occupation Name
res = rating_user.join(user_occ) \
    .map(lambda x: (x[1][1], (x[1][0], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: v[0] / v[1]) \
    .join(occ_names) \
    .map(lambda x: (x[1][1], x[1][0]))

res.saveAsTextFile("/Lab3/output/Bai5")
sc.stop()