from pyspark import SparkContext, SparkConf

def get_age_group(age):
    a = int(age)
    if a < 18: return "<18"
    elif a <= 24: return "18-24"
    elif a <= 34: return "25-34"
    elif a <= 44: return "35-44"
    else: return "45+"

conf = SparkConf() \
    .setAppName("Assignment_Bai4") \
    .setMaster("yarn") \
    .set("spark.yarn.jars", "file:///D:/Install/Spark/spark-3.5.8-bin-hadoop3/jars/*.jar")
sc = SparkContext(conf=conf)

users = sc.textFile("/Lab3/users.txt").map(lambda l: l.split(','))
ratings = sc.textFile("/Lab3/ratings_*.txt").map(lambda l: l.split(','))

user_age = users.map(lambda x: (x[0], get_age_group(x[2])))
rating_user = ratings.map(lambda x: (x[0], (x[1], float(x[2]))))

result = user_age.join(rating_user) \
    .map(lambda x: ((x[1][1][0], x[1][0]), (x[1][1][1], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: v[0] / v[1])

result.saveAsTextFile("/Lab3/output/Bai4")
sc.stop()