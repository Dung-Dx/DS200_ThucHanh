from pyspark import SparkContext, SparkConf

conf = SparkConf() \
    .setAppName("Assignment_Bai1") \
    .setMaster("yarn") \
    .set("spark.yarn.jars", "file:///D:/Install/Spark/spark-3.5.8-bin-hadoop3/jars/*.jar")

sc = SparkContext(conf=conf)

movies = sc.textFile("/Lab3/movies.txt").map(lambda l: l.split(','))
ratings = sc.textFile("/Lab3/ratings_*.txt").map(lambda l: l.split(','))

movie_names = movies.map(lambda x: (x[0], x[1]))
rating_stats = ratings.map(lambda x: (x[1], (float(x[2]), 1))) \
                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                      .mapValues(lambda v: (v[0] / v[1], v[1])) \
                      .filter(lambda x: x[1][1] >= 50)

result = movie_names.join(rating_stats).sortBy(lambda x: x[1][1][0], ascending=False)
result.saveAsTextFile("/Lab3/output/Bai1")
sc.stop()