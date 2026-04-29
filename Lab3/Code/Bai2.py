from pyspark import SparkContext, SparkConf

conf = SparkConf() \
    .setAppName("Assignment_Bai2") \
    .setMaster("yarn") \
    .set("spark.yarn.jars", "file:///D:/Install/Spark/spark-3.5.8-bin-hadoop3/jars/*.jar")
sc = SparkContext(conf=conf)

movies = sc.textFile("/Lab3/movies.txt").map(lambda l: l.split(','))
ratings = sc.textFile("/Lab3/ratings_*.txt").map(lambda l: l.split(','))

movie_genres = movies.map(lambda x: (x[0], x[2].split('|')))
rating_val = ratings.map(lambda x: (x[1], float(x[2])))

# Join và tách từng genre
genre_rating = movie_genres.join(rating_val) \
    .flatMap(lambda x: [(g, (x[1][1], 1)) for g in x[1][0]]) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: v[0] / v[1]) \
    .sortBy(lambda x: x[1], ascending=False)

genre_rating.saveAsTextFile("/Lab3/output/Bai2")
sc.stop()