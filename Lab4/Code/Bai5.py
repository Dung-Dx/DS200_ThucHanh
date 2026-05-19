import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round

DATA_DIR = "../"
RESULT_DIR = "../Result"

def read_csv(spark, filename):
    return spark.read.csv(f"{DATA_DIR}/{filename}", header=True, inferSchema=True, sep=";")

def save_result(df, filename):
    os.makedirs(RESULT_DIR, exist_ok=True)
    rows = df.collect()
    path = os.path.join(RESULT_DIR, filename)

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        for row in rows:
            writer.writerow([row[c] for c in df.columns])

    print(f"Saved: {path}")

spark = SparkSession.builder.appName("Bai5_Review_Statistics").getOrCreate()

reviews = read_csv(spark, "Order_Reviews.csv")

clean_reviews = reviews.withColumn(
    "Review_Score",
    col("Review_Score").cast("int")
).filter(
    col("Review_Score").isNotNull()
).filter(
    (col("Review_Score") >= 1) & (col("Review_Score") <= 5)
)

avg_result = clean_reviews.select(
    round(avg("Review_Score"), 2).alias("Avg_Review_Score")
)

count_result = clean_reviews.groupBy("Review_Score") \
    .agg(count("Review_ID").alias("Number_Of_Reviews")) \
    .orderBy("Review_Score")

avg_result.show()
count_result.show()

save_result(avg_result, "Bai5_Avg_Result.csv")
save_result(count_result, "Bai5_Count_By_Score_Result.csv")

spark.stop()