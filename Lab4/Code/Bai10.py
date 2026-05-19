import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, round, desc, rank
from pyspark.sql.window import Window

DATA_DIR = "../"
RESULT_DIR = "../Result"

def read_csv(spark, filename):
    return spark.read.csv(f"{DATA_DIR}/{filename}", header=True, inferSchema=True, sep=";")

def save_result(df, filename, limit=None):
    os.makedirs(RESULT_DIR, exist_ok=True)
    if limit is not None:
        df = df.limit(limit)

    rows = df.collect()
    path = os.path.join(RESULT_DIR, filename)

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        for row in rows:
            writer.writerow([row[c] for c in df.columns])

    print(f"Saved: {path}")

spark = SparkSession.builder.appName("Bai10_Seller_Ranking").getOrCreate()

items = read_csv(spark, "Order_Items.csv")

seller_stats = items.withColumn(
    "Revenue",
    col("Price") + col("Freight_Value")
).groupBy(
    "Seller_ID"
).agg(
    round(sum("Revenue"), 2).alias("Total_Revenue"),
    countDistinct("Order_ID").alias("Total_Orders")
)

window_spec = Window.orderBy(desc("Total_Revenue"), desc("Total_Orders"))

result = seller_stats.withColumn(
    "Seller_Rank",
    rank().over(window_spec)
).orderBy(
    "Seller_Rank"
)

result.show(20, truncate=False)
save_result(result, "Bai10_Result.csv", limit=200)

spark.stop()