import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round, desc

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

spark = SparkSession.builder.appName("Bai7_Top_Product_And_Avg_Review").getOrCreate()

items = read_csv(spark, "Order_Items.csv")
reviews = read_csv(spark, "Order_Reviews.csv")
products = read_csv(spark, "Products.csv")

clean_reviews = reviews.withColumn(
    "Review_Score",
    col("Review_Score").cast("int")
).filter(
    (col("Review_Score") >= 1) & (col("Review_Score") <= 5)
)

joined = items.join(clean_reviews.select("Order_ID", "Review_Score"), "Order_ID", "left") \
    .join(products.select("Product_ID", "Product_Category_Name"), "Product_ID", "left")

result = joined.groupBy("Product_ID", "Product_Category_Name") \
    .agg(
        count("Order_Item_ID").alias("Quantity_Sold"),
        round(avg("Review_Score"), 2).alias("Avg_Review_Score")
    ) \
    .orderBy(desc("Quantity_Sold"))

result.show(20, truncate=False)
save_result(result, "Bai7_Result.csv", limit=100)

spark.stop()