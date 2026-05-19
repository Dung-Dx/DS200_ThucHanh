import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, datediff, desc

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

spark = SparkSession.builder.appName("Bai8_Delivery_Performance").getOrCreate()

orders = read_csv(spark, "Orders.csv")
items = read_csv(spark, "Order_Items.csv")

joined = orders.join(items, "Order_ID", "inner")

result = joined.withColumn(
    "Delivered_Carrier_Date",
    to_timestamp("Order_Delivered_Carrier_Date", "yyyy-MM-dd HH:mm")
).withColumn(
    "Shipping_Limit_Timestamp",
    to_timestamp("Shipping_Limit_Date", "yyyy-MM-dd HH:mm")
).filter(
    col("Delivered_Carrier_Date").isNotNull() &
    col("Shipping_Limit_Timestamp").isNotNull()
).withColumn(
    "Delivery_Diff_Days",
    datediff(col("Delivered_Carrier_Date"), col("Shipping_Limit_Timestamp"))
).select(
    "Order_ID",
    "Product_ID",
    "Seller_ID",
    "Delivered_Carrier_Date",
    "Shipping_Limit_Timestamp",
    "Delivery_Diff_Days"
).orderBy(
    desc("Delivery_Diff_Days")
)

result.show(20, truncate=False)
save_result(result, "Bai8_Result.csv", limit=100)

spark.stop()