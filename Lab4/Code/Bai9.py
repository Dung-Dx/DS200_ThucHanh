import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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

spark = SparkSession.builder.appName("Bai9_Customer_Segmentation").getOrCreate()

orders = read_csv(spark, "Orders.csv")
items = read_csv(spark, "Order_Items.csv")
customers = read_csv(spark, "Customer_List.csv")

order_value = items.withColumn(
    "Order_Item_Value",
    col("Price") + col("Freight_Value")
).groupBy(
    "Order_ID"
).agg(
    round(sum("Order_Item_Value"), 2).alias("Order_Value")
)

joined = orders.join(customers, "Customer_Trx_ID", "inner") \
    .join(order_value, "Order_ID", "left")

customer_stats = joined.groupBy("Customer_Trx_ID", "Customer_Country") \
    .agg(
        countDistinct("Order_ID").alias("Total_Orders"),
        round(avg("Order_Value"), 2).alias("Avg_Order_Value"),
        min(to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm")).alias("First_Purchase"),
        max(to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm")).alias("Last_Purchase")
    ) \
    .withColumn("Purchase_Period_Days", datediff(col("Last_Purchase"), col("First_Purchase")) + lit(1)) \
    .withColumn("Purchase_Frequency", round(col("Total_Orders") / col("Purchase_Period_Days"), 4)) \
    .withColumn(
        "Customer_Group",
        when((col("Total_Orders") >= 5) & (col("Avg_Order_Value") >= 200), "VIP")
        .when(col("Total_Orders") >= 3, "Frequent")
        .when(col("Avg_Order_Value") >= 200, "High_Value")
        .otherwise("Normal")
    ) \
    .orderBy(desc("Total_Orders"), desc("Avg_Order_Value"))

customer_stats.show(20, truncate=False)
save_result(customer_stats, "Bai9_Result.csv", limit=200)

spark.stop()