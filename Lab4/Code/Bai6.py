import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, sum, round, desc

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

spark = SparkSession.builder.appName("Bai6_Revenue_2024_By_Category").getOrCreate()

orders = read_csv(spark, "Orders.csv")
items = read_csv(spark, "Order_Items.csv")
products = read_csv(spark, "Products.csv")

orders_2024 = orders.withColumn(
    "Purchase_Date",
    to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm")
).filter(year("Purchase_Date") == 2024)

joined = orders_2024.join(items, "Order_ID", "inner") \
    .join(products, "Product_ID", "left")

result = joined.withColumn(
    "Revenue",
    col("Price") + col("Freight_Value")
).groupBy(
    "Product_Category_Name"
).agg(
    round(sum("Revenue"), 2).alias("Total_Revenue_2024")
).orderBy(
    desc("Total_Revenue_2024")
)

result.show(100, truncate=False)
save_result(result, "Bai6_Result.csv")

spark.stop()