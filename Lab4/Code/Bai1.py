import os
import csv
from pyspark.sql import SparkSession

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

spark = SparkSession.builder.appName("Bai1_Read_All_CSV").getOrCreate()

orders = read_csv(spark, "Orders.csv")
customers = read_csv(spark, "Customer_List.csv")
items = read_csv(spark, "Order_Items.csv")
reviews = read_csv(spark, "Order_Reviews.csv")
products = read_csv(spark, "Products.csv")

for name, df in [
    ("Orders", orders),
    ("Customer_List", customers),
    ("Order_Items", items),
    ("Order_Reviews", reviews),
    ("Products", products),
]:
    print("\n==============================")
    print(name)
    print("==============================")
    df.printSchema()
    df.show(5, truncate=False)
    save_result(df, f"Bai1_{name}_Preview.csv", limit=10)

spark.stop()