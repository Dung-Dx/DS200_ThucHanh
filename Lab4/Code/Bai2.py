import os
import csv

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import SparkSession

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

spark = SparkSession.builder.appName("Bai2_Total_Statistics").getOrCreate()

orders = read_csv(spark, "Orders.csv")
customers = read_csv(spark, "Customer_List.csv")
items = read_csv(spark, "Order_Items.csv")

summary = spark.createDataFrame([
    ("Tong so don hang", orders.select("Order_ID").distinct().count()),
    ("So luong khach hang", customers.select("Customer_Trx_ID").distinct().count()),
    ("So luong nguoi ban", items.select("Seller_ID").distinct().count()),
], ["Metric", "Value"])

summary.show(truncate=False)
save_result(summary, "Bai2_Result.csv")

spark.stop()