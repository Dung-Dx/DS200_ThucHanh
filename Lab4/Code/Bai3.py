import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, desc

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

spark = SparkSession.builder.appName("Bai3_Orders_By_Country").getOrCreate()

orders = read_csv(spark, "Orders.csv")
customers = read_csv(spark, "Customer_List.csv")

order_customer = orders.join(customers, "Customer_Trx_ID", "inner")

result = order_customer.groupBy("Customer_Country") \
    .agg(countDistinct("Order_ID").alias("Total_Orders")) \
    .orderBy(desc("Total_Orders"))

result.show(truncate=False)
save_result(result, "Bai3_Result.csv")

spark.stop()