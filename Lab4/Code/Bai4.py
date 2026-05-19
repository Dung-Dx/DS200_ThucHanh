import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month, countDistinct, col

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

spark = SparkSession.builder.appName("Bai4_Orders_By_Year_Month").getOrCreate()

orders = read_csv(spark, "Orders.csv")

orders_with_date = orders.withColumn(
    "Purchase_Date",
    to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm")
)

result = orders_with_date.groupBy(
    year("Purchase_Date").alias("Year"),
    month("Purchase_Date").alias("Month")
).agg(
    countDistinct("Order_ID").alias("Total_Orders")
).orderBy(
    col("Year").asc(),
    col("Month").desc()
)

result.show(100, truncate=False)
save_result(result, "Bai4_Result.csv")

spark.stop()