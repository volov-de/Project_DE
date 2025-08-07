import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DATA_PATH = f"s3a://startde-raw/raw_items"
TARGET_PATH = f"s3a://startde-project/vj-volov/seller_items"

def _spark_session():
    return (SparkSession.builder
            .appName("SparkJob1-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config('spark.hadoop.fs.s3a.access.key', "r7LX3wSCP5ZK1yXupKEVVG")
            .config('spark.hadoop.fs.s3a.secret.key', "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")
            .getOrCreate())


def main():
    spark = _spark_session()
    orders_df = spark.read.parquet(DATA_PATH)
    orders_df.show()

    # Создание новых столбцов
    orders_df = orders_df.withColumn(
        "returned_items_count",
        F.col("ordered_items_count") - (F.col("ordered_items_count") * F.col("avg_percent_to_sold") / 100)
    ).withColumn(
        "potential_revenue",
        (F.col("availability_items_count") + F.col("ordered_items_count")) * F.col("item_price")
    ).withColumn(
        "total_revenue",
        (F.col("goods_sold_count") - F.col("returned_items_count")) * F.col("item_price")
    ).withColumn(
        "avg_daily_sales",
        F.when(F.col("days_on_sell") > 0, F.col("goods_sold_count") / F.col("days_on_sell")).otherwise(0.0)
    ).withColumn(
        "days_to_sold",
        F.when(F.col("avg_daily_sales") > 0, F.col("availability_items_count") / F.col("avg_daily_sales")).otherwise(0.0)
    ).withColumn(
        "item_rate_percent",
        F.percent_rank().over(Window.orderBy("item_rate"))
    )

    orders_df.show() #Проверка что получилось
    
    orders_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()

    
if __name__ == "__main__":
    main()