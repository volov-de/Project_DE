import io
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Пути к исходным данным и точке выгрузки отчета
DATA_PATH = "s3a://startde-raw/raw_items"
TARGET_PATH = "s3a://startde-project/vj-volov/seller_items"

# Функция для инициализации SparkSession с параметрами доступа к S3
def _spark_session():
    return (
        SparkSession.builder
        .appName("SparkJob1-" + uuid.uuid4().hex)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
        .config("spark.hadoop.fs.s3a.endpoint", "https://hb.bizmrg.com")
        .config("spark.hadoop.fs.s3a.region", "ru-msk")
        .config("spark.hadoop.fs.s3a.access.key", "r7LX3wSCP5ZK1yXupKEVVG")
        .config("spark.hadoop.fs.s3a.secret.key", "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")
        .getOrCreate()
    )

# Основная функция обработки данных
def main():
    spark = _spark_session()
    orders_df = spark.read.parquet(DATA_PATH)
    orders_df.show() #Проверка исходных данных

    # Вычисляем метрики + приводим типы всех колонок под схему витрины

    orders_df = (orders_df.withColumn(
            "returned_items_count",
            F.round(F.col("ordered_items_count") - (F.col("ordered_items_count") * F.col("avg_percent_to_sold") / F.lit(100.0))).cast("int")
        )
        .withColumn(
            "potential_revenue",
            ((F.col("availability_items_count") + F.col("ordered_items_count")) * F.col("item_price")).cast("bigint")
        )
        .withColumn(
            "total_revenue",
            ((F.col("goods_sold_count") - F.col("returned_items_count")) * F.col("item_price")).cast("bigint")
        )
        .withColumn(
            "avg_daily_sales",
            F.when(F.col("days_on_sell") > 0, (F.col("goods_sold_count") / F.col("days_on_sell")).cast("double")).otherwise(F.lit(0.0).cast("double"))
        )
        .withColumn(
            "days_to_sold",
            F.when(F.col("avg_daily_sales") > 0, (F.col("availability_items_count") / F.col("avg_daily_sales")).cast("double")).otherwise(F.lit(0.0).cast("double"))
        )
        .withColumn(
            "item_rate_percent",
            F.percent_rank().over(Window.orderBy(F.col("item_rate"))).cast("double")
        )

        # Явное приведение типов остальных колонок под схему Greenplum
        .withColumn("sku_id", F.col("sku_id").cast("bigint"))
        .withColumn("title", F.col("title").cast("string"))
        .withColumn("category", F.col("category").cast("string"))
        .withColumn("brand", F.col("brand").cast("string"))
        .withColumn("seller", F.col("seller").cast("string"))
        .withColumn("group_type", F.col("group_type").cast("string"))
        .withColumn("country", F.col("country").cast("string"))
        .withColumn("availability_items_count", F.col("availability_items_count").cast("bigint"))
        .withColumn("ordered_items_count", F.col("ordered_items_count").cast("bigint"))
        .withColumn("warehouses_count", F.col("warehouses_count").cast("bigint"))
        .withColumn("item_price", F.col("item_price").cast("bigint"))
        .withColumn("goods_sold_count", F.col("goods_sold_count").cast("bigint"))
        .withColumn("item_rate", F.col("item_rate").cast("double"))
        .withColumn("days_on_sell", F.col("days_on_sell").cast("bigint"))
        .withColumn("avg_percent_to_sold", F.col("avg_percent_to_sold").cast("bigint"))
    )

    orders_df.show() #Проверка что получилось

    orders_df.write.mode("overwrite").parquet(TARGET_PATH) # Выгружаем отчет в S3
    spark.stop()

if __name__ == "__main__":
    main()
