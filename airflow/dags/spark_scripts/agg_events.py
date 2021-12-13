import sys

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def save_to_parquet(load_data_date):

    spark = SparkSession.builder \
        .getOrCreate()

    schema = "event_time timestamp, " \
             "event_type string, " \
             "product_id long, " \
             "category_id long, " \
             "category_code string, " \
             "brand string, " \
             "price float, " \
             "user_id long, " \
             "user_session string"

    df = spark.read \
        .option("sep", ",") \
        .option("inferSchema", "false") \
        .option("header", "false") \
        .schema(schema) \
        .csv(f"/data/{load_data_date}.csv")

    df_views_count = df \
                    .where(f.col("event_type") == "view") \
                    .groupBy("event_time", "category_code") \
                    .agg(f.count("*").alias("views_count")) \
                    .orderBy(f.col("views_count"))

    df_views_count.repartition(1).write.mode("overwrite").format("parquet") \
        .save(f'/data/{load_data_date}_views_count.parquet')

    df_purchase_count = df \
                        .where(f.col("event_type") == "purchase") \
                        .groupBy("event_time", "brand") \
                        .agg(f.count("*").alias("purchase_count")) \
                        .orderBy(f.col("purchase_count"))

    df_purchase_count.repartition(1).write.mode("overwrite").format("parquet") \
        .save(f'/data/{load_data_date}_purchase_count.parquet')


if __name__ == '__main__':
    save_to_parquet(sys.argv[1])
