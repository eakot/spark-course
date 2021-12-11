import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from datetime import datetime


def col_filter(col_name, pattern):
    return f.lower(f.col(col_name)).like(f"%{pattern}%")


def load_to_postgres(df_to_load, dbtable):
    (
        df_to_load
            .withColumn("load_datetime", f.lit(datetime.now()))
            .write
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .option("url", "jdbc:postgresql://172.17.0.1:5432/postgres")
            .option("user", "p_user")
            .option("password", "password123")
            .option("dbtable", dbtable)
            .option("fetchsize", 10000)
            .save()
    )


load_date = sys.argv[1]
source_file = f"/data/{load_date}"

spark = SparkSession.builder \
        .getOrCreate()

schema = "date string, " \
        "event_type string, " \
        "product_id long, " \
        "category_id long, " \
        "category_code string, " \
        "brand string, " \
        "price float, " \
        "user_id long, " \
        "user_session string"

initial_df = spark.read \
        .option("sep", ",") \
        .option("header", "false") \
        .option("enforceSchema", "true") \
        .schema(schema) \
        .csv(source_file)

agg_view_by_category_df = initial_df \
    .where(col_filter("event_type", "view")) \
    .groupBy("date", "category_code") \
    .agg(f.count("*").alias("view_count")) \
    .orderBy(f.col("view_count").desc())

load_to_postgres(agg_view_by_category_df, "public.view_by_category")

agg_purchase_by_brand = initial_df \
    .where(col_filter("event_type", "purchase")) \
    .groupBy("date", "brand") \
    .agg(f.count("*").alias("purchase_count")) \
    .orderBy(f.col("purchase_count").desc())

load_to_postgres(agg_purchase_by_brand, "public.purchase_by_brand")

spark.stop()
