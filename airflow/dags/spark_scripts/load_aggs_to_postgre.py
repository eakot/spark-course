import fire
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from datetime import datetime


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


def events_to_agg(df, event_type, group_by):
    agg_col_name = f"{event_type}_count"
    return df \
        .where(f.lower(f.col("event_type")).like(f"%{event_type}%")) \
        .groupBy(*group_by) \
        .agg(f.count("*").alias(agg_col_name)) \
        .orderBy(f.col(agg_col_name).desc())


def events_to_agg_to_postgres(load_date):
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

    events_df = spark.read \
            .option("sep", ",") \
            .option("header", "false") \
            .option("enforceSchema", "true") \
            .schema(schema) \
            .csv(source_file)

    agg_view_by_category_df = events_to_agg(events_df, "view", ["date", "category_code"])
    load_to_postgres(agg_view_by_category_df, "public.view_by_category")

    agg_purchase_by_brand_df = events_to_agg(events_df, "purchase", ["date", "brand"])
    load_to_postgres(agg_purchase_by_brand_df, "public.purchase_by_brand")

    spark.stop()


if __name__ == '__main__':
    fire.Fire(events_to_agg_to_postgres)