import fire
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def events_aggregation(load_date):

    spark = SparkSession.builder \
        .appName("events_aggregation") \
        .getOrCreate()

    schema = "event_time timestamp, " \
             "event_type string, " \
             "product_id long, " \
             "category_id long, " \
             "category_code string, " \
             "brand string, " \
             "price float, " \
             "user_id long, " \
             "user_session string, " \
             "date date"

    # Reading to dataframe
    df_events = spark.read \
        .option("sep", ",") \
        .schema(schema) \
        .csv(f"/data/events_{load_date}.csv")

    # Amount of categories views for a particular date
    df_views = df_events \
        .where(f.col("event_type") == "view") \
        .groupBy("date", "category_code") \
        .agg(f.count("*").alias("views_count")) \
        .orderBy(f.col("views_count").desc())

    # Amount of brands sales for a particular date
    df_purchase = df_events \
        .where(f.col("event_type") == "purchase") \
        .groupBy("date", "brand") \
        .agg(f.count("*").alias("purchase_count")) \
        .orderBy(f.col("purchase_count").desc())

    events_type = ["views", "purchase"]

    df_views.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres") \
        .option("user", "p_user") \
        .option("password", "password123") \
        .option("dbtable", "public.events_views") \
        .option("fetchsize", 10000) \
        .save()

    df_purchase.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres") \
        .option("user", "p_user") \
        .option("password", "password123") \
        .option("dbtable", "public.events_purchase") \
        .option("fetchsize", 10000) \
        .save()

    df_views.repartition(1).write.mode("overwrite").format("parquet").save(f"/data/categories_views_{load_date}.parquet")
    df_purchase.repartition(1).write.mode("overwrite").format("parquet").save(f"/data/brands_sales_{load_date}.parquet")

    spark.stop()


if __name__ == '__main__':
    fire.Fire(events_aggregation)
