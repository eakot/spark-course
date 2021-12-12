import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import fire

def agg(load_date):

    spark = SparkSession.builder \
        .appName("agg") \
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

    df = spark.read \
        .option("sep", ",") \
        .schema(schema) \
        .csv(f"/data/events_{load_date}.csv")

    df = df \
        .filter(f.col("event_type") == 'purchase') \
        .groupBy("category_code") \
        .agg( f.count("*").alias("purchase_count"))\
        .orderBy(f.col("purchase_count").desc())

    df.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres") \
        .option("user", "p_user") \
        .option("password", "password123") \
        .option("dbtable", "public.events_views") \
        .option("fetchsize", 10000) \
        .save()

    df.repartition(1).write.mode("overwrite").format("parquet").save(f"/data/events_{load_date}.parquet")
    spark.stop()

if __name__ == '__main__':
    fire.Fire(agg)