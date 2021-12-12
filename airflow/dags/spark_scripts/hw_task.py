import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import fire

def csv_to_parquet(load_data):
    """
    First task on spark (after source data downloaded) is
    to read the csv file and write the data in parquet format.
    """
    spark = SparkSession.builder \
        .getOrCreate()

    df = spark.read \
        .option("sep", ",") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f'/data/{load_data}'.format(load_data=load_data))

    df = df.toDF('event_time', 'event_type', 'product_id', 'category_id', 'category_code', 'brand', 'price', 'user_id', 'user_session', 'date')

    events_df = df \
        .filter(f.col("event_type") == 'view') \
        .groupBy("category_code") \
        .agg(f.count("event_type").alias("view_count"))\
        .orderBy(f.col("view_count").desc())

    events_df_raspr = df \
        .filter(f.col("event_type") == 'purchase') \
        .select(f.round("price", -1).alias("round_price")) \
        .groupBy("round_price")\
        .agg(f.count("round_price").alias("purchase_count"))\
        .orderBy(f.col("purchase_count").asc())

    events_df.repartition(1).write.mode("overwrite").format("parquet").save(f'/data/{load_data}_agg.parquet'.format(load_data=load_data))

if __name__ == '__main__':
    fire.Fire(csv_to_parquet)

