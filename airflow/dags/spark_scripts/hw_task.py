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
        .option("sep", ";") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv('/data/{load_data}!'.format(load_data=load_data))

    df = df \
        .filter(f.col("event_type") == 'view') \
        .groupBy("category_code") \
        .agg( f.count("event_type").alias("view_count"))\
        .orderBy(f.col("view_count").desc())

    df.repartition(1).write.mode("overwrite").format("parquet").save("/data")

if __name__ == '__main__':
    fire.Fire(csv_to_parquet)

