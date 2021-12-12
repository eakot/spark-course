import fire
from pyspark.sql import SparkSession


def csv_to_parquet(source_csv_file, target_parquet_dir):
    """
    First task on spark (after source data downloaded) is
    to read the csv file and write the data in parquet format.
    """
    spark = SparkSession.builder \
        .appName("parquet_to_postgres") \
        .getOrCreate()

    df = spark.read \
        .option("sep", ";") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(source_csv_file)

    df.repartition(1).write.mode("overwrite").format("parquet").save(target_parquet_dir)


if __name__ == '__main__':
    fire.Fire(csv_to_parquet)
