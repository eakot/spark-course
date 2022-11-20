import fire
from pyspark.sql import SparkSession


def csv_to_parquet(source_csv_file: str, target_parquet_dir: str) -> None:
    """
    First Spark's task (except for downloading data) which reads
    the csv file and writes the data to parquet format.
    """
    spark = (SparkSession.builder
        .appName('csv_to_parquet')
        .getOrCreate()
    )

    # Extract
    df = (spark.read
        .format('csv')
        .option('sep', ';')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .load(source_csv_file)
    )
    # Transform
    count_view = df.filter(f.col('event_type') == 'view').count()
    count_purchase = df.filter(f.col('event_type') == 'purchase').count()
    df_load = spark.createDataFrame([{"Count_view": count_view, "Count_purchase": count_purchase}])

    # Load
    (df
        .repartition(1)
        .write
        .mode('overwrite')
        .format('parquet')
        .save(target_parquet_dir)
    )

    spark.stop()


if __name__ == '__main__':
    fire.Fire(csv_to_parquet)
