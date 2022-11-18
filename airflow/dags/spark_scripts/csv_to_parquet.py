import fire

import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pathlib import Path

def csv_to_parquet(source_csv_file: str=None, target_parquet_dir: str=None) -> None:

    spark = (
        SparkSession.builder
            .appName('csv_to_parquet')
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = t.StructType(
        [
            t.StructField("date", t.DateType()),
            t.StructField("action", t.StringType()),
            t.StructField("id", t.StringType()),
            t.StructField("id2", t.StringType()),
            t.StructField("category", t.StringType()),
            t.StructField("brand", t.StringType()),
            t.StructField("price", t.DecimalType()),
            t.StructField("id3", t.StringType()),
            t.StructField("id4", t.StringType()),
            t.StructField("date2", t.DateType()),
        ]
    )

    df = spark.read.csv(
        source_csv_file,
        schema=schema,
        sep=',',
        header=False
    )
    df.printSchema()

    # количество просмотров по категориям (date, category, views_count)
    category_views = (
        df.where(f.col('action') == 'view')
        .groupBy(['date', 'category'])
        .agg(f.count("*").alias("views_count"))
    )
    # продажи товаров брендов (date, brand, purchase_count)
    brand_purchases = (
        df.where(f.col('action') == 'purchase')
        .groupBy(['date', 'brand'])
        .agg(f.count("*").alias("purchase_count"))
    )

    filename = Path(source_csv_file).stem.replace('-', '')
    # Load
    (
        category_views
        .coalesce(1)
        .write
        .parquet(
            path=str(Path(target_parquet_dir, f"category_views_{filename}.parquet")),
            mode='overwrite'
        )
    )

    (
        brand_purchases
        .coalesce(1)
        .write
        .parquet(
            path=str(Path(target_parquet_dir, f"brand_purchases_{filename}.parquet")),
            mode='overwrite'
        )
    )

    spark.stop()


if __name__ == '__main__':
    fire.Fire(csv_to_parquet)
