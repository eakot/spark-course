import fire
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pathlib import Path
def csv_to_parquet(source_csv_file: str, target_parquet_dir: str) -> None:

    spark = (SparkSession.builder
        .appName('csv_to_parquet')
        .getOrCreate()
    )

    data_schema = [
        StructField('event_time', TimestampType(), True),
        StructField('event_type', StringType(), True),
        StructField('product_id', IntegerType(), True),
        StructField('category_id', LongType(), True),
        StructField('category_code', StringType(), True),
        StructField('brand', StringType(), True),
        StructField('price', DoubleType(), True),
        StructField('user_id', IntegerType(), True),
        StructField('user_session', StringType(), True),
        StructField('date', DateType(), True)
    ]
    final_struct = StructType(fields=data_schema)

    # Extract
    df = spark.read.csv(
        source_csv_file,
        schema=final_struct,
        sep=',',
        header=False
    )
#        .format('csv')
#        .option('sep', ';')
#        .option('header', 'true')
#        .option('inferSchema', 'true')
#        .load(source_csv_file)

    # Transform
    count_view = (df.filter(f.col('event_type') == 'view')
                  .groupBy(["event_time", "category_code"])
                  .agg(f.count('*').alias('count_view')))
    brand_purchase = (df.filter(f.col('event_type') == 'purchase')
                  .groupBy(["event_time", "brand"])
                  .agg(f.count('*').alias('brand_purchase')))

    filename = Path(source_csv_file).stem.replace('-', '_')
    count_view.show()
    brand_purchase.show()
    # Load
    (count_view
        .repartition(1)
        .write
        .parquet(
            path=str(Path(target_parquet_dir, f"count_view_{filename}.parquet")),
            mode="overwrite"

        )
    )
    (brand_purchase
        .repartition(1)
        .write
        .parquet(
            path=str(Path(target_parquet_dir, f"brand_purchase_{filename}.parquet")),
            mode="overwrite"

        )
    )

    spark.stop()


if __name__ == '__main__':
    fire.Fire(csv_to_parquet)
