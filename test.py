import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = (SparkSession.builder
    .config("spark.driver.memory", "2G") \
    .appName('spark_test_task')\
    .getOrCreate()
)

date = sys.argv[1]

schema = "event_time timestamp, " \
     "event_type string, " \
     "product_id long, " \
     "category_id long, " \
     "category_code string, " \
     "brand string, " \
     "price float, " \
     "user_id long, " \
     "user_session string"

df = spark.read\
    .schema(schema) \
    .csv('/data/' + date)

df.show()

df_views_count = df\
        .where(f.col("event_type") == "view")\
        .groupBy("event_time", "category_code")\
        .agg(f.count("*").alias("views_count"))

df_views_count.show()

df_views_count.repartition(1)\
        .write.mode("overwrite")\
        .format("parquet")\
        .save('/data/' + date + '_views_count.parquet')


df_purchase_count = df \
        .where(f.col("event_type") == "purchase") \
        .groupBy("event_time", "brand") \
        .agg(f.count("*").alias("purchase_count")) \
        .orderBy(f.col("purchase_count"))

df_purchase_count.repartition(1)\
        .write.mode("overwrite")\
        .format("parquet") \
        .save('/data/' + date + '_purchase_count.parquet')
