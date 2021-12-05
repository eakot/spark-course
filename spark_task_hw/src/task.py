import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from src.conf import SRC_FILE, TARGET_FILE

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .getOrCreate()

df = spark.read \
    .parquet(SRC_FILE)

df_group = df\
    .groupBy("age")\
    .agg(f.count(f.col("age")).alias("count"))

df_group = df_group.sort("age", ascending=False)

df_group.explain()
df_group.show()

df_group.repartition(1).write.mode("overwrite").format("csv").save(TARGET_FILE)