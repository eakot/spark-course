from pyspark.sql import SparkSession
from src.conf import SRC_FILE, TARGET_FILE

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .getOrCreate()

df = spark.read \
    .option("sep", ";") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(SRC_FILE)

df.repartition(1).write.mode("overwrite").format("parquet").save(TARGET_FILE)
