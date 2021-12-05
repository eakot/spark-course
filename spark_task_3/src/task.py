from pyspark.sql import SparkSession
from src.conf import OUTPUT_FILE, TARGET_TABLE
import pyspark.sql.functions as f

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

url = "jdbc:postgresql://postgresql:5432/postgres"

df_bank = spark.read \
    .option("driver", "org.postgresql.Driver") \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", TARGET_TABLE) \
    .option("user", "p_user") \
    .option("password", "password123") \
    .load()

df_age = df_bank \
        .groupBy("age") \
        .agg(f.count("*").alias("count")) \
        .orderBy("age")

df_age.explain()
df_age.show()

df_age.write.option("header", True).csv(OUTPUT_FILE)
