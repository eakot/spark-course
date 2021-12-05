from pyspark.sql import SparkSession
from src.conf import OUTPUT_FILE, TARGET_TABLE
import pyspark.sql.functions as f

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

url = "jdbc:postgresql://postgresql:5432/postgres"
connectionProperties = {"driver": "org.postgresql.Driver", "user": "p_user", "password": "password123"}

# Doing aggregation on the Spark side
df_bank = spark.read.jdbc(url=url, table=TARGET_TABLE, properties=connectionProperties)
df_age = df_bank \
        .groupBy("age") \
        .agg(f.count("*").alias("count")) \
        .orderBy("age")

print("Execution plan (aggregation on the Spark side):")
df_age.explain()

# Doing aggregation on the database side
pushdown_query = f"(SELECT age, COUNT(*) as count from {TARGET_TABLE} GROUP BY age ORDER BY age) sql_agg"
df_age_sql = spark.read.jdbc(url=url, table=pushdown_query, properties=connectionProperties)

print("Execution plan (aggregation on the database side):")
df_age_sql.explain()

# Writing data to a parquet file
df_age.repartition(1).write.mode("overwrite").format("parquet").save(OUTPUT_FILE)
