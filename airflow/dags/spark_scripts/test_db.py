from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder\
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.3.1.jar") \
    .getOrCreate()

url = "jdbc:postgresql://172.17.0.1:5432/postgres"

df = spark.read.format("jdbc")\
        .option("driver", "org.postgresql.Driver")\
        .option("url", url)\
        .option("user", "p_user")\
        .option("password", "password123")\
        .option("dbtable", "public.bank")\
        .option("fetchsize", 10000)\
        .load()

# Save result of aggregate query to file
age = df \
        .groupBy("age")\
        .agg(
            f.count("*").alias("count")
            )\
        .orderBy("age")\

age.repartition(1).write.mode("overwrite").format("parquet").save("/data/test_db.parquet")