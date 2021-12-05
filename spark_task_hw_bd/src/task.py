import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from src.conf import SRC_FILE, TARGET_FILE

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

url = "jdbc:postgresql://postgresql:5432/postgres"

df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", "public.bank") \
    .option("user", "p_user") \
    .option("password", "password123") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.na.drop("any")

df_group = df\
    .groupBy("age")\
    .agg(f.count(f.col("age")).alias("count"))

df_group = df_group.sort("age", ascending=False)
df_group.explain()
df_group.show()

df_group.repartition(1).write.mode("overwrite").format("csv").save(TARGET_FILE)






