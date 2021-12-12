from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from src.conf import TARGET_FILE, SRC_TABLE

spark = SparkSession.builder \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

properties = {"user": "p_user", "password": "password123", "driver": "org.postgresql.Driver"}
# url = "jdbc:postgresql://localhost:5432/postgres"
url = "jdbc:postgresql://postgresql:5432/postgres"

# Делаем преобразования на стороне Spark
data = spark.read.jdbc(url=url, table=SRC_TABLE, properties=properties)
data_agg = data.groupBy("age")\
           .agg(count('*') .alias('num_of_person'))\
           .orderBy("age")

print("План запроса с агрегацией на стороне Spark:\n")
data_agg.explain()

# Делаем преобразования на стороне БД
sql_query = f"(SELECT age, COUNT(*) AS num_persons FROM {SRC_TABLE} GROUP BY age ORDER BY age) sql_agg"
data_from_sql = spark.read.jdbc(url=url, table=sql_query, properties=properties)

print("План запроса с агрегацией на стороне БД:\n")
data_from_sql.explain()

data_agg.repartition(1).write.mode("overwrite").format("parquet").save(TARGET_FILE)
