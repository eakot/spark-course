from pyspark.sql import SparkSession
from src.conf import SRC_TABLE, TARGET_FILE, EXPLAIN_INFO_FILE
import pyspark.sql.functions as f
from contextlib import redirect_stdout

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

url = "jdbc:postgresql://postgresql:5432/postgres"

df = spark.read.format("jdbc")\
        .option("driver", "org.postgresql.Driver")\
        .option("url", url)\
        .option("user", "p_user")\
        .option("password", "password123")\
        .option("dbtable", SRC_TABLE)\
        .option("fetchsize", 10000)\
        .load()

print("\nDataFrame info\n")
df.show()
df.printSchema()

# Save result of aggregate query to file
age = df \
        .groupBy("age")\
        .agg(
            f.count("*").alias("count")
            )\
        .orderBy("age")\

age.repartition(1).write.mode("overwrite").format("parquet").save(TARGET_FILE)

# Explain info
print("\nExecution plan on spark side\n")
ex_plan_spark = df \
                    .groupBy("age")\
                    .agg(
                        f.count("*").alias("count")
                        )\
                    .orderBy("age")\
                    .explain
ex_plan_spark()

print("\nExecution plan on DB side\n")
query = """ (select  age, COUNT(*) as "count"
            from public.bank
            group by age 
            order by age) age_info """

ex_plan_db = spark.read.format("jdbc")\
                .option("driver", "org.postgresql.Driver")\
                .option("url", url)\
                .option("user", "p_user")\
                .option("password", "password123")\
                .option("dbtable", query)\
                .option("fetchsize", 10000)\
                .load()\
                .explain
ex_plan_db()

# Save explain info to file
with open(EXPLAIN_INFO_FILE, "w") as explain_info:
    explain_info.write("Execution plan (explain) on spark side:\n")
    with redirect_stdout(explain_info):
        ex_plan_spark()
    explain_info.write("\nExecution plan (explain) on DB side:\n")
    with redirect_stdout(explain_info):
        ex_plan_db()