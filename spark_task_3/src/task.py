from pyspark.sql import SparkSession
from src.conf import SOURCE_TABLE, TARGET_FILE

import pyspark.sql.functions as f

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

url = "jdbc:postgresql://postgresql:5432/postgres"

# Запрос с агрегацией на стороне спарка
(
    spark
        .read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", url)
        .option("dbtable", SOURCE_TABLE)
        .option("user", "p_user")
        .option("password", "password123")
        .load()
        .groupBy("age")
        .agg(f.count("*").alias("row_count"))
        .orderBy("age")
        .explain()
)

# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- Sort [age#0 ASC NULLS FIRST], true, 0
#     +- Exchange rangepartitioning(age#0 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [id=#18]
#        +- HashAggregate(keys=[age#0], functions=[count(1)])
#           +- Exchange hashpartitioning(age#0, 200), ENSURE_REQUIREMENTS, [id=#15]
#              +- HashAggregate(keys=[age#0], functions=[partial_count(1)])
#                 +- Scan JDBCRelation(public.bank) [numPartitions=1] [age#0] PushedAggregates: [], PushedFilters: [], PushedGroupby: [], ReadSchema: struct<age:int>

# Запрос с агрегацией на стороне базы
push_down_query = """
(SELECT age, count(*)
FROM bank
GROUP BY age
ORDER BY age) age_agg
"""

(
    spark
        .read
        .jdbc(url=url,
              table=push_down_query,
              properties={"user": "p_user", "password": "password123", "driver": 'org.postgresql.Driver'})
        .explain()
)
# == Physical Plan ==
#  *(1) Scan JDBCRelation((SELECT age, count(*)
#  FROM bank
#  GROUP BY age
#  ORDER BY age) age_agg) [numPartitions=1] [age#0,count#1L] PushedAggregates: [], PushedFilters: [], PushedGroupby: [], ReadSchema: struct<age:int,count:bigint>
