import fire
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def aggregation_from_postgres(output_file, source_table, url, login, password):

    spark = SparkSession.builder \
        .config("spark.jars", "jars/postgresql-42.3.1.jar") \
        .getOrCreate()

    # Doing aggregation on the Spark side
    connection_properties = {"driver": "org.postgresql.Driver", "user": login, "password": password}
    df_bank = spark.read.jdbc(url=url, table=source_table, properties=connection_properties)

    df_age = df_bank \
        .groupBy("age") \
        .agg(f.count("*").alias("count")) \
        .orderBy("age")

    print("Execution plan (aggregation on the Spark side):")
    df_age.explain()

    # Doing aggregation on the database side
    pushdown_query = f"(SELECT age, COUNT(*) as count from {source_table} GROUP BY age ORDER BY age) sql_agg"
    df_age_sql = spark.read.jdbc(url=url, table=pushdown_query, properties=connection_properties)

    print("Execution plan (aggregation on the database side):")
    df_age_sql.explain()

    # Writing data to a parquet file
    df_age.repartition(1).write.mode("overwrite").format("parquet").save(output_file)


if __name__ == '__main__':
    fire.Fire(aggregation_from_postgres)

