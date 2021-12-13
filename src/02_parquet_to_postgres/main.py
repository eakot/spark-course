import fire
from pyspark.sql import SparkSession


def parquet_to_postgres(source_parquet_file, target_tablename, url, login, password, ):

    # jars_path = "jars/postgresql-42.3.1.jar"

    spark = SparkSession.builder \
        .config("spark.jars", "jars/postgresql-42.3.1.jar") \
        .getOrCreate()

    df = spark.read.parquet(source_parquet_file).na.drop('any')

    df.show()
    df.printSchema()

    (df
        .write
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .option("url", url)
        .option("user", login)
        .option("password", password)
        .option("dbtable", target_tablename)
        .option("fetchsize", 10000)
        .save()
    )


if __name__ == '__main__':
    fire.Fire(parquet_to_postgres)

