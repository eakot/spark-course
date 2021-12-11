import fire
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


def postgres_to_csv(source_tablename, target_csv_file, url, login, password, ):

    jars_path = "/jars/postgresql-42.3.1.jar"

    spark = SparkSession.builder \
        .getOrCreate()

    pushdown_query = "(SELECT age, count(*) AS cnt FROM bank GROUP BY age ORDER BY age) cnt_by_age"

    initial_df = spark \
        .read \
        .option("driver", "org.postgresql.Driver") \
        .format("jdbc") \
        .option("url", url) \
        .option("user", login) \
        .option("password", password) \
        .option("dbtable", source_tablename) \
        .option("fetchsize", 10000) \
        .load()

    pushdown_df = spark \
        .read \
        .option("driver", "org.postgresql.Driver") \
        .format("jdbc") \
        .option("url", url) \
        .option("user", login) \
        .option("password", password) \
        .option("dbtable", pushdown_query) \
        .option("fetchsize", 10000) \
        .load()

    # план запроса с агрегацией на стороне базы
    pushdown_df.explain()

    # план запроса с агрегацией на стороне спарка
    initial_df.groupBy("age").agg(count("age").alias("cnt")).orderBy("age").explain()

    # сохраняем результаты запроса в файл data/age.csv
    (
        pushdown_df
            .coalesce(1)
            .write
            .option("header", "true")
            .option("sep", ";")
            .mode("overwrite")
            .csv(target_csv_file)
    )


if __name__ == '__main__':
    fire.Fire(postgres_to_csv)
