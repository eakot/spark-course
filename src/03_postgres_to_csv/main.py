import fire
from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count


def write_logs(logger, df: DataFrame) -> None:
    """
    Function which writes logs.
    """
    logging = (
        'Query Physical Plan:\n' + 
        df._sc._jvm.PythonSQLUtils.explainString(
            df._jdf.queryExecution(), 'simple'
        ) +
        '\n'
    )
    logger.info(logging)


def postgres_to_csv(source_tablename: str, target_csv_dir: str,
                    url: str, login: str, password: str) -> None:
    """
    Third Spark's task (except for downloading data) which reads
    the postgres db data and writes the data to csv  file.  Also 
    shows some logs.
    """
    jars_path = 'jars/postgresql-42.3.1.jar'
    spark = (SparkSession.builder
        .config('spark.jars', jars_path)
        .appName('postgres_to_csv')
        .getOrCreate()
    )

    # Extract
    df_spark = (spark.read
        .format('jdbc')
        .option('driver', 'org.postgresql.Driver')
        .option('url', url)
        .option('user', login)
        .option('password', password)
        .option('dbtable', source_tablename)
        .option('fetchsize', 10000)
        .load()
    )

    # Transform
    df_spark_age = (df_spark
        .groupBy('age')
        .agg(count('age').alias('count'))
    )

    # Load
    (df_spark_age
        .repartition(1)

        .write
        .format('csv')
        .option('header', 'true')
        .option('delimiter', ';')
        .option('mode', 'overwrite')
        .save(target_csv_dir)
    )

    # Extract -> Transform
    pushdown_query = f'''(
        SELECT age, 
               COUNT(age) AS count 
          FROM {source_tablename} 
         GROUP BY age
    ) age_alias'''

    df_postgre = (spark.read
        .format('jdbc')
        .option('driver', 'org.postgresql.Driver')
        .option('url', url)
        .option('user', login)
        .option('password', password)
        .option('dbtable', pushdown_query)
        .option('fetchsize', 10000)
        .load()
    )

    # Write logs
    logger.add('logs/logs.log')
    write_logs(logger, df_spark_age)
    write_logs(logger, df_postgre)

    spark.stop()


if __name__ == '__main__':
    fire.Fire(postgres_to_csv)
    