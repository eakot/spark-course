from src.conf import SRC_TABLE, TARGET_FILE
#from typing import Callable

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f


def extract_data(spark: SparkSession, SRC_TABLE: str) -> DataFrame:
    return (spark
        .read
        .format('jdbc')
        .option('driver', 'org.postgresql.Driver')
        .option('url', 'jdbc:postgresql://postgresql:5432/postgre')
        .option('user', 'p_user')
        .option('password', 'password123')
        .option('dbtable', SRC_TABLE)
        .option('fetchsize', 10000)
        .load()
    )


def transform_data(df: DataFrame) -> DataFrame:
    return (df
        .groupBy('age')
        .agg(f.count('age').alias('count'))
    )


def save_data(df: DataFrame, TARGET_FILE: str) -> None:
    (df
        .coalesce(1)

        .write
        .format('csv')
        .option('header', 'true')
        .option('delimiter', ';')
        .option('mode', 'overwrite')
        .save(TARGET_FILE)
    )


# def explain_query(extract_transform: Callable) -> Callable:
#     """
#     Декоратор, позволяющий увидеть физический план запроса для 
#     функции, которая агрегирует данные на уровне базы, а затем 
#     возвращает уже сагрегированные данные.
#     """
#     def wrapper(*args, **kwargs):
#         return extract_transform(*args, **kwargs).explain()
#     return wrapper


# @explain_query
def extract_transform(spark: SparkSession, SRC_TABLE: str) -> DataFrame:
    pushdown_query = f'''(
        SELECT age, 
               COUNT(age) AS count 
          FROM {SRC_TABLE} 
         GROUP BY age
    ) age_alias'''

    return (spark
        .read
        .format('jdbc')
        .option('driver', 'org.postgresql.Driver')
        .option('url', 'jdbc:postgresql://postgresql:5432/postgre')
        .option('user', 'p_user')
        .option('password', 'password123')
        .option('dbtable', pushdown_query)
        .option('fetchsize', 10000)
        .load()
    )


def write_logs(logger, df: DataFrame) -> None:
    logging = (
        'Query Physical Plan:\n' + 
        df._sc._jvm.PythonSQLUtils.explainString(
            df._jdf.queryExecution(), 'simple'
        ) +
        '\n'
    )
    logger.info(logging)


if __name__ == '__main__':
    spark = (SparkSession
        .builder
        .config('spark.driver.memory', '2G')
        .config('spark.jars', 'jars/postgresql-42.3.1.jar')
        .appName('age_distr')
        .getOrCreate()
    )

    df_bank = extract_data(spark, SRC_TABLE)
    df_age = transform_data(df_bank)
    save_data(df_age, TARGET_FILE)

    # Если выводим логи на консоль
    # print('\n')
    # print('Агрегация на стороне спарка:\n')
    # transform_data(df_bank).explain()  # Агрегация на стороне спарка
    # print('\n')

    # print('Агрегация на стороне базы:\n')
    # extract_transform(spark, SRC_TABLE)  # Агрегация на стороне базы
    # print('\n')

    # Если хотим писать логи в файл
    logger.add('logs/logs.log')

    # Пишем логи для агрегации на стороне спарка
    write_logs(logger, df_age)

    # Пишем логи для агрегации на стороне базы
    df_age = extract_transform(spark, SRC_TABLE)
    write_logs(logger, df_age)

    spark.stop()
