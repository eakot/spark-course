from src.conf import SRC_TABLE, TARGET_FILE
from typing import Callable

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f


def extract_data(spark: SparkSession, SRC_TABLE: str) -> DataFrame:
    return (spark
        .read
        .option('driver', 'org.postgresql.Driver')
        .format('jdbc')
        .option('url', 'jdbc:postgresql://postgresql:5432/postgre')
        .option('dbtable', SRC_TABLE)
        .option('user', 'p_user')
        .option('password', 'password123')
        .load()
    )


def transform_data(df: DataFrame) -> DataFrame:
    return (df
        .groupBy('age')
        .agg(f.count('age').alias('count'))
    )


def save_data(df: DataFrame, TARGET_FILE: str) -> None:
    (df
        .write
        .option('header', 'true')
        .option('delimiter', ';')
        .csv(TARGET_FILE)
    )


def explain_query(extract_transform: Callable) -> Callable:
    """
    Декоратор, позволяющий увидеть физический план запроса для 
    функции, которая агрегирует данные на уровне базы, а затем 
    возвращает уже сагрегированные данные.
    """
    def wrapper(*args, **kwargs):
        return extract_transform(*args, **kwargs).explain()
    return wrapper


@explain_query
def extract_transform(spark: SparkSession, SRC_TABLE: str) -> DataFrame:
    jdbc_url = 'jdbc:postgresql://postgresql:5432/postgre'
    connection_properties = {
        'user': 'p_user',
        'password': 'password123',
        'driver': 'org.postgresql.Driver'
    }
    
    pushdown_query = '(SELECT age, COUNT(age) AS count FROM public.bank GROUP BY age) age_alias'

    return (spark
        .read
        .jdbc(url=jdbc_url, table=pushdown_query, properties=connection_properties)
    )


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

    print('\n')
    print('Агрегация на стороне спарка:\n')
    transform_data(df_bank).explain()  # Агрегация на стороне спарка
    print('\n')

    print('Агрегация на стороне базы:\n')
    extract_transform(spark, SRC_TABLE)
    #extract_transform(spark, SRC_TABLE).explain()  # Агрегация на стороне базы
    print('\n')

    spark.stop()
