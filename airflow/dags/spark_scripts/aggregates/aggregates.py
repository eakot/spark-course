from src.conf import CREDENTIALS

import fire
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
from pyspark.sql.types import (
    StructField, StructType, LongType, StringType, DateType
)

# Globals
HOST = CREDENTIALS['host']
PORT = CREDENTIALS['port']
DB = CREDENTIALS['db']
USER = CREDENTIALS['user']
PASSWORD = CREDENTIALS['password']


def cat_count(events_df: DataFrame,
              event_type: str = 'view') -> DataFrame:
    """
    Number of *event_type* by categories (date, category, views_count)
    """
    return (events_df
        .where(f.col('event_type') == event_type)
        .groupBy(f.col('date'), f.col('category_id'))
        .agg(
            f.count('*').alias(event_type + '_count')
        )
    )


def brand_events(events_df: DataFrame,
                 event_type: str = 'purchase') -> DataFrame:
    """
    *Event_type* of brand goods (date, brand, purchase_count)
    """
    return (events_df
        .where(f.col('event_type') == event_type)
        .groupBy(f.col('date'), f.col('brand'))
        .agg(
            f.count('*').alias(event_type + '_count')
        )
    )


def daily_aggregates(source_filename: str) -> None:
    # Init SparkSession
    spark = (SparkSession.builder
        .appName('daily_events_aggregates')
        .getOrCreate()
    )

    # CSV Schema
    schema_events_daily = StructType([
        StructField('event_time', DateType(), True),
        StructField('event_type', StringType(), True),
        StructField('product_id', LongType(), True),
        StructField('category_id', LongType(), True),
        StructField('category_code', StringType(), True),
        StructField('brand', StringType(), True),
        StructField('price', LongType(), True),
        StructField('user_id', LongType(), True),
        StructField('user_session', StringType(), True),
        StructField('date', DateType(), True)
    ])

    # Read daily data
    df_events_daily = (spark.read
        .format('csv')
        .option('delimiter', ',')
        .schema(schema_events_daily)
        .option('header', 'true')
        .option('nullValue', '')
        .load(f'/data/{source_filename}')
    )

    # Compute aggregates
    view_cat_count = cat_count(df_events_daily, event_type='view')
    brand_goods_sales = brand_events(df_events_daily, event_type='purchase')

    # Load to PostgreSQL database
    (view_cat_count.write
        .format('jdbc')
        .option('driver', 'org.postgresql.Driver')
        .mode('append')
        .option('url', f'jdbc:postgresql://{HOST}:{PORT}/{DB}')
        .option('user', USER)
        .option('password', PASSWORD)
        .option('dbtable', 'view_cat_count')
        .option('fetchsize', 10000)
        .save()
    )

    (brand_goods_sales.write
        .format('jdbc')
        .option('driver', 'org.postgresql.Driver')
        .mode('append')
        .option('url', f'jdbc:postgresql://{HOST}:{PORT}/{DB}')
        .option('user', USER)
        .option('password', PASSWORD)
        .option('dbtable', 'brand_goods_sales')
        .option('fetchsize', 10000)
        .save()
    )


if __name__ == '__main__':
    fire.Fire(daily_aggregates)
    