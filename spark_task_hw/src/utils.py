import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType

orders_schema = "order_id INTEGER, timestamp TIMESTAMP, symbol STRING, volume INTEGER"

symbol_parsed_schema = StructType([
    StructField("prefix", StringType(), False),
    StructField("symbol", StringType(), False)
])


def parse_symbol_raw(symbol_raw):
    prefix = symbol_raw[:3]
    symbol = symbol_raw[3:9]
    return prefix, symbol


parse_symbol_raw_udf = f.udf(parse_symbol_raw, symbol_parsed_schema)
