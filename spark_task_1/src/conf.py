import os

SRC_FILE = os.getenv("SRC_FILE", "data/src/order/orders.csv.gz")

TARGET_FILE = os.getenv("TARGET_FILE", "data/target/order/orders.parquet")

