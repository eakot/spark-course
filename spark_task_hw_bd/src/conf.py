import os

SRC_FILE = os.getenv("SRC_FILE", "postgres.bank")

TARGET_FILE = os.getenv("TARGET_FILE", "data/src/order/orders.csv.gz")

