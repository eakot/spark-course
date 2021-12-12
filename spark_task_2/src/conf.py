import os

SRC_FILE = os.getenv("SRC_FILE", "data/bank.parquet")

TARGET_TABLE = os.getenv("TARGET_TABLE", "public.bank")

