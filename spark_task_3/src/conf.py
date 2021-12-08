import os

SRC_TABLE = os.getenv("SRC_FILE", "public.bank")

TARGET_FILE = os.getenv("TARGET_TABLE", "data/age.parquet")

EXPLAIN_INFO_FILE = os.getenv("EXPLAIN_INFO_FILE", "data/explain.txt")
