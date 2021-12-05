import os

OUTPUT_FILE = os.getenv("OUTPUT_FILE", "data/bank_age.parquet")

TARGET_TABLE = os.getenv("TARGET_TABLE", "public.bank")

