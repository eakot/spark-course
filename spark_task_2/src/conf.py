import os

SRC_FILE = os.getenv("SRC_FILE", "data/habr_data.parquet")

TARGET_TABLE = os.getenv("TARGET_TABLE", "public.habr_data")

