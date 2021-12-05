import os

SOURCE_TABLE = os.getenv("SOURCE_TABLE", "public.habr_data")

TARGET_FILE = os.getenv("TARGET_FILE", "data/habr_data.parquet")
