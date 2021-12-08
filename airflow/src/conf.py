import os

SAVED_TABLE = os.getenv("SAVED_TABLE", "data/age.csv")
SAVED_TABLE_SQL = os.getenv("SAVED_TABLE", "data/age_sql.csv")
TARGET_TABLE = os.getenv("TARGET_TABLE", "public.habr_data")


