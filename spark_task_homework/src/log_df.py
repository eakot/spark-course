from pyspark.sql import SparkSession

import pyspark.sql.functions as f
import textwrap as tw
                                                              
def log_df(df, name, method, wrap=None):
    # explain to str
    if method == "explain":
        log_data = df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "simple")
        # split text by 60 symbols
        if wrap:
            log_data = tw.fill(log_data, width=60)

        return f"Logging {name}\n" + log_data

    if method == "schema":
        log_data = df._jdf.schema().treeString()

        return f"Logging {name}\n" + log_data