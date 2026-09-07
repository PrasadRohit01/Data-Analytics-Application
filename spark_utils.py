"""
spark_utils.py
--------------
One place to build/get the SparkSession so every layer shares the same
configuration (and reuses the active session when run inside Databricks).
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "AnalystWorld") -> SparkSession:
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")   # small-data friendly
        .config("spark.driver.memory", "2g")
        .master("local[*]")
        .getOrCreate()
    )
