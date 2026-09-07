"""
bronze_layer.py
----------------
BRONZE = land the data exactly as scraped, with full audit/lineage columns,
no cleaning, no assumptions. Widgets are read here (this is the pipeline's
entry point) so the whole run is reproducible from a single set of
parameters.
"""

import json
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, StructField, StructType, IntegerType
)

import db_utils

logger = logging.getLogger("analyst_world.bronze")

BRONZE_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("record_type", StringType(), True),
    StructField("table_index", IntegerType(), True),
    StructField("row_index", IntegerType(), True),
    StructField("content", StringType(), True),   # JSON-encoded raw payload
    StructField("source_url", StringType(), True),
    StructField("scraped_at", StringType(), True),
    StructField("batch_id", StringType(), True),
])


def build_bronze_dataframe(spark, raw_records: list, config):
    """Turn the scraper's list[dict] into a Spark DataFrame and stamp it
    with pipeline-level audit columns (run_id, environment, ingestion time).
    """
    if not raw_records:
        logger.warning("No raw records to load into Bronze for run_id=%s", config.run_id)

    rows = []
    for rec in raw_records:
        rows.append({
            "record_id": rec.get("record_id"),
            "record_type": rec.get("record_type"),
            "table_index": rec.get("table_index"),
            "row_index": rec.get("row_index"),
            "content": json.dumps(rec.get("content"), default=str),
            "source_url": rec.get("source_url"),
            "scraped_at": rec.get("scraped_at"),
            "batch_id": rec.get("batch_id"),
        })

    df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    df = (
        df.withColumn("run_id", F.lit(config.run_id))
          .withColumn("environment", F.lit(config.environment))
          .withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("pipeline_layer", F.lit("bronze"))
    )
    return df


def run(spark, raw_records: list, config):
    """Bronze entry point: build the DataFrame, write it to MySQL, log the
    run, and hand the DataFrame back so main.py can pass it straight into
    Silver without a round trip through the DB (the write still happens so
    the raw layer is durably persisted)."""
    bronze_df = build_bronze_dataframe(spark, raw_records, config)
    row_count = bronze_df.count()

    bronze_df.persist()
    written = db_utils.write_dataframe_to_mysql(
        bronze_df, config.bronze_table, config, if_exists=(
            "replace" if config.load_mode == "overwrite" else "append"
        )
    )
    db_utils.log_pipeline_run(config, "bronze", "success", written)

    logger.info("Bronze layer complete: %d records staged from %s", row_count, config.source_url)
    return bronze_df
