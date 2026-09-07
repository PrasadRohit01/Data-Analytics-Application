"""
silver_layer.py
-----------------
SILVER = cleaned, conformed data.

Bronze's 'content' column is a JSON blob whose shape depends entirely on
the website scraped (a table row from Site A looks nothing like a table
row from Site B). Rather than guess a fixed wide schema, Silver explodes
every record's JSON payload into a tidy long format:

    record_id | field_name | field_value_raw | field_value_clean | ...

This is the standard trick for "unknown/variable shape source" pipelines:
it's always valid regardless of what the source page looked like, and Gold
can still pivot it back to a wide table when the caller wants one
(see gold_layer.build_wide_curated_table).

Cleaning applied here:
  - trim + collapse whitespace
  - drop null/empty field values
  - drop exact duplicate (record_id, field_name, field_value) rows
  - standardize field_name to snake_case
  - flag numeric-looking values and attach a parsed numeric column
  - flag likely low-quality rows (e.g. nav/boilerplate junk) as is_valid=false
    instead of silently dropping them, so BI/Gold can decide what to do
"""

import logging
import re

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

logger = logging.getLogger("analyst_world.silver")

_NUMERIC_RE = re.compile(r"^-?\d+(\.\d+)?$")


def _snake_case(name: str) -> str:
    if name is None:
        return "unknown_field"
    name = re.sub(r"[^\w]+", "_", name.strip().lower())
    name = re.sub(r"_+", "_", name).strip("_")
    return name or "unknown_field"


snake_case_udf = F.udf(_snake_case, StringType())


def run(spark, bronze_df, config):
    # 1. Parse the JSON 'content' blob into a map, then explode to long format
    parsed = bronze_df.withColumn(
        "content_map", F.from_json(F.col("content"), MapType(StringType(), StringType()))
    )

    exploded = parsed.select(
        "run_id", "record_id", "record_type", "table_index", "row_index",
        "source_url", "scraped_at",
        F.explode_outer("content_map").alias("field_name_raw", "field_value_raw"),
    )

    # 2. Clean
    cleaned = (
        exploded
        .withColumn("field_name", snake_case_udf(F.col("field_name_raw")))
        .withColumn(
            "field_value_clean",
            F.trim(F.regexp_replace(F.coalesce(F.col("field_value_raw"), F.lit("")), r"\s+", " ")),
        )
        .withColumn(
            "field_value_numeric",
            F.when(F.col("field_value_clean").rlike(r"^-?\d+(\.\d+)?$"),
                   F.col("field_value_clean").cast("double")),
        )
        .withColumn(
            "is_valid",
            (F.length(F.col("field_value_clean")) > 0)
            & (~F.col("field_value_clean").isin("N/A", "n/a", "-", "null", "None")),
        )
        .withColumn("silver_batch_id", F.lit(config.run_id))
        .withColumn("cleaned_at", F.current_timestamp())
        .dropDuplicates(["record_id", "field_name", "field_value_clean"])
    )

    silver_df = cleaned.select(
        "run_id", "silver_batch_id", "record_id", "record_type", "table_index", "row_index",
        "field_name", "field_value_raw", "field_value_clean", "field_value_numeric",
        "is_valid", "source_url", "scraped_at", "cleaned_at",
    )

    total = silver_df.count()
    valid = silver_df.filter(F.col("is_valid")).count()
    logger.info("Silver layer complete: %d fields cleaned (%d flagged valid, %d flagged invalid)",
                total, valid, total - valid)

    import db_utils
    silver_df.persist()
    written = db_utils.write_dataframe_to_mysql(
        silver_df, config.silver_table, config, if_exists=(
            "replace" if config.load_mode == "overwrite" else "append"
        )
    )
    db_utils.log_pipeline_run(config, "silver", "success", written)

    return silver_df
