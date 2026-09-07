"""
gold_layer.py
--------------
GOLD = business/BI-ready data. Two tables are produced:

1. gold_curated_data   - Silver's tidy long format pivoted back to one row
                          per scraped record with one column per field, i.e.
                          exactly what a BI tool wants to point a dashboard
                          at (only rows/fields Silver flagged as valid are
                          included).

2. gold_data_summary   - one row per pipeline run: row counts, field/column
                          counts, validity rate, per-record-type breakdown.
                          Gives BI teams (and you) an at-a-glance data
                          quality / freshness view without touching the
                          detail table.
"""

import logging

from pyspark.sql import functions as F

import db_utils

logger = logging.getLogger("analyst_world.gold")


def build_wide_curated_table(silver_df):
    """Pivot Silver's long (record_id, field_name, field_value_clean) format
    back into one row per record with a column per field - the shape BI
    tools and analysts expect."""
    valid_only = silver_df.filter(F.col("is_valid"))

    keys = valid_only.select(
        "record_id", "record_type", "table_index", "row_index",
        "source_url", "scraped_at", "run_id",
    ).dropDuplicates(["record_id"])

    pivoted = (
        valid_only
        .groupBy("record_id")
        .pivot("field_name")
        .agg(F.first("field_value_clean"))
    )

    wide = keys.join(pivoted, on="record_id", how="left")
    return wide.withColumn("gold_loaded_at", F.current_timestamp())


def build_summary_table(silver_df, config):
    """One profiling row for this run: overall + per-record-type stats."""
    overall = silver_df.agg(
        F.countDistinct("record_id").alias("total_records"),
        F.count("*").alias("total_fields"),
        F.sum(F.col("is_valid").cast("int")).alias("valid_fields"),
        F.countDistinct("field_name").alias("distinct_fields"),
    ).collect()[0]

    total_fields = overall["total_fields"] or 0
    valid_fields = overall["valid_fields"] or 0
    validity_rate = round((valid_fields / total_fields) * 100, 2) if total_fields else 0.0

    by_type = (
        silver_df.groupBy("record_type")
        .agg(F.countDistinct("record_id").alias("record_count"))
        .collect()
    )
    by_type_str = ", ".join(f"{r['record_type']}={r['record_count']}" for r in by_type)

    spark = silver_df.sparkSession
    summary_df = spark.createDataFrame([{
        "run_id": config.run_id,
        "source_url": config.source_url,
        "total_records": overall["total_records"] or 0,
        "total_fields": total_fields,
        "valid_fields": valid_fields,
        "distinct_field_names": overall["distinct_fields"] or 0,
        "validity_rate_pct": validity_rate,
        "records_by_type": by_type_str,
    }])
    return summary_df.withColumn("summary_generated_at", F.current_timestamp())


def run(spark, silver_df, config):
    wide_df = build_wide_curated_table(silver_df)
    summary_df = build_summary_table(silver_df, config)

    wide_count = wide_df.count()
    wide_df.persist()

    written_data = db_utils.write_dataframe_to_mysql(
        wide_df, config.gold_data_table, config, if_exists=(
            "replace" if config.load_mode == "overwrite" else "append"
        )
    )
    written_summary = db_utils.write_dataframe_to_mysql(
        summary_df, config.gold_summary_table, config, if_exists="append"
    )

    db_utils.log_pipeline_run(config, "gold", "success", written_data)

    logger.info(
        "Gold layer complete: %d curated rows written to '%s', summary written to '%s'",
        wide_count, config.gold_data_table, config.gold_summary_table,
    )
    return wide_df, summary_df
