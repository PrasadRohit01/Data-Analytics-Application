"""
main.py
-------
Analyst World pipeline entry point.

Usage (standalone):
    python main.py --source_url "https://example.com/some-data-page" \
        --mysql_host localhost --mysql_user root --mysql_password secret \
        --mysql_database analyst_world

Usage (Databricks notebook):
    Just run this file's contents as a notebook cell / %run it. WidgetManager
    detects dbutils automatically and renders real widgets at the top of the
    notebook (source_url, environment, mysql_host, ...).

Flow:
    scrape_website(url)  -->  Bronze.run()  -->  Silver.run()  -->  Gold.run()
Every layer writes its own MySQL table before handing the DataFrame to the
next layer, and every run is recorded in `pipeline_run_log`.
"""

import logging
import sys

import bronze_layer
import gold_layer
import silver_layer
from config import WidgetManager
from scraper import ScrapeError, scrape_website
from spark_utils import get_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("analyst_world.main")


def run_pipeline(cli_args=None):
    config = WidgetManager().build(cli_args)
    logger.info("=" * 70)
    logger.info("ANALYST WORLD - run_id=%s | url=%s | env=%s", config.run_id, config.source_url, config.environment)
    logger.info("=" * 70)

    spark = get_spark()

    # ---- Scrape -----------------------------------------------------
    try:
        raw_records = scrape_website(config.source_url)
    except ScrapeError as exc:
        logger.error("Scraping failed: %s", exc)
        import db_utils
        db_utils.log_pipeline_run(config, "scrape", "failed", 0, str(exc))
        sys.exit(1)

    # ---- Bronze -------------------------------------------------------
    bronze_df = bronze_layer.run(spark, raw_records, config)

    # ---- Silver -------------------------------------------------------
    silver_df = silver_layer.run(spark, bronze_df, config)

    # ---- Gold -----------------------------------------------------------
    gold_df, summary_df = gold_layer.run(spark, silver_df, config)

    logger.info("Pipeline complete for run_id=%s", config.run_id)
    logger.info("Gold curated preview:")
    gold_df.show(10, truncate=60)
    logger.info("Gold summary:")
    summary_df.show(truncate=100)

    return {
        "config": config,
        "bronze_df": bronze_df,
        "silver_df": silver_df,
        "gold_df": gold_df,
        "summary_df": summary_df,
    }


if __name__ == "__main__":
    run_pipeline()
