"""
config.py
---------
Central configuration for Analyst World.

Handles "widgets" the same way the Bronze layer notebook would in Databricks:
 - If running inside a Databricks notebook, real dbutils widgets are created
   and read (widget values show up as text boxes at the top of the notebook).
 - If running as a plain Python script (local / any orchestrator), the same
   parameters are supplied via command-line arguments or environment
   variables, so the pipeline behaves identically either way.
"""

import argparse
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime


def _get_dbutils():
    """Return the Databricks dbutils object if we are running in a
    Databricks notebook/job, otherwise None."""
    try:
        import IPython
        ip = IPython.get_ipython()
        if ip is not None and "dbutils" in ip.user_ns:
            return ip.user_ns["dbutils"]
    except Exception:
        pass
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark)
    except Exception:
        pass
    return None


class WidgetManager:
    """Creates + reads pipeline parameters as Databricks widgets when
    available, and transparently falls back to argparse/env vars otherwise.
    Call get_widgets() once from main.py to get a populated Config object.
    """

    def __init__(self):
        self.dbutils = _get_dbutils()

    def _widget(self, name, default, label=None):
        if self.dbutils is not None:
            try:
                self.dbutils.widgets.text(name, default, label or name)
                return self.dbutils.widgets.get(name)
            except Exception:
                pass
        return os.environ.get(name.upper(), default)

    def build(self, cli_args=None):
        parser = argparse.ArgumentParser(description="Analyst World pipeline")
        parser.add_argument("--source_url", default=None, help="Website to scrape")
        parser.add_argument("--environment", default="dev", choices=["dev", "test", "prod"])
        parser.add_argument("--mysql_host", default="localhost")
        parser.add_argument("--mysql_port", default="3306")
        parser.add_argument("--mysql_user", default="root")
        parser.add_argument("--mysql_password", default="")
        parser.add_argument("--mysql_database", default="analyst_world")
        parser.add_argument("--load_mode", default="append", choices=["append", "overwrite"])
        args, _ = parser.parse_known_args(cli_args)

        source_url = self._widget("source_url", args.source_url or "")
        environment = self._widget("environment", args.environment)
        mysql_host = self._widget("mysql_host", args.mysql_host)
        mysql_port = self._widget("mysql_port", args.mysql_port)
        mysql_user = self._widget("mysql_user", args.mysql_user)
        mysql_password = self._widget("mysql_password", args.mysql_password)
        mysql_database = self._widget("mysql_database", args.mysql_database)
        load_mode = self._widget("load_mode", args.load_mode)

        if not source_url:
            raise ValueError(
                "source_url is required. Pass --source_url <url>, set the "
                "SOURCE_URL env var, or fill in the 'source_url' widget."
            )

        return Config(
            source_url=source_url,
            environment=environment,
            mysql_host=mysql_host,
            mysql_port=int(mysql_port),
            mysql_user=mysql_user,
            mysql_password=mysql_password,
            mysql_database=mysql_database,
            load_mode=load_mode,
        )


@dataclass
class Config:
    source_url: str
    environment: str = "dev"
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_database: str = "analyst_world"
    load_mode: str = "append"

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    run_timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    # Table names per medallion layer
    bronze_table: str = "bronze_scraped_data"
    silver_table: str = "silver_cleaned_data"
    gold_data_table: str = "gold_curated_data"
    gold_summary_table: str = "gold_data_summary"
    run_log_table: str = "pipeline_run_log"

    @property
    def mysql_url(self) -> str:
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
        )

    @property
    def jdbc_url(self) -> str:
        # Only needed if you switch the writers over to spark.write.jdbc()
        return f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
