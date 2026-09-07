"""
db_utils.py
-----------
MySQL persistence helpers shared by the Bronze / Silver / Gold layers.

We move data between Spark and MySQL via pandas (toPandas / read_sql).
For the data volumes this project targets (single-page scrapes, well under
1M rows) that's simpler and more portable than shipping the MySQL JDBC
driver jar around, while still giving each layer a real, queryable table
that BI tools can connect to directly.

If you later need to push much bigger volumes straight from Spark, swap
write_dataframe_to_mysql()'s body for spark_df.write.jdbc(...) using
Config.jdbc_url and the mysql-connector-j jar on the Spark classpath -
every call site above this function stays the same.
"""

import json
import logging

import pandas as pd
import pymysql
from sqlalchemy import create_engine, text

logger = logging.getLogger("analyst_world.db")


def get_engine(config):
    return create_engine(config.mysql_url, pool_pre_ping=True)


def ensure_database_exists(config):
    """Create the target database if it doesn't exist yet (idempotent)."""
    conn = pymysql.connect(
        host=config.mysql_host,
        port=config.mysql_port,
        user=config.mysql_user,
        password=config.mysql_password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{config.mysql_database}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        conn.commit()
    finally:
        conn.close()


def _stringify_complex_columns(df: pd.DataFrame) -> pd.DataFrame:
    """MySQL has no native struct/array type - JSON-encode any column that
    holds dict/list values (e.g. Bronze's 'content' column) before writing."""
    df = df.copy()
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, (dict, list))).any():
            df[col] = df[col].apply(lambda v: json.dumps(v, default=str) if v is not None else None)
    return df


def write_dataframe_to_mysql(spark_df, table_name: str, config, if_exists: str = "append"):
    """Write a Spark DataFrame to a MySQL table.

    if_exists: 'append' | 'replace' | 'fail'  (pandas.to_sql semantics)
    """
    ensure_database_exists(config)
    engine = get_engine(config)

    pdf = spark_df.toPandas()
    pdf = _stringify_complex_columns(pdf)

    pdf.to_sql(table_name, con=engine, if_exists=if_exists, index=False, chunksize=1000)
    logger.info("Wrote %d rows to MySQL table '%s' (if_exists=%s)", len(pdf), table_name, if_exists)
    return len(pdf)


def read_table_as_pandas(table_name: str, config, where_clause: str = None) -> pd.DataFrame:
    ensure_database_exists(config)
    engine = get_engine(config)
    query = f"SELECT * FROM `{table_name}`"
    if where_clause:
        query += f" WHERE {where_clause}"
    return pd.read_sql(text(query), con=engine)


def log_pipeline_run(config, layer: str, status: str, row_count: int, message: str = ""):
    """Append a row to the pipeline_run_log table so every run is auditable
    (source url, run id, per-layer row counts, success/failure)."""
    ensure_database_exists(config)
    engine = get_engine(config)
    log_row = pd.DataFrame([{
        "run_id": config.run_id,
        "run_timestamp": config.run_timestamp,
        "source_url": config.source_url,
        "layer": layer,
        "status": status,
        "row_count": row_count,
        "message": message,
    }])
    log_row.to_sql(config.run_log_table, con=engine, if_exists="append", index=False)
