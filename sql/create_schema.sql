-- Analyst World - reference schema
-- NOTE: bronze_layer.py / silver_layer.py / gold_layer.py auto-create these
-- tables on first run via pandas.to_sql(). This script is provided so you
-- can pre-create the database/tables manually, inspect the intended
-- structure, or hand it to a DBA.

CREATE DATABASE IF NOT EXISTS analyst_world
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE analyst_world;

-- BRONZE: raw scraped data, one row per scraped element, untouched
CREATE TABLE IF NOT EXISTS bronze_scraped_data (
    record_id            VARCHAR(64),
    record_type          VARCHAR(32),   -- table_row | paragraph | link | metadata
    table_index          INT NULL,
    row_index            INT NULL,
    content              JSON,          -- raw scraped payload for this record
    source_url           VARCHAR(2048),
    scraped_at           VARCHAR(64),
    batch_id             VARCHAR(64),
    run_id               VARCHAR(64),
    environment          VARCHAR(16),
    ingestion_timestamp  DATETIME,
    pipeline_layer       VARCHAR(16)
);

-- SILVER: cleaned, tidy long format (one row per field per record)
CREATE TABLE IF NOT EXISTS silver_cleaned_data (
    run_id               VARCHAR(64),
    silver_batch_id      VARCHAR(64),
    record_id            VARCHAR(64),
    record_type          VARCHAR(32),
    table_index          INT NULL,
    row_index            INT NULL,
    field_name           VARCHAR(255),
    field_value_raw      TEXT,
    field_value_clean    TEXT,
    field_value_numeric  DOUBLE NULL,
    is_valid             BOOLEAN,
    source_url           VARCHAR(2048),
    scraped_at           VARCHAR(64),
    cleaned_at           DATETIME
);

-- GOLD: BI-ready wide table, one row per record, dynamic field_* columns
-- (columns beyond the keys below vary per source site and are added
-- automatically by pandas.to_sql on first write / ALTERed on schema drift)
CREATE TABLE IF NOT EXISTS gold_curated_data (
    record_id     VARCHAR(64),
    record_type   VARCHAR(32),
    table_index   INT NULL,
    row_index     INT NULL,
    source_url    VARCHAR(2048),
    scraped_at    VARCHAR(64),
    run_id        VARCHAR(64),
    gold_loaded_at DATETIME
    -- ... plus one column per distinct field_name found on the page
);

-- GOLD: one row per pipeline run, for BI data-quality/freshness dashboards
CREATE TABLE IF NOT EXISTS gold_data_summary (
    run_id                VARCHAR(64),
    source_url            VARCHAR(2048),
    total_records         INT,
    total_fields          INT,
    valid_fields          INT,
    distinct_field_names  INT,
    validity_rate_pct     DOUBLE,
    records_by_type       VARCHAR(1024),
    summary_generated_at  DATETIME
);

-- Audit log for every pipeline run, across all layers
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id          VARCHAR(64),
    run_timestamp   VARCHAR(64),
    source_url      VARCHAR(2048),
    layer           VARCHAR(16),   -- scrape | bronze | silver | gold
    status          VARCHAR(16),   -- success | failed
    row_count       INT,
    message         VARCHAR(2048)
);
