# Analyst World

Feed it any URL. It scrapes the page, lands the raw data in a **Bronze**
table, cleans it in **Silver**, and produces BI-ready tables in **Gold** —
all in MySQL, all orchestrated by PySpark.

```
URL --> scraper.py --> Bronze (raw, JSON payload)
                          |
                          v
                       Silver (cleaned, tidy long format: one row per field)
                          |
                          v
                       Gold  (curated wide table + run-level summary table)
```

## Why this shape

Any site you feed in can look completely different from the last one —
different tables, different columns, different everything. So:

- **Bronze** just lands whatever the scraper found (table rows, paragraphs,
  links, meta tags) as JSON, with full lineage (`source_url`, `run_id`,
  `batch_id`, `ingestion_timestamp`). Nothing is thrown away or assumed.
- **Silver** explodes that JSON into a **tidy long format**
  (`record_id, field_name, field_value_clean, is_valid, ...`). This format
  is valid no matter what the source page's shape was, and is where all
  the real cleaning happens: whitespace/junk removal, dedup, snake_case
  field names, numeric parsing, and a per-field `is_valid` quality flag
  (bad values are flagged, not silently dropped, so nothing disappears
  without a trace).
- **Gold** pivots Silver's valid fields back into a normal wide table
  (`gold_curated_data` — one row per scraped record, one column per field)
  plus a `gold_data_summary` table (one row per run: record counts, field
  counts, validity %, breakdown by record type) so BI teams get both the
  data and a data-quality snapshot without extra queries.

Every layer **writes to MySQL before handing off to the next layer**, and
every run — including failures — is logged to `pipeline_run_log`.

## Project layout

```
analyst_world/
  config.py                  # Config dataclass + WidgetManager (Databricks widgets or CLI/env)
  scraper.py                 # requests + BeautifulSoup: URL -> list[dict]
  spark_utils.py             # shared SparkSession
  db_utils.py                # MySQL read/write helpers (pandas + SQLAlchemy/PyMySQL)
  bronze_layer.py            # raw landing
  silver_layer.py            # cleaning
  gold_layer.py              # curated + summary
  main.py                    # orchestrator / entry point
  app.py                     # Flask web UI - wires the real pipeline to a browser interface
  templates/index.html       # Jinja template for app.py
  static/style.css           # shared styling for the UI
  static/logo.svg            # app icon
  demo.html                  # standalone preview - open directly in a browser, no server/install needed
  sql/create_schema.sql      # reference DDL (tables also auto-create on first run)
  test_pipeline_offline.py   # runs the whole pipeline with a local HTML sample, no MySQL/network needed
  requirements.txt
```

## Web interface

Two ways to see it:

1. **`demo.html`** - double-click it (or open it in any browser) straight
   from the unzipped folder. No install, no server. It runs a simulated
   pipeline against sample data so you can see the look and feel right
   away - handy for previewing on a phone or before MySQL is set up.

2. **`app.py`** - the real thing:
   ```bash
   pip install -r requirements.txt
   python app.py
   ```
   then open `http://localhost:5000`. Enter a URL, optionally expand
   "MySQL connection" to point at your database, and hit **Run pipeline** -
   it runs the actual scrape -> bronze -> silver -> gold pipeline and shows
   the real bronze record count, silver validity rate, and a gold-table
   preview, read straight from what `main.run_pipeline()` produced.

The layout mirrors the pipeline itself: three stacked bands - Bronze,
Silver, Gold - in that order, each colored in its own material tone,
each showing that layer's real output.

## Dashboard

Below the three layers is a dashboard for the gold output, with:

- **Category field / Value field** - pick which two gold columns to plot
  (populated from whatever fields were actually found on the scraped page).
- **Chart type** - bar, line, pie, or doughnut.
- **Color palette** - Medallion (bronze/silver/gold), Ocean, Sunset, or Mono.
- **Background theme** - dark (default), slate, or light, via the three dots
  top-right. The choice is remembered on your next visit.

The bronze/silver/gold accent colors stay fixed across all three
background themes - they're the pipeline's identity, not part of the
ambient theme.

## Setup

```bash
pip install -r requirements.txt
```

You need a reachable MySQL server (local, RDS, whatever). Tables are
auto-created on first write; `sql/create_schema.sql` is there if you'd
rather pre-create them or hand it to a DBA.

## Run it

```bash
python main.py \
  --source_url "https://example.com/some-page-with-a-table" \
  --mysql_host localhost \
  --mysql_port 3306 \
  --mysql_user root \
  --mysql_password yourpassword \
  --mysql_database analyst_world \
  --load_mode append
```

`--load_mode overwrite` truncates+replaces each layer's table instead of
appending a new batch — handy while you're iterating.

You can also set `SOURCE_URL`, `MYSQL_HOST`, etc. as environment variables
instead of flags.

### Running inside Databricks

Drop these files into a Databricks Repo and `%run main.py` (or run it as a
notebook/job). `WidgetManager` detects `dbutils` automatically and renders
real notebook widgets (`source_url`, `environment`, `mysql_host`, ...) at
the top — same code, same behavior, no changes needed.

## Test without a live MySQL server

```bash
python test_pipeline_offline.py
```

This scrapes a local HTML sample (no internet needed) and stubs out the
MySQL writes, so you can confirm the scrape -> bronze -> silver -> gold
logic end-to-end before you've even stood up MySQL.

## Querying the output

```sql
-- What did the last run for a given URL produce?
SELECT * FROM gold_curated_data WHERE source_url = '...' ORDER BY gold_loaded_at DESC;

-- Data quality at a glance
SELECT * FROM gold_data_summary ORDER BY summary_generated_at DESC;

-- Full audit trail
SELECT * FROM pipeline_run_log ORDER BY run_timestamp DESC;
```

Point Power BI / Tableau / whatever straight at `gold_curated_data` and
`gold_data_summary` — that's the intended BI-facing surface.

## Known gaps / things to harden before production

- **JavaScript-rendered pages**: the scraper uses `requests` + BeautifulSoup,
  so it only sees server-rendered HTML. For JS-heavy sites, swap
  `fetch_html()` for a headless-browser fetch (Playwright/Selenium) and
  feed the resulting HTML into `scrape_website(url, html=...)` — every
  downstream layer is unaffected.
- **Pagination / multi-page scraping** isn't handled — it scrapes exactly
  the one URL you give it. Wrapping `main.py` in a loop over a list of URLs
  (writing to the same run) is a natural next step.
- **Large-scale writes**: the MySQL write path goes through pandas
  (`toPandas()` + `to_sql`), which is fine for the sub-1M-row / sub-1GB
  scale this is built for. For much bigger volumes, switch
  `db_utils.write_dataframe_to_mysql` to `spark_df.write.jdbc(...)` with
  the `mysql-connector-j` jar on the Spark classpath.
- **Robots.txt / rate limiting / auth-gated pages** aren't handled —
  respect the target site's terms of use before pointing this at it.
- **Gold's wide table has dynamic columns** (one per field found on the
  page) — that's intentional (it adapts to whatever site you scrape) but
  means the column set can change between different `source_url`s that
  land in the same table. Filter by `source_url` when querying.
