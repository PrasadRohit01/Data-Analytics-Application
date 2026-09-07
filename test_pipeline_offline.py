"""
test_pipeline_offline.py
-------------------------
Sanity test: runs scrape -> bronze -> silver -> gold against a local HTML
sample (no network) and stubs out the MySQL writes (no DB server needed),
so you can confirm the whole transform logic works before wiring up a real
MySQL instance.

Run: python test_pipeline_offline.py
"""

import logging
from unittest.mock import patch

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s | %(name)s | %(message)s")

SAMPLE_HTML = """
<html>
<head>
  <title>Sample Product Page</title>
  <meta name="description" content="A sample catalog page for testing">
</head>
<body>
  <p>Welcome to our store. Prices below are updated daily.</p>
  <table>
    <tr><th>Product</th><th>Price</th><th>Stock</th></tr>
    <tr><td>Widget A</td><td>19.99</td><td>42</td></tr>
    <tr><td>Widget B</td><td>29.99</td><td>N/A</td></tr>
    <tr><td>  Widget C  </td><td>9.99</td><td>7</td></tr>
  </table>
  <a href="/product/widget-a">Widget A details</a>
  <a href="/product/widget-b">Widget B details</a>
</body>
</html>
"""


def main():
    import scraper
    import bronze_layer
    import silver_layer
    import gold_layer
    from config import Config
    from spark_utils import get_spark

    config = Config(source_url="https://example-test.local/catalog")

    print("\n--- SCRAPE ---")
    raw_records = scraper.scrape_website(config.source_url, html=SAMPLE_HTML)
    print(f"Scraped {len(raw_records)} raw records")
    for r in raw_records[:3]:
        print(" ", r)

    spark = get_spark("AnalystWorldOfflineTest")

    fake_written = {"count": 0}

    def fake_write(spark_df, table_name, cfg, if_exists="append"):
        n = spark_df.count()
        fake_written["count"] += n
        print(f"[stubbed write] table={table_name} rows={n} if_exists={if_exists}")
        return n

    def fake_log(cfg, layer, status, row_count, message=""):
        print(f"[stubbed log] layer={layer} status={status} rows={row_count} msg={message}")

    with patch("db_utils.write_dataframe_to_mysql", side_effect=fake_write), \
         patch("db_utils.log_pipeline_run", side_effect=fake_log):

        print("\n--- BRONZE ---")
        bronze_df = bronze_layer.run(spark, raw_records, config)
        bronze_df.show(5, truncate=40)

        print("\n--- SILVER ---")
        silver_df = silver_layer.run(spark, bronze_df, config)
        silver_df.show(20, truncate=40)

        print("\n--- GOLD ---")
        gold_df, summary_df = gold_layer.run(spark, silver_df, config)
        print("Gold curated table:")
        gold_df.show(truncate=60)
        print("Gold summary table:")
        summary_df.show(truncate=100)

    print("\nOK - offline pipeline test completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
