"""
app.py
------
A small Flask front end for Analyst World. Fill in a URL (and, if needed,
your MySQL connection details) in the browser, hit "Run pipeline", and it
runs the real scrape -> bronze -> silver -> gold pipeline (main.run_pipeline)
and renders the results using the same bronze/silver/gold layer layout as
the static demo.

Run:
    pip install -r requirements.txt flask
    python app.py
    -> open http://localhost:5000

This is a synchronous, single-user dev server - fine for running the
pipeline yourself locally or in a Databricks cluster web terminal. For a
multi-user deployment, move the pipeline call to a background task queue
and poll for status instead of blocking the request.
"""

import logging
import traceback

from flask import Flask, render_template, request

import db_utils
from main import run_pipeline

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("analyst_world.app")

DEFAULTS = {
    "mysql_host": "localhost",
    "mysql_port": "3306",
    "mysql_user": "root",
    "mysql_password": "",
    "mysql_database": "analyst_world",
}

# Simple in-memory store of the last run's results, keyed by nothing in
# particular - this is a single-user dev tool, not a multi-tenant service.
LAST_RESULT = None


def _record_type_breakdown(bronze_pdf):
    counts = bronze_pdf["record_type"].value_counts().to_dict()
    return sorted(counts.items())


def _build_view_model(result):
    bronze_pdf = result["bronze_df"].toPandas()
    silver_pdf = result["silver_df"].toPandas()
    gold_pdf = result["gold_df"].limit(50).toPandas()
    summary_pdf = result["summary_df"].toPandas()
    summary = summary_pdf.iloc[0].to_dict() if len(summary_pdf) else {}

    gold_columns = [c for c in gold_pdf.columns if c not in ("gold_loaded_at",)]

    return {
        "config": result["config"],
        "bronze_count": len(bronze_pdf),
        "bronze_chips": _record_type_breakdown(bronze_pdf),
        "silver_count": len(silver_pdf),
        "silver_validity": summary.get("validity_rate_pct", 0),
        "gold_count": int(summary.get("total_records", len(gold_pdf))),
        "gold_columns": gold_columns,
        "gold_rows": gold_pdf[gold_columns].to_dict("records"),
        "error": None,
    }


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html", defaults=DEFAULTS, result=LAST_RESULT)


@app.route("/run", methods=["POST"])
def run():
    global LAST_RESULT
    form = request.form
    cli_args = [
        "--source_url", form.get("source_url", "").strip(),
        "--environment", "dev",
        "--mysql_host", form.get("mysql_host", DEFAULTS["mysql_host"]).strip(),
        "--mysql_port", form.get("mysql_port", DEFAULTS["mysql_port"]).strip(),
        "--mysql_user", form.get("mysql_user", DEFAULTS["mysql_user"]).strip(),
        "--mysql_password", form.get("mysql_password", DEFAULTS["mysql_password"]),
        "--mysql_database", form.get("mysql_database", DEFAULTS["mysql_database"]).strip(),
        "--load_mode", "append",
    ]

    defaults = {
        "mysql_host": cli_args[cli_args.index("--mysql_host") + 1],
        "mysql_port": cli_args[cli_args.index("--mysql_port") + 1],
        "mysql_user": cli_args[cli_args.index("--mysql_user") + 1],
        "mysql_password": cli_args[cli_args.index("--mysql_password") + 1],
        "mysql_database": cli_args[cli_args.index("--mysql_database") + 1],
    }

    try:
        result = run_pipeline(cli_args)
        LAST_RESULT = _build_view_model(result)
    except SystemExit:
        LAST_RESULT = {"error": "Couldn't reach or scrape that URL. Check it's correct and publicly reachable."}
    except Exception as exc:
        logger.error("Pipeline run failed: %s\n%s", exc, traceback.format_exc())
        LAST_RESULT = {"error": str(exc)}

    return render_template("index.html", defaults=defaults, result=LAST_RESULT,
                            submitted_url=form.get("source_url", ""))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
