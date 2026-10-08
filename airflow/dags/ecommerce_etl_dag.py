"""Daily ETL, one Airflow task per step:

    extract -> validate -> clean -> load -> db_checks

Tasks pass data to each other through small pickle files in
data/staging/<run_id>/ (DataFrames are too big/awkward for XCom).
Only tiny summaries (row counts) go through XCom.

Manual run options: trigger with config {"skip_api": true} to skip the FX API.
"""
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.param import Param

PROJECT_DIR = Path("/opt/airflow/project")
STAGING_ROOT = PROJECT_DIR / "data" / "staging"
SOURCE_NAME = "product_price_updates.csv"

# make `import src...` work inside the Airflow container
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

log = logging.getLogger(__name__)


# ---------------------------------------------------------------- helpers

def _prepare() -> None:
    """The pipeline code uses relative paths (data/raw, sql/...), so run from the project root."""
    os.chdir(PROJECT_DIR)


def _staging_dir(run_id: str) -> Path:
    d = STAGING_ROOT / run_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _known_product_ids(conn) -> set:
    with conn.cursor() as cur:
        cur.execute("SELECT product_id FROM products")
        return {r[0] for r in cur.fetchall()}


def _log_failure(context) -> None:
    """Runs only when a task has failed for good (all retries used).
    Writes a 'failed' row to etl_run_log with rows_loaded = 0 (the load was rolled back)."""
    try:
        _prepare()
        from src.loading.load import log_run
        from src.utils.db import connect

        run_id = context["ts_nodash"]
        dag_run = context.get("dag_run")
        started = dag_run.start_date if dag_run and dag_run.start_date else datetime.now(timezone.utc)
        task_id = context["task_instance"].task_id
        message = f"task {task_id} failed: {context.get('exception')}"[:500]

        with connect() as conn:
            log_run(conn, run_id, started, "failed", 0, 0, message)
            conn.commit()
    except Exception:  # never hide the real failure behind a logging problem
        log.exception("could not write the failed run to etl_run_log")


default_args = {
    "owner": "muneeb",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _log_failure,
}


# ---------------------------------------------------------------- DAG

@dag(
    dag_id="ecommerce_etl",
    description="Price updates + FX rates ETL into PostgreSQL",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    params={"skip_api": Param(False, type="boolean", description="Skip the currency API")},
    tags=["ecommerce", "etl"],
)
def ecommerce_etl():

    @task
    def extract(ts_nodash=None, params=None) -> dict:
        _prepare()
        from src.ingestion.extract import extract_fx_rates, extract_price_updates, fx_payload_to_frame
        from src.loading.load import ensure_tables
        from src.utils.db import connect

        # make sure the ETL tables exist (also needed by the failure logger)
        with connect() as conn:
            ensure_tables(conn)
            conn.commit()

        staging = _staging_dir(ts_nodash)

        prices_raw = extract_price_updates()
        prices_raw.to_pickle(staging / "prices_raw.pkl")
        log.info("extracted %d price-update rows from CSV", len(prices_raw))

        fx_rows = 0
        if not params.get("skip_api", False):
            fx = fx_payload_to_frame(extract_fx_rates())  # also saves the raw JSON in data/raw
            fx.to_pickle(staging / "fx.pkl")
            fx_rows = len(fx)
            log.info("extracted %d FX rates from API", fx_rows)

        return {"price_rows": len(prices_raw), "fx_rows": fx_rows}

    @task
    def validate(ts_nodash=None) -> dict:
        _prepare()
        from src.transformation.validate import validate_fx_rates, validate_price_updates
        from src.utils.db import connect

        staging = _staging_dir(ts_nodash)
        prices_raw = pd.read_pickle(staging / "prices_raw.pkl")

        with connect() as conn:
            known_ids = _known_product_ids(conn)

        # CSV problems are only reported: the clean step rejects bad rows, it does not stop the run.
        csv_issues = validate_price_updates(prices_raw, known_ids)
        for issue in csv_issues:
            log.warning("CSV check failed: %s", issue)
        if any(i.check == "missing_columns" for i in csv_issues):
            raise RuntimeError("CSV is missing required columns, cannot continue")

        # FX problems stop the run before anything is loaded.
        fx_issue_count = 0
        fx_path = staging / "fx.pkl"
        if fx_path.exists():
            fx_issues = validate_fx_rates(pd.read_pickle(fx_path))
            for issue in fx_issues:
                log.warning("FX check failed: %s", issue)
            if fx_issues:
                raise RuntimeError("FX data failed validation, not loading it")
            fx_issue_count = len(fx_issues)

        return {"csv_issues": len(csv_issues), "fx_issues": fx_issue_count}

    @task
    def clean(ts_nodash=None) -> dict:
        _prepare()
        from src.transformation.clean import clean_price_updates
        from src.utils.db import connect

        staging = _staging_dir(ts_nodash)
        prices_raw = pd.read_pickle(staging / "prices_raw.pkl")

        with connect() as conn:
            known_ids = _known_product_ids(conn)

        clean_df, rejected_df = clean_price_updates(prices_raw, known_ids)
        clean_df.to_pickle(staging / "clean.pkl")
        rejected_df.to_pickle(staging / "rejected.pkl")
        log.info("clean=%d rejected=%d", len(clean_df), len(rejected_df))

        return {"clean": len(clean_df), "rejected": len(rejected_df)}

    @task
    def load(ts_nodash=None) -> dict:
        _prepare()
        from src.loading.load import load_fx_rates, load_price_history, load_rejected
        from src.utils.db import connect

        staging = _staging_dir(ts_nodash)
        clean_df = pd.read_pickle(staging / "clean.pkl")
        rejected_df = pd.read_pickle(staging / "rejected.pkl")

        with connect() as conn:
            try:
                # one transaction: either everything is loaded or nothing is
                prices = load_price_history(conn, clean_df)
                rejected = load_rejected(conn, rejected_df, SOURCE_NAME, ts_nodash)

                fx = 0
                fx_path = staging / "fx.pkl"
                if fx_path.exists():
                    fx = load_fx_rates(conn, pd.read_pickle(fx_path))

                conn.commit()
            except Exception:
                conn.rollback()
                raise

        log.info("loaded prices=%d fx=%d rejected=%d", prices, fx, rejected)
        return {"prices": prices, "fx": fx, "rejected": rejected}

    @task
    def db_checks(load_result: dict, ts_nodash=None, dag_run=None) -> None:
        _prepare()
        from src.loading.load import log_run
        from src.transformation.validate import run_db_checks
        from src.utils.db import connect

        started = dag_run.start_date if dag_run and dag_run.start_date else datetime.now(timezone.utc)

        with connect() as conn:
            db_issues = run_db_checks(conn)
            for issue in db_issues:
                log.warning("DB check failed: %s", issue)
            if not db_issues:
                log.info("all database quality checks passed")

            loaded = load_result["prices"] + load_result["fx"]
            message = f"prices={load_result['prices']}, fx={load_result['fx']}, db_issues={len(db_issues)}"
            log_run(conn, ts_nodash, started, "success", loaded, load_result["rejected"], message)
            conn.commit()

    extracted = extract()
    validated = validate()
    cleaned = clean()
    loaded = load()
    checked = db_checks(loaded)

    extracted >> validated >> cleaned >> loaded >> checked


ecommerce_etl()