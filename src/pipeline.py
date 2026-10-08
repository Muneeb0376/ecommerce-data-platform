"""Run the whole ETL:  extract -> validate -> clean -> load.

    python -m src.pipeline

Add --skip-api if the currency API is unreachable.
"""
import argparse
import logging
from datetime import datetime, timezone

from src.ingestion.extract import extract_fx_rates, extract_price_updates, fx_payload_to_frame
from src.loading.load import (
    ensure_tables,
    load_fx_rates,
    load_price_history,
    load_rejected,
    log_run,
)
from src.transformation.clean import clean_price_updates
from src.transformation.validate import run_db_checks, validate_fx_rates, validate_price_updates
from src.utils.db import connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("etl")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-api", action="store_true")
    args = ap.parse_args()

    started = datetime.now(timezone.utc)
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    loaded = rejected = 0

    with connect() as conn:
        ensure_tables(conn)
        conn.commit()  # keep the tables even if a later step fails and we roll back
        try:
            # ---- EXTRACT ----
            prices_raw = extract_price_updates()
            log.info("extracted %d price-update rows from CSV", len(prices_raw))
            with conn.cursor() as cur:
                cur.execute("SELECT product_id FROM products")
                known_ids = {r[0] for r in cur.fetchall()}

            # ---- VALIDATE ----
            for issue in validate_price_updates(prices_raw, known_ids):
                log.warning("CSV check failed: %s", issue)

            # ---- CLEAN ----
            clean, bad = clean_price_updates(prices_raw, known_ids)
            log.info("clean=%d rejected=%d", len(clean), len(bad))

            # ---- LOAD ----
            loaded += load_price_history(conn, clean)
            rejected += load_rejected(conn, bad, "product_price_updates.csv", run_id)

            if not args.skip_api:
                fx = fx_payload_to_frame(extract_fx_rates())
                fx_issues = validate_fx_rates(fx)
                for issue in fx_issues:
                    log.warning("FX check failed: %s", issue)
                if fx_issues:
                    raise RuntimeError("FX data failed validation, not loading it")
                loaded += load_fx_rates(conn, fx)

            # ---- database-wide quality report ----
            db_issues = run_db_checks(conn)
            for issue in db_issues:
                log.warning("DB check failed: %s", issue)
            if not db_issues:
                log.info("all database quality checks passed")

            log_run(conn, run_id, started, "success", loaded, rejected)
            conn.commit()
            log.info("done: loaded=%d rejected=%d", loaded, rejected)
        except Exception as exc:
            conn.rollback()
            log_run(conn, run_id, started, "failed", loaded, rejected, str(exc)[:500])
            conn.commit()
            log.error("pipeline failed: %s", exc)
            raise


if __name__ == "__main__":
    main()