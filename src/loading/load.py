"""LOAD step: write clean rows to PostgreSQL. Every function is idempotent
(re-running the pipeline does not create duplicates)."""
import json
from pathlib import Path

import pandas as pd

SCHEMA_SQL = Path("sql/02_etl_tables.sql")


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))


def load_fx_rates(conn, df: pd.DataFrame) -> int:
    rows = list(df[["base_currency", "currency", "rate", "rate_date"]].itertuples(index=False, name=None))
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO fx_rates (base_currency, currency, rate, rate_date)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (base_currency, currency, rate_date)
               DO UPDATE SET rate = EXCLUDED.rate, loaded_at = now()""",
            rows,
        )
    return len(rows)


def load_price_history(conn, df: pd.DataFrame) -> int:
    rows = [
        (int(r.product_id), r.effective_date, float(r.new_price), r.currency, r.updated_by)
        for r in df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO product_price_history
                   (product_id, effective_date, new_price, currency, updated_by)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (product_id, effective_date)
               DO UPDATE SET new_price = EXCLUDED.new_price,
                             currency = EXCLUDED.currency,
                             updated_by = EXCLUDED.updated_by,
                             loaded_at = now()""",
            rows,
        )
    return len(rows)


def load_rejected(conn, df: pd.DataFrame, source: str, run_id: str) -> int:
    # NaN / NaT are not valid JSON, so convert them to None (-> JSON null)
    df = df.astype(object).where(df.notna(), None)
    rows = [
        (source, r["reject_reason"], json.dumps(r.drop("reject_reason").to_dict(), default=str), run_id)
        for _, r in df.iterrows()
    ]
    if rows:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO etl_rejected_rows (source, reject_reason, raw_row, run_id) "
                "VALUES (%s, %s, %s::jsonb, %s)",
                rows,
            )
    return len(rows)


def log_run(conn, run_id, started_at, status, loaded, rejected, message="") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO etl_run_log (run_id, started_at, finished_at, status, rows_loaded, rows_rejected, message)
               VALUES (%s, %s, now(), %s, %s, %s, %s)
               ON CONFLICT (run_id) DO UPDATE
               SET finished_at = now(), status = EXCLUDED.status, rows_loaded = EXCLUDED.rows_loaded,
                   rows_rejected = EXCLUDED.rows_rejected, message = EXCLUDED.message""",
            (run_id, started_at, status, loaded, rejected, message),
        )