"""Export the main PostgreSQL tables to the S3 data lake as Parquet files.

Each run writes a full snapshot of every table to
raw/<table>/dt=<date>/run_id=<run_id>/<table>.parquet
(same layout as the price-update files, same run_id => same keys, so retries are safe).
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from psycopg import sql

from src.loading.s3 import build_key, get_client, upload_file
from src.utils.db import connect

log = logging.getLogger(__name__)

# Only these tables can be exported (the names go into SQL, so keep a fixed list).
EXPORT_TABLES = [
    "users",
    "sellers",
    "categories",
    "products",
    "orders",
    "order_items",
    "payments",
    "reviews",
    "events",
]


def read_table(conn, table: str) -> pd.DataFrame:
    """Read a whole table into a DataFrame."""
    if table not in EXPORT_TABLES:
        raise ValueError(f"table '{table}' is not in the export list")

    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))
        columns = [col.name for col in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def export_table(conn, table: str, out_dir: Path, run_id: str, client) -> int:
    """Write one table to a local Parquet file, upload it to S3, return the row count."""
    df = read_table(conn, table)

    path = out_dir / f"{table}.parquet"
    df.to_parquet(path, engine="pyarrow", index=False)

    key = build_key("raw", table, run_id, f"{table}.parquet")
    upload_file(path, key, client=client)

    log.info("exported %s: %d rows -> %s", table, len(df), key)
    return len(df)


def export_all(run_id: str, out_dir: Path) -> dict:
    """Export every table in EXPORT_TABLES. Returns {table: row_count}."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    client = get_client()
    counts = {}
    with connect() as conn:
        for table in EXPORT_TABLES:
            counts[table] = export_table(conn, table, out_dir, run_id, client)
    return counts


if __name__ == "__main__":
    # Manual run from the project root: python -m src.loading.export_to_s3
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    result = export_all(run_id, Path("data") / "staging" / run_id / "export")
    print(result)