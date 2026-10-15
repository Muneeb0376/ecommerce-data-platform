"""Export the main PostgreSQL tables to the S3 data lake as Parquet files.

Each run writes a full snapshot of every table to
raw/<table>/dt=<date>/run_id=<run_id>/<table>.parquet
(same layout as the price-update files, same run_id => same keys, so retries are safe).

Tables are read in chunks (server-side cursor) and written chunk by chunk,
so memory stays small even for millions of rows.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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

CHUNK_ROWS = 100_000
NUMERIC_OID = 1700  # PostgreSQL type id of NUMERIC


def write_table_parquet(conn, table: str, path: Path) -> int:
    """Stream one table into a local Parquet file, chunk by chunk. Returns the row count."""
    if table not in EXPORT_TABLES:
        raise ValueError(f"table '{table}' is not in the export list")

    writer = None
    schema = None
    total = 0
    try:
        # a named cursor is a server-side cursor: rows are fetched in chunks, not all at once
        with conn.cursor(name=f"export_{table}") as cur:
            cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))
            columns = [col.name for col in cur.description]
            numeric_cols = [col.name for col in cur.description if col.type_code == NUMERIC_OID]

            first = True
            while True:
                rows = cur.fetchmany(CHUNK_ROWS)
                if not rows and not first:
                    break

                df = pd.DataFrame(rows, columns=columns)
                for col in numeric_cols:
                    df[col] = df[col].astype("float64")

                if writer is None:
                    batch = pa.Table.from_pandas(df, preserve_index=False)
                    schema = batch.schema
                    writer = pq.ParquetWriter(
                        path, schema, coerce_timestamps="us", allow_truncated_timestamps=True
                    )
                else:
                    batch = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

                writer.write_table(batch)
                total += len(df)
                first = False
                if not rows:
                    break
    finally:
        if writer is not None:
            writer.close()
    return total


def export_table(conn, table: str, out_dir: Path, run_id: str, client) -> int:
    """Write one table to a local Parquet file, upload it to S3, return the row count."""
    path = out_dir / f"{table}.parquet"
    rows = write_table_parquet(conn, table, path)

    key = build_key("raw", table, run_id, f"{table}.parquet")
    upload_file(path, key, client=client)
    path.unlink()  # the copy in S3 is the one that matters, don't keep big local files

    log.info("exported %s: %d rows -> %s", table, rows, key)
    return rows


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