"""S3 data lake helpers.

Layout (partitioned by day and run):
    <zone>/<dataset>/dt=YYYY-MM-DD/run_id=<run_id>/<file>

Uploading the same run_id again overwrites the same keys, so a retry
never creates duplicates (same idea as the idempotent database loads).

Credentials are read from the environment (AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_REGION) and the bucket from S3_BUCKET.
Nothing secret is stored in code.
"""
import os
from pathlib import Path

import pandas as pd


def get_bucket() -> str:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET is not set")
    return bucket


def get_client():
    import boto3  # imported here so tests and non-S3 code do not need boto3

    return boto3.client("s3", region_name=os.environ["AWS_REGION"])


def build_key(zone: str, dataset: str, run_id: str, filename: str) -> str:
    """run_id looks like Airflow's ts_nodash, e.g. 20261010T120000."""
    if len(run_id) < 8 or not run_id[:8].isdigit():
        raise ValueError(f"run_id must start with YYYYMMDD, got {run_id!r}")
    day = f"{run_id[0:4]}-{run_id[4:6]}-{run_id[6:8]}"
    return f"{zone}/{dataset}/dt={day}/run_id={run_id}/{filename}"


def upload_dataframe(df: pd.DataFrame, key: str, client=None, bucket: str | None = None) -> str:
    client = client or get_client()
    body = df.to_csv(index=False).encode("utf-8")
    client.put_object(Bucket=bucket or get_bucket(), Key=key, Body=body, ContentType="text/csv")
    return key


def upload_file(path: Path, key: str, client=None, bucket: str | None = None) -> str:
    client = client or get_client()
    client.put_object(Bucket=bucket or get_bucket(), Key=key, Body=Path(path).read_bytes())
    return key