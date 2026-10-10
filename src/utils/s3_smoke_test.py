"""One-off check that the AWS credentials in .env can reach the S3 bucket.

It creates the data-lake folder layout (raw / processed / analytics),
uploads a tiny test file, lists it, and deletes it again.
Secrets are never printed.

Run from the project root:
    python src/utils/s3_smoke_test.py
"""
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

REQUIRED = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET"]

# S3 has no real folders; a zero-byte object ending in "/" makes the
# folder visible in the console.
FOLDERS = [
    "raw/orders/",
    "raw/users/",
    "raw/events/",
    "processed/orders/",
    "processed/events/",
    "analytics/daily_sales/",
    "analytics/customer_metrics/",
]

TEST_KEY = "raw/_connection_test.txt"


def main() -> int:
    missing = [name for name in REQUIRED if not os.getenv(name)]
    if missing:
        print(f"Missing in .env: {', '.join(missing)}")
        return 1

    bucket = os.environ["S3_BUCKET"]
    s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])

    try:
        s3.head_bucket(Bucket=bucket)
        print(f"[ok] bucket '{bucket}' is reachable")

        for prefix in FOLDERS:
            s3.put_object(Bucket=bucket, Key=prefix)
        print(f"[ok] created {len(FOLDERS)} folders")

        s3.put_object(Bucket=bucket, Key=TEST_KEY, Body=b"hello from the ecommerce pipeline")
        listed = s3.list_objects_v2(Bucket=bucket, Prefix="raw/")
        keys = [obj["Key"] for obj in listed.get("Contents", [])]
        print(f"[ok] uploaded test file, raw/ now contains: {keys}")

        s3.delete_object(Bucket=bucket, Key=TEST_KEY)
        print("[ok] test file deleted")
    except ClientError as err:
        code = err.response["Error"]["Code"]
        print(f"[fail] AWS error: {code}")
        print("Hints: 403 = key/policy problem, 404/NoSuchBucket = wrong bucket name, "
              "301/PermanentRedirect = wrong region.")
        return 1
    except BotoCoreError as err:
        print(f"[fail] {type(err).__name__}: {err}")
        return 1

    print("S3 setup works.")
    return 0


if __name__ == "__main__":
    sys.exit(main())