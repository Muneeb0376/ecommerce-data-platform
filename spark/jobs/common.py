"""Shared helpers for the Spark jobs: session, S3 paths, latest run_id."""
import os

from pyspark.sql import SparkSession

BUCKET = os.environ.get("S3_BUCKET", "ecommerce-data-lake-muneeb")
REGION = os.environ.get("AWS_REGION", "ap-southeast-2")


def get_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # hide the INFO noise, keep warnings and errors
    return spark


def _run_ids(spark, table: str) -> set:
    """All run_ids that exist under raw/<table>/dt=*/run_id=*/"""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    base = jvm.org.apache.hadoop.fs.Path(f"s3a://{BUCKET}/raw/{table}/")
    fs = base.getFileSystem(conf)
    ids = set()
    for dt_dir in fs.listStatus(base):
        for run_dir in fs.listStatus(dt_dir.getPath()):
            name = run_dir.getPath().getName()
            if name.startswith("run_id="):
                ids.add(name[len("run_id="):])
    return ids


def latest_run_id(spark, tables: list) -> str:
    """Newest run_id that exists for every table in `tables`."""
    common = None
    for table in tables:
        ids = _run_ids(spark, table)
        common = ids if common is None else common & ids
    if not common:
        raise RuntimeError(f"no run_id found that exists for all of: {tables}")
    return max(common)  # run_id looks like 20261015T161227, so max() is the newest


def read_raw(spark, table: str, run_id: str):
    return spark.read.parquet(f"s3a://{BUCKET}/raw/{table}/dt=*/run_id={run_id}/{table}.parquet")


def analytics_path(dataset: str, run_id: str) -> str:
    return f"s3a://{BUCKET}/analytics/{dataset}/run_id={run_id}/"