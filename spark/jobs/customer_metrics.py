"""Customer metrics: orders, spend, first/last order and repeat-customer flag per user.

Reads the newest raw snapshot from S3, writes Parquet to
analytics/customer_metrics/run_id=<run_id>/
"""
from pyspark.sql import functions as F

from common import analytics_path, get_spark, latest_run_id, read_raw

VALID_STATUSES = ["delivered", "shipped", "paid"]


def main():
    spark = get_spark("customer_metrics")
    run_id = latest_run_id(spark, ["orders", "users"])
    print(f"using raw snapshot run_id={run_id}")

    orders = read_raw(spark, "orders", run_id)
    users = read_raw(spark, "users", run_id).select("user_id", "country", "created_at")

    is_valid = F.col("status").isin(VALID_STATUSES)
    per_user = orders.groupBy("user_id").agg(
        F.count("*").alias("total_orders"),
        F.sum(F.when(is_valid, 1).otherwise(0)).alias("valid_orders"),
        F.round(F.sum(F.when(is_valid, F.col("total_amount")).otherwise(0)), 2).alias("total_spend"),
        F.min("order_date").alias("first_order_at"),
        F.max("order_date").alias("last_order_at"),
    )

    metrics = (
        users.join(per_user, "user_id", "left")
        .fillna({"total_orders": 0, "valid_orders": 0, "total_spend": 0.0})
        .withColumn(
            "avg_order_value",
            F.when(F.col("valid_orders") > 0, F.round(F.col("total_spend") / F.col("valid_orders"), 2)),
        )
        .withColumn("is_repeat_customer", F.col("valid_orders") >= 2)
    )

    out = analytics_path("customer_metrics", run_id)
    metrics.coalesce(4).write.mode("overwrite").parquet(out)

    result = spark.read.parquet(out)
    print(f"customer_metrics: {result.count()} customers written to {out}")
    result.groupBy("is_repeat_customer").count().show()
    result.orderBy(F.desc("total_spend")).show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()