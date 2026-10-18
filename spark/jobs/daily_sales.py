"""Daily sales: orders, valid orders, revenue, units and average order value per day.

Reads the newest raw snapshot from S3, writes Parquet to
analytics/daily_sales/run_id=<run_id>/order_month=<yyyy-MM>/
"""
from pyspark.sql import functions as F

from common import analytics_path, get_spark, latest_run_id, read_raw

VALID_STATUSES = ["delivered", "shipped", "paid"]


def main():
    spark = get_spark("daily_sales")
    run_id = latest_run_id(spark, ["orders", "order_items"])
    print(f"using raw snapshot run_id={run_id}")

    orders = read_raw(spark, "orders", run_id)
    items = read_raw(spark, "order_items", run_id)

    units = items.groupBy("order_id").agg(F.sum("quantity").alias("units"))

    o = (
        orders.join(units, "order_id", "left")
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("is_valid", F.col("status").isin(VALID_STATUSES))
    )

    daily = (
        o.groupBy("order_date")
        .agg(
            F.count("*").alias("orders"),
            F.sum(F.when(F.col("is_valid"), 1).otherwise(0)).alias("valid_orders"),
            F.sum(F.when(F.col("status").isin("cancelled", "returned"), 1).otherwise(0)).alias(
                "cancelled_or_returned"
            ),
            F.round(F.sum(F.when(F.col("is_valid"), F.col("total_amount")).otherwise(0)), 2).alias(
                "revenue"
            ),
            F.sum(F.when(F.col("is_valid"), F.col("units")).otherwise(0)).alias("units_sold"),
        )
        .withColumn(
            "avg_order_value",
            F.when(F.col("valid_orders") > 0, F.round(F.col("revenue") / F.col("valid_orders"), 2)),
        )
        .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    )

    out = analytics_path("daily_sales", run_id)
    daily.repartition("order_month").write.mode("overwrite").partitionBy("order_month").parquet(out)

    result = spark.read.parquet(out)
    print(f"daily_sales: {result.count()} days written to {out}")
    result.orderBy(F.desc("revenue")).show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()