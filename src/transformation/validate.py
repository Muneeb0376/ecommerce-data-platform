"""VALIDATE step: find problems, never change data.

Every check returns a list of Issue objects so the pipeline can report them
and decide whether to continue or stop.
"""
from dataclasses import dataclass

import pandas as pd

VALID_ORDER_STATUSES = {"pending", "paid", "shipped", "delivered", "cancelled", "returned"}
REQUIRED_PRICE_COLUMNS = ["product_id", "new_price", "currency", "effective_date"]


@dataclass
class Issue:
    check: str
    count: int
    detail: str = ""

    def __str__(self) -> str:
        return f"[{self.check}] {self.count} row(s) {self.detail}".strip()


# ---------- CSV: product price updates ----------

def validate_price_updates(df: pd.DataFrame, known_product_ids: set[int] | None = None) -> list[Issue]:
    missing = [c for c in REQUIRED_PRICE_COLUMNS if c not in df.columns]
    if missing:
        # structural problem: no point running the other checks
        return [Issue("missing_columns", len(missing), str(missing))]

    issues: list[Issue] = []

    for col in REQUIRED_PRICE_COLUMNS:
        n = int(df[col].isna().sum())
        if n:
            issues.append(Issue("null_value", n, f"in column '{col}'"))

    price = pd.to_numeric(df["new_price"], errors="coerce")
    not_numeric = int((price.isna() & df["new_price"].notna()).sum())
    if not_numeric:
        issues.append(Issue("non_numeric_price", not_numeric))
    negative = int((price <= 0).sum())
    if negative:
        issues.append(Issue("non_positive_price", negative))

    dates = pd.to_datetime(df["effective_date"], errors="coerce")
    bad_dates = int((dates.isna() & df["effective_date"].notna()).sum())
    if bad_dates:
        issues.append(Issue("bad_date", bad_dates))

    dup_exact = int(df.duplicated().sum())
    if dup_exact:
        issues.append(Issue("exact_duplicate", dup_exact))
    # exact duplicates are removed first, so only real conflicts (same key,
    # different values) are counted here
    deduped = df.drop_duplicates()
    dup_key = int(deduped.duplicated(subset=["product_id", "effective_date"], keep=False).sum())
    if dup_key:
        issues.append(Issue("conflicting_duplicate_key", dup_key, "same product_id + effective_date"))

    if known_product_ids is not None:
        ids = pd.to_numeric(df["product_id"], errors="coerce")
        unknown = int((~ids.isin(known_product_ids) & ids.notna()).sum())
        if unknown:
            issues.append(Issue("unknown_product_id", unknown))

    return issues


# ---------- API: currency rates ----------

def validate_fx_rates(df: pd.DataFrame) -> list[Issue]:
    issues: list[Issue] = []
    if df.empty:
        return [Issue("empty_fx_response", 0)]
    bad = int((~(df["rate"] > 0)).sum())
    if bad:
        issues.append(Issue("non_positive_rate", bad))
    dup = int(df.duplicated(subset=["base_currency", "currency", "rate_date"]).sum())
    if dup:
        issues.append(Issue("duplicate_rate", dup))
    if "USD" not in set(df["currency"]):
        issues.append(Issue("missing_usd", 1))
    return issues


# ---------- Database data-quality checks (SQL based) ----------

DB_CHECKS = {
    "orders_invalid_status": (
        "SELECT COUNT(*) FROM orders WHERE status <> ALL(%(statuses)s)",
        {"statuses": sorted(VALID_ORDER_STATUSES)},
    ),
    "orders_null_user": ("SELECT COUNT(*) FROM orders WHERE user_id IS NULL", {}),
    "orders_negative_total": ("SELECT COUNT(*) FROM orders WHERE total_amount < 0", {}),
    "products_non_positive_price": ("SELECT COUNT(*) FROM products WHERE price <= 0", {}),
    "duplicate_user_email": (
        "SELECT COUNT(*) FROM (SELECT email FROM users GROUP BY email HAVING COUNT(*) > 1) t",
        {},
    ),
    "orphan_order_items": (
        "SELECT COUNT(*) FROM order_items oi LEFT JOIN orders o USING (order_id) WHERE o.order_id IS NULL",
        {},
    ),
}


def run_db_checks(conn) -> list[Issue]:
    issues = []
    with conn.cursor() as cur:
        for name, (sql, params) in DB_CHECKS.items():
            cur.execute(sql, params)
            n = cur.fetchone()[0]
            if n:
                issues.append(Issue(name, n))
    return issues