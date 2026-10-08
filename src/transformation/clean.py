"""CLEAN step: turn the raw price-update frame into trusted rows.

Returns (clean, rejected). Rejected rows keep a `reject_reason` so nothing is
silently dropped.
"""
import pandas as pd


def clean_price_updates(df: pd.DataFrame, known_product_ids: set[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    d = df.copy()

    # 1. normalise text
    d["currency"] = d["currency"].str.strip().str.upper()
    if "updated_by" in d.columns:
        d["updated_by"] = d["updated_by"].str.strip().str.lower()

    # 2. convert types (bad values become NaN / NaT)
    d["product_id"] = pd.to_numeric(d["product_id"], errors="coerce")
    d["new_price"] = pd.to_numeric(d["new_price"], errors="coerce")
    d["effective_date"] = pd.to_datetime(d["effective_date"], errors="coerce").dt.date

    # 3. drop exact duplicates (safe: identical rows carry no extra information)
    d = d.drop_duplicates()

    # 4. mark rejects, first matching reason wins
    d["reject_reason"] = None
    rules = [
        ("missing_value", d[["product_id", "new_price", "currency", "effective_date"]].isna().any(axis=1)),
        ("non_positive_price", d["new_price"] <= 0),
        ("unknown_product", ~d["product_id"].isin(known_product_ids)),
    ]
    for reason, mask in rules:
        d.loc[mask & d["reject_reason"].isna(), "reject_reason"] = reason

    # 5. same product + date but different prices is ambiguous: reject all of them
    ok = d["reject_reason"].isna()
    conflict = ok & d.duplicated(subset=["product_id", "effective_date"], keep=False)
    d.loc[conflict, "reject_reason"] = "conflicting_duplicate"

    rejected = d[d["reject_reason"].notna()].copy()
    clean = d[d["reject_reason"].isna()].drop(columns="reject_reason").copy()
    clean["product_id"] = clean["product_id"].astype(int)
    clean["new_price"] = clean["new_price"].round(2)
    return clean.reset_index(drop=True), rejected.reset_index(drop=True)
