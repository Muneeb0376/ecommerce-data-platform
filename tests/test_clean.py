import pandas as pd

from src.transformation.clean import clean_price_updates

KNOWN = {1, 2, 3}


def make_df(rows):
    return pd.DataFrame(
        rows,
        columns=["product_id", "new_price", "currency", "effective_date", "updated_by"],
    )


def reasons(rejected):
    return sorted(rejected["reject_reason"].tolist())


def test_good_row_passes():
    df = make_df([(1, 10.5, "USD", "2026-10-01", "ali")])
    clean, rejected = clean_price_updates(df, KNOWN)
    assert len(clean) == 1
    assert rejected.empty


def test_exact_duplicate_kept_once():
    row = (1, 10.0, "USD", "2026-10-01", "ali")
    clean, rejected = clean_price_updates(make_df([row, row]), KNOWN)
    assert len(clean) == 1
    assert rejected.empty


def test_conflicting_duplicate_rejects_both():
    df = make_df([
        (1, 10.0, "USD", "2026-10-01", "ali"),
        (1, 12.0, "USD", "2026-10-01", "ali"),
    ])
    clean, rejected = clean_price_updates(df, KNOWN)
    assert clean.empty
    assert reasons(rejected) == ["conflicting_duplicate", "conflicting_duplicate"]


def test_same_product_different_date_is_fine():
    df = make_df([
        (1, 10.0, "USD", "2026-10-01", "ali"),
        (1, 12.0, "USD", "2026-10-02", "ali"),
    ])
    clean, rejected = clean_price_updates(df, KNOWN)
    assert len(clean) == 2
    assert rejected.empty


def test_negative_price_rejected():
    clean, rejected = clean_price_updates(make_df([(1, -5.0, "USD", "2026-10-01", "ali")]), KNOWN)
    assert clean.empty
    assert reasons(rejected) == ["non_positive_price"]


def test_zero_price_rejected():
    clean, rejected = clean_price_updates(make_df([(1, 0, "USD", "2026-10-01", "ali")]), KNOWN)
    assert clean.empty
    assert reasons(rejected) == ["non_positive_price"]


def test_unknown_product_rejected():
    clean, rejected = clean_price_updates(make_df([(99, 10.0, "USD", "2026-10-01", "ali")]), KNOWN)
    assert clean.empty
    assert reasons(rejected) == ["unknown_product"]


def test_null_price_rejected():
    clean, rejected = clean_price_updates(make_df([(1, None, "USD", "2026-10-01", "ali")]), KNOWN)
    assert clean.empty
    assert reasons(rejected) == ["missing_value"]


def test_text_is_normalised():
    df = make_df([(1, 10.0, " usd ", "2026-10-01", " Ahmed ")])
    clean, _ = clean_price_updates(df, KNOWN)
    assert clean.loc[0, "currency"] == "USD"
    assert clean.loc[0, "updated_by"] == "ahmed"


def test_works_without_updated_by_column():
    df = make_df([(1, 10.0, "USD", "2026-10-01", "ali")]).drop(columns="updated_by")
    clean, rejected = clean_price_updates(df, KNOWN)
    assert len(clean) == 1
    assert rejected.empty