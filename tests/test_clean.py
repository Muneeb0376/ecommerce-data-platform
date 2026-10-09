import pandas as pd
import pytest

from src.transformation.clean import clean_price_updates

KNOWN = {1, 2, 3}


def make_df(rows):
    cols = ["product_id", "new_price", "currency", "effective_date", "updated_by"]
    return pd.DataFrame(rows, columns=cols)


def good_row(pid=1, price=10.0, date="2026-01-01"):
    return (pid, price, "USD", date, "admin")


def test_valid_row_is_kept():
    clean, rejected = clean_price_updates(make_df([good_row()]), KNOWN)
    assert len(clean) == 1
    assert rejected.empty


def test_text_is_normalised():
    df = make_df([(1, 10.0, " usd ", "2026-01-01", "  Admin ")])
    clean, _ = clean_price_updates(df, KNOWN)
    assert clean.loc[0, "currency"] == "USD"
    assert clean.loc[0, "updated_by"] == "admin"


def test_output_types_and_rounding():
    df = make_df([("2", "10.456", "USD", "2026-01-01", "admin")])
    clean, _ = clean_price_updates(df, KNOWN)
    assert int(clean.loc[0, "product_id"]) == 2
    assert clean.loc[0, "new_price"] == pytest.approx(10.46)


def test_works_without_updated_by_column():
    df = make_df([good_row()]).drop(columns="updated_by")
    clean, rejected = clean_price_updates(df, KNOWN)
    assert len(clean) == 1
    assert rejected.empty


def test_exact_duplicates_are_dropped_not_rejected():
    df = make_df([good_row(), good_row()])
    clean, rejected = clean_price_updates(df, KNOWN)
    assert len(clean) == 1
    assert rejected.empty


@pytest.mark.parametrize(
    "row",
    [
        (1, None, "USD", "2026-01-01", "admin"),          # missing price
        (1, "abc", "USD", "2026-01-01", "admin"),         # non-numeric price
        (1, 10.0, "USD", "not-a-date", "admin"),          # bad date
        (1, 10.0, None, "2026-01-01", "admin"),           # missing currency
    ],
)
def test_missing_or_unparseable_values_are_rejected(row):
    clean, rejected = clean_price_updates(make_df([row]), KNOWN)
    assert clean.empty
    assert list(rejected["reject_reason"]) == ["missing_value"]


@pytest.mark.parametrize("price", [0, -5])
def test_non_positive_price_is_rejected(price):
    clean, rejected = clean_price_updates(make_df([good_row(price=price)]), KNOWN)
    assert clean.empty
    assert list(rejected["reject_reason"]) == ["non_positive_price"]


def test_unknown_product_is_rejected():
    clean, rejected = clean_price_updates(make_df([good_row(pid=999)]), KNOWN)
    assert clean.empty
    assert list(rejected["reject_reason"]) == ["unknown_product"]


def test_conflicting_duplicates_are_all_rejected():
    df = make_df([good_row(price=10.0), good_row(price=12.0)])
    clean, rejected = clean_price_updates(df, KNOWN)
    assert clean.empty
    assert list(rejected["reject_reason"]) == ["conflicting_duplicate"] * 2


def test_first_matching_reason_wins():
    # unknown product AND missing price -> missing_value is checked first
    df = make_df([(999, None, "USD", "2026-01-01", "admin")])
    _, rejected = clean_price_updates(df, KNOWN)
    assert list(rejected["reject_reason"]) == ["missing_value"]


def test_nothing_is_silently_lost():
    df = make_df(
        [
            good_row(pid=1),
            good_row(pid=2, price=-1),
            good_row(pid=999),
            good_row(pid=3, price=5.0),
            good_row(pid=3, price=6.0),
            good_row(pid=1),  # exact duplicate of the first row
        ]
    )
    clean, rejected = clean_price_updates(df, KNOWN)
    assert len(clean) + len(rejected) == len(df.drop_duplicates())
    assert len(clean) == 1
    assert len(rejected) == 4