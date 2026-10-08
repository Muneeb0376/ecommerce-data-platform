import pandas as pd

from src.transformation.clean import clean_price_updates
from src.transformation.validate import validate_fx_rates, validate_price_updates

KNOWN = {1, 2, 3, 4, 5, 6, 7}


def make_df():
    return pd.DataFrame(
        {
            "product_id": ["1", "1", "2", "3", "4", "999", "5", "6"],
            "new_price": ["10.5", "10.5", "-3", None, "abc", "9", "7", "8"],
            "currency": ["usd", "usd", "USD", "USD", "USD", "USD", "USD", "USD"],
            "effective_date": ["2026-10-01", "2026-10-01", "2026-10-01", "2026-10-01",
                               "2026-10-01", "2026-10-01", "bad", "2026-10-02"],
            "updated_by": [" Ali ", " Ali ", "x", "x", "x", "x", "x", "x"],
        }
    )


def checks(issues):
    return {i.check for i in issues}


def test_validate_finds_all_problem_types():
    found = checks(validate_price_updates(make_df(), KNOWN))
    assert {"null_value", "non_numeric_price", "non_positive_price", "bad_date",
            "exact_duplicate", "unknown_product_id"} <= found


def test_validate_missing_columns():
    assert checks(validate_price_updates(pd.DataFrame({"x": [1]}))) == {"missing_columns"}


def test_clean_keeps_only_good_rows_and_normalises():
    clean, rejected = clean_price_updates(make_df(), KNOWN)
    assert list(clean["product_id"]) == [1, 6]
    assert set(clean["currency"]) == {"USD"}
    assert clean["updated_by"].tolist() == ["ali", "x"]
    reasons = set(rejected["reject_reason"])
    assert {"missing_value", "non_positive_price", "unknown_product"} <= reasons


def test_clean_rejects_conflicting_duplicates():
    df = pd.DataFrame(
        {
            "product_id": ["1", "1"], "new_price": ["5", "6"], "currency": ["USD", "USD"],
            "effective_date": ["2026-10-01", "2026-10-01"], "updated_by": ["a", "a"],
        }
    )
    clean, rejected = clean_price_updates(df, KNOWN)
    assert clean.empty
    assert set(rejected["reject_reason"]) == {"conflicting_duplicate"}


def test_clean_never_loses_rows():
    df = make_df()
    clean, rejected = clean_price_updates(df, KNOWN)
    assert len(clean) + len(rejected) == len(df.drop_duplicates())


def test_fx_validation():
    good = pd.DataFrame({"base_currency": ["USD", "USD"], "currency": ["USD", "PKR"],
                         "rate": [1.0, 280.0], "rate_date": ["2026-10-08"] * 2})
    assert validate_fx_rates(good) == []
    bad = good.assign(rate=[1.0, -2.0])
    assert "non_positive_rate" in checks(validate_fx_rates(bad))
