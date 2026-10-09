import pandas as pd

from src.transformation.validate import (
    Issue,
    run_db_checks,
    validate_fx_rates,
    validate_price_updates,
)

KNOWN = {1, 2, 3}


def price_df(rows):
    cols = ["product_id", "new_price", "currency", "effective_date"]
    return pd.DataFrame(rows, columns=cols)


def checks(issues):
    return [i.check for i in issues]


# ---------- price updates ----------

def test_clean_price_data_has_no_issues():
    df = price_df([(1, 10.0, "USD", "2026-01-01"), (2, 5.0, "USD", "2026-01-01")])
    assert validate_price_updates(df, KNOWN) == []


def test_missing_columns_stops_other_checks():
    df = pd.DataFrame({"product_id": [1]})
    issues = validate_price_updates(df, KNOWN)
    assert checks(issues) == ["missing_columns"]
    assert issues[0].count == 3


def test_null_values_are_counted_per_column():
    df = price_df([(1, None, "USD", "2026-01-01"), (2, 5.0, None, "2026-01-01")])
    issues = validate_price_updates(df, KNOWN)
    assert checks(issues).count("null_value") == 2


def test_non_numeric_price():
    df = price_df([(1, "abc", "USD", "2026-01-01")])
    assert "non_numeric_price" in checks(validate_price_updates(df, KNOWN))


def test_non_positive_price():
    df = price_df([(1, 0, "USD", "2026-01-01"), (2, -3, "USD", "2026-01-01")])
    issue = next(i for i in validate_price_updates(df, KNOWN) if i.check == "non_positive_price")
    assert issue.count == 2


def test_bad_date():
    df = price_df([(1, 10.0, "USD", "not-a-date")])
    assert "bad_date" in checks(validate_price_updates(df, KNOWN))


def test_exact_duplicate_is_not_a_conflict():
    row = (1, 10.0, "USD", "2026-01-01")
    found = checks(validate_price_updates(price_df([row, row]), KNOWN))
    assert "exact_duplicate" in found
    assert "conflicting_duplicate_key" not in found


def test_conflicting_duplicate_key():
    df = price_df([(1, 10.0, "USD", "2026-01-01"), (1, 12.0, "USD", "2026-01-01")])
    issue = next(i for i in validate_price_updates(df, KNOWN) if i.check == "conflicting_duplicate_key")
    assert issue.count == 2


def test_unknown_product_id():
    df = price_df([(999, 10.0, "USD", "2026-01-01")])
    assert "unknown_product_id" in checks(validate_price_updates(df, KNOWN))


def test_unknown_product_check_skipped_when_ids_not_given():
    df = price_df([(999, 10.0, "USD", "2026-01-01")])
    assert validate_price_updates(df) == []


def test_validate_never_changes_the_data():
    df = price_df([(1, "abc", "usd ", "2026-01-01")])
    before = df.copy()
    validate_price_updates(df, KNOWN)
    pd.testing.assert_frame_equal(df, before)


# ---------- fx rates ----------

def fx_df(rows):
    return pd.DataFrame(rows, columns=["base_currency", "currency", "rate", "rate_date"])


def test_valid_fx_rates():
    df = fx_df([("USD", "USD", 1.0, "2026-01-01"), ("USD", "EUR", 0.9, "2026-01-01")])
    assert validate_fx_rates(df) == []


def test_empty_fx_response():
    assert checks(validate_fx_rates(fx_df([]))) == ["empty_fx_response"]


def test_non_positive_fx_rate():
    df = fx_df([("USD", "USD", 1.0, "2026-01-01"), ("USD", "EUR", 0, "2026-01-01")])
    assert "non_positive_rate" in checks(validate_fx_rates(df))


def test_duplicate_fx_rate():
    df = fx_df([("USD", "USD", 1.0, "2026-01-01"), ("USD", "USD", 1.0, "2026-01-01")])
    assert "duplicate_rate" in checks(validate_fx_rates(df))


def test_missing_usd():
    df = fx_df([("USD", "EUR", 0.9, "2026-01-01")])
    assert "missing_usd" in checks(validate_fx_rates(df))


# ---------- Issue ----------

def test_issue_string_format():
    assert str(Issue("bad_date", 2)) == "[bad_date] 2 row(s)"
    assert str(Issue("null_value", 1, "in column 'x'")) == "[null_value] 1 row(s) in column 'x'"


# ---------- database checks (fake connection, no real DB needed) ----------

class FakeCursor:
    def __init__(self, counts):
        self.counts = counts
        self.calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.current = self.counts[self.calls]
        self.calls += 1

    def fetchone(self):
        return (self.current,)


class FakeConn:
    def __init__(self, counts):
        self.cur = FakeCursor(counts)

    def cursor(self):
        return self.cur


def test_run_db_checks_reports_only_non_zero_counts():
    from src.transformation.validate import DB_CHECKS

    counts = [0] * len(DB_CHECKS)
    counts[1] = 4
    issues = run_db_checks(FakeConn(counts))
    assert len(issues) == 1
    assert issues[0].count == 4


def test_run_db_checks_all_clean():
    from src.transformation.validate import DB_CHECKS

    assert run_db_checks(FakeConn([0] * len(DB_CHECKS))) == []