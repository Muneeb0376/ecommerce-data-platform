"""EXTRACT step: read the CSV and call the currency-rates API.

No cleaning here. Extract only gets data and keeps a raw copy.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

RAW_DIR = Path("data/raw")
FX_URL = "https://open.er-api.com/v6/latest/USD"  # free, no API key


def extract_price_updates(path: Path = RAW_DIR / "product_price_updates.csv") -> pd.DataFrame:
    # dtype=str so bad values ("abc") do not crash the read; validate/clean convert types.
    return pd.read_csv(path, dtype=str)


def extract_fx_rates(url: str = FX_URL, timeout: int = 15) -> dict:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("result") != "success":
        raise RuntimeError(f"FX API returned an error: {payload}")

    # keep the raw response so a run can be audited / replayed later
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (RAW_DIR / f"fx_rates_{stamp}.json").write_text(json.dumps(payload), encoding="utf-8")
    return payload


def fx_payload_to_frame(payload: dict) -> pd.DataFrame:
    base = payload["base_code"]
    as_of = datetime.fromtimestamp(payload["time_last_update_unix"], tz=timezone.utc).date()
    rows = [
        {"base_currency": base, "currency": cur, "rate": rate, "rate_date": as_of}
        for cur, rate in payload["rates"].items()
    ]
    return pd.DataFrame(rows)
