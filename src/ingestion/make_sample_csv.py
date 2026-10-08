"""Create data/raw/product_price_updates.csv from real product ids.

The file is intentionally a bit dirty (nulls, duplicates, negative prices,
unknown products, messy text) so validate/clean have real work to do.

Run:  python -m src.ingestion.make_sample_csv
"""
import random
from pathlib import Path

import pandas as pd

from src.utils.db import connect

OUT = Path("data/raw/product_price_updates.csv")


def main() -> None:
    random.seed(42)
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT product_id, price FROM products ORDER BY random() LIMIT 80")
        rows = cur.fetchall()

    records = []
    for i, (pid, price) in enumerate(rows):
        new_price = round(float(price) * random.uniform(0.8, 1.25), 2)
        records.append(
            {
                "product_id": pid,
                "new_price": new_price,
                "currency": "USD",
                "effective_date": f"2026-10-{random.randint(1, 8):02d}",
                "updated_by": random.choice(["ali", "sara", " Ahmed ", "SARA"]),
            }
        )

    # --- inject dirty rows ---
    records.append({**records[0]})  # exact duplicate
    records.append({**records[1], "new_price": records[1]["new_price"] + 5})  # conflicting duplicate
    records.append({**records[2], "new_price": -10.0})  # negative price
    records.append({**records[3], "new_price": None})  # missing price
    records.append({**records[4], "product_id": 999999})  # unknown product
    records.append({**records[5], "effective_date": "not-a-date"})  # bad date
    records.append({**records[6], "currency": "pkr"})  # lowercase currency

    df = pd.DataFrame(records).sample(frac=1, random_state=1).reset_index(drop=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"Wrote {len(df)} rows -> {OUT}")


if __name__ == "__main__":
    main()
