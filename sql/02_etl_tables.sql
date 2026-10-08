-- Stage 2 ETL tables. Safe to run many times.

CREATE TABLE IF NOT EXISTS fx_rates (
    base_currency TEXT        NOT NULL,
    currency      TEXT        NOT NULL,
    rate          NUMERIC(18,8) NOT NULL CHECK (rate > 0),
    rate_date     DATE        NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (base_currency, currency, rate_date)
);

CREATE TABLE IF NOT EXISTS product_price_history (
    product_id     INT         NOT NULL REFERENCES products(product_id),
    effective_date DATE        NOT NULL,
    new_price      NUMERIC(12,2) NOT NULL CHECK (new_price > 0),
    currency       TEXT        NOT NULL,
    updated_by     TEXT,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, effective_date)
);

CREATE TABLE IF NOT EXISTS etl_rejected_rows (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT        NOT NULL,
    reject_reason TEXT        NOT NULL,
    raw_row       JSONB       NOT NULL,
    run_id        TEXT        NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id        TEXT PRIMARY KEY,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL,
    rows_loaded   INT,
    rows_rejected INT,
    message       TEXT
);
