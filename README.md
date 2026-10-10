# E-Commerce Data Platform

![CI](https://github.com/Muneeb0376/ecommerce-data-platform/actions/workflows/ci.yml/badge.svg)

A production-style batch data pipeline for an e-commerce domain: it ingests product price updates (CSV) and currency exchange rates (API), validates and cleans them, loads the trusted rows into PostgreSQL, and keeps every rejected row with a reason. The pipeline is orchestrated by Apache Airflow and runs fully in Docker.

**Stack:** Python · pandas · PostgreSQL 17 · Apache Airflow · Docker Compose · pytest

## Architecture

```
CSV price updates ──┐
                    ├─► Validate ─► Clean ─► Load ─► PostgreSQL
FX rates API ───────┘      │          │         │
                           │          │         ├─ product_price_history
                           │          │         ├─ fx_rates
                           │          └─ rejects ─┴─ etl_rejected_rows (with reason)
                           └─ issues reported       etl_run_log (one row per run)

Orchestration: Airflow DAG (scheduler + webserver, LocalExecutor)
```

Pipeline steps live in separate modules so each can be tested on its own:

| Step | Module | What it does |
|------|--------|--------------|
| Validate | `src/transformation/validate.py` | Finds problems and reports them as `Issue` objects. Never changes data. |
| Clean | `src/transformation/clean.py` | Normalises text, converts types, drops exact duplicates, splits rows into clean and rejected. |
| Load | `src/loading/load.py` | Writes to PostgreSQL. Every function is idempotent. |

## Data quality and rejected rows

Nothing is silently dropped. Every rejected row is stored in `etl_rejected_rows` with its `run_id` and a `reject_reason`:

| Reason | Meaning |
|--------|---------|
| `missing_value` | A required field is empty or could not be parsed (price, date, ...) |
| `non_positive_price` | Price is zero or negative |
| `unknown_product` | `product_id` does not exist in the products table |
| `conflicting_duplicate` | Same product and date with different prices, so all copies are rejected |

Rejected rows from the latest run:

![Rejected rows by reason](docs/images/rejected_rows.png)

## Idempotency

Re-running the pipeline never creates duplicates:

- `fx_rates` and `product_price_history` use `INSERT ... ON CONFLICT DO UPDATE`.
- `etl_run_log` uses `ON CONFLICT (run_id) DO UPDATE`.
- `load_rejected` deletes the rejects of the same `run_id` and source before inserting, so an Airflow retry does not double the rows. Rejects of different runs are kept as history.

## Orchestration

The Airflow DAG runs the pipeline end to end, with retries and logging.

![Airflow DAG](docs/images/airflow_dag.png)

## Project structure

```
ecommerce-data-platform/
├── airflow/dags/          # Airflow DAG definitions
├── docker/airflow/        # Airflow image (Dockerfile)
├── sql/                   # Schema and ETL table scripts (run on first DB start)
├── src/
│   ├── ingestion/         # CSV and API readers
│   ├── transformation/    # validate.py, clean.py
│   └── loading/           # load.py
├── tests/                 # pytest unit tests
├── docs/images/           # README screenshots
├── data/                  # Local input data
├── docker-compose.yml
├── .env.example
└── requirements.txt
```

## Getting started

**Requirements:** Docker Desktop, Python 3.12+, Git.

```powershell
git clone https://github.com/Muneeb0376/ecommerce-data-platform.git
cd ecommerce-data-platform

# 1. Configuration
copy .env.example .env      # then edit .env and set real passwords and a secret key

# 2. Start PostgreSQL and Airflow
docker compose up -d --build
docker compose ps
```

- Airflow UI: <http://localhost:8080> (log in with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from your `.env`)
- PostgreSQL: `localhost:<POSTGRES_PORT>` (database `ecommerce`)
- Schema scripts in `sql/` run automatically the first time the database volume is created.

Open the Airflow UI, enable the DAG and click **Trigger DAG**.

## Running the tests

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest -v
```

The tests cover the cleaning rules, every validation check and the database checks (with a fake connection, so no running database is needed).

## Useful queries

Rejected rows of the latest run:

```sql
SELECT reject_reason, COUNT(*) AS rows_rejected
FROM etl_rejected_rows
WHERE run_id = (SELECT run_id FROM etl_run_log ORDER BY started_at DESC LIMIT 1)
GROUP BY reject_reason
ORDER BY rows_rejected DESC;
```

Run history:

```sql
SELECT run_id, status, rows_loaded, rows_rejected, message
FROM etl_run_log
ORDER BY started_at DESC
LIMIT 5;
```

## Security

Secrets are read from environment variables. `.env` is git-ignored and only `.env.example` with placeholders is committed.

## Roadmap

- [x] Python + PostgreSQL + ETL with data-quality checks
- [x] Airflow orchestration in Docker
- [x] Unit tests
- [ ] AWS S3 data lake (raw / processed / analytics)
- [ ] PySpark processing
- [ ] Data warehouse (star schema) and Power BI dashboards
- [ ] Kafka and Spark Streaming for real-time events