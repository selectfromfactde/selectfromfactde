
# Olist Marketplace Pipeline (Local Medallion + Star Schema)

This repo is a **reproducible local project** that turns the public **Olist Brazilian e‑commerce dataset** into a modeled warehouse with a **Medallion (bronze/silver/gold)** data lake and a **star schema** for analytics. It is designed to be simple to run on a laptop yet faithful to how DEs build pipelines at work.

## What you get

- **Bronze → Silver → Gold** PySpark jobs (batch/micro-batch friendly)
- **Star schema**: `fact_orders`, `fact_order_items`, `dim_customers`, `dim_products`, `dim_sellers`, `dim_dates`, `dim_geography`
- **Gold metrics** (daily): GMV, Orders, AOV, Active Customers, On‑time Delivery %, Avg Review Score, Repeat Purchase %
- **Airflow DAG** (optional) to orchestrate extract→transform→publish locally
- **Config‑driven paths** and a minimal test scaffold

> 💡 This project does **not** ship the Olist data. Download it from Kaggle and drop the CSVs into `data/raw/olist/` as noted below.

## Quickstart

1) **Install** Python 3.10+ and Java 8+ (for PySpark).  
2) `pip install -r requirements.txt`  
3) **Download Olist** CSVs from Kaggle and place here (or use the helper below):

```
data/raw/olist/
  olist_orders_dataset.csv
  olist_order_items_dataset.csv
  olist_order_payments_dataset.csv
  olist_customers_dataset.csv
  olist_order_reviews_dataset.csv
  olist_products_dataset.csv
  olist_sellers_dataset.csv
  olist_geolocation_dataset.csv
```

4) **Run transforms** (local mode):
```
# Bronze: raw CSV -> normalized CSV (optional) / pass-through
python -m src.etl.bronze_to_silver --mode bronze

# Silver: cleaned, typed Parquet
python -m src.etl.bronze_to_silver --mode silver

# Gold: facts/dims + daily marketplace metrics
python -m src.etl.silver_to_gold
```

Outputs land under `data/{bronze|silver|gold}` partitioned by date.

### Download data from Kaggle (not bundled)

If you have Kaggle API credentials set up (`~/.kaggle/kaggle.json`), you can fetch the data with:

```
source .venv/bin/activate
python scripts/download_olist.py
```

### Load Gold to a local Postgres

Install Postgres locally and start it.

- macOS (Homebrew):
  - `brew install postgresql@15`
  - `brew services start postgresql@15`
- Ubuntu/Debian:
  - `sudo apt-get update && sudo apt-get install postgresql`

Create database and user (adjust as you like):

```
createuser -s analytics || true
psql -d postgres -c "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'analytics') THEN CREATE ROLE analytics LOGIN PASSWORD 'analytics'; END IF; END $$;"
createdb olist -O analytics || true
```

Load Gold Parquet into Postgres:

```
source .venv/bin/activate
python -m src.etl.load_to_postgres --conn postgresql+psycopg2://analytics:analytics@localhost:5432/olist
```

### Build metrics tables for BI

Create materialized daily metrics in Postgres (schema `gold`) from the loaded facts/dims:

```
source .venv/bin/activate
python -m src.etl.build_metrics --conn postgresql+psycopg2://analytics:analytics@localhost:5432/olist --full
```

Optional: seed a small synthetic window (e.g., for demo screenshots) without using the original data:

```
python scripts/seed_metrics_demo.py --conn postgresql+psycopg2://analytics:analytics@localhost:5432/olist --start 2024-01-01 --days 45
```

This populates:
- `gold.daily_sales` (gmv, orders, items, freight, aov)
- `gold.daily_customer` (new_customers, repeat_customers, repeat_rate)
- `gold.delivery_kpis` (on_time_rate, avg_delivery_lag_days, late_orders)
- `gold.review_kpis` (avg_review_score, n_reviews, pct_5_star)

### Explore in Superset (optional)

Install Superset locally (into your current venv or via pipx):

```
# inside .venv
pip install apache-superset

# initialize metadata and create admin user
superset db upgrade
superset fab create-admin \
  --username admin --password admin \
  --firstname Superset --lastname Admin \
  --email admin@example.com || true
superset init

# run the web server
superset run -h 0.0.0.0 -p 8088
```

In the Superset UI (http://localhost:8088), add a Database connection with:

```
postgresql+psycopg2://analytics:analytics@localhost:5432/olist
```

Create datasets on `public.fact_orders`, `public.fact_order_items`, `public.dim_*`, and on the metrics in `gold.*` to build charts & dashboards.

## Project layout

```
config/
  paths.yaml               # data locations and options
data/
  raw/ bronze/ silver/ gold/  # medallion layers (local)
src/
  etl/
    bronze_to_silver.py
    silver_to_gold.py
    load_to_postgres.py
    build_metrics.py
  utils/
    spark.py
    io.py
sql/
  star_schema_postgres.sql    # optional if loading to Postgres later
airflow/
  dags/olist_pipeline_dag.py  # optional
scripts/
  download_olist.py           # optional Kaggle helper
  seed_metrics_demo.py        # optional synthetic metrics seeder
tests/
  test_basic_shapes.py
```

## Star schema (conceptual)

- **fact_orders** (grain: order) → status, timestamps, customer_id, seller_count, total_items, totals
- **fact_order_items** (grain: order_id+item_seq) → product_id, seller_id, price, freight
- **dim_customers** → city/state, geokey
- **dim_products** → category, physical attributes
- **dim_sellers** → city/state, geokey
- **dim_dates** → calendar attributes
- **dim_geography** → (city, state, zipcode prefix)

## Gold metrics (daily)

- `orders` (# of orders purchased that day)
- `gmv` (sum of `payment_value` that day)
- `aov` (gmv / orders)
- `active_customers` (# customers with >= 1 order that day)
- `on_time_pct` (% delivered_on <= estimated_delivery_date among delivered orders)
- `avg_review` (mean review_score of reviews created that day)
- `repeat_purchase_pct` (% customers with ≥2 lifetime orders up to that day)

See `src/etl/silver_to_gold.py` for the Spark jobs.

## Airflow (optional)

If you have Airflow locally, drop `airflow/dags/olist_pipeline_dag.py` into your `dags/` folder or set `AIRFLOW_HOME` to this repo and run the scheduler. The DAG wires:
`bronze -> silver -> gold`

## Data Source & License

This project references the “Olist Brazilian E‑Commerce” dataset on Kaggle. The dataset is licensed CC BY‑NC‑SA 4.0 (NonCommercial + ShareAlike). This repository does not redistribute the data; you must download it directly from Kaggle and accept the license terms.

### Code License

The code in this repository is licensed under the MIT License. See the `LICENSE` file for details. This license applies to the code only and does not cover any third‑party datasets.

## Tests

Run `pytest` to execute minimal shape checks. Expand with real data contracts (e.g., Great Expectations) as needed.
