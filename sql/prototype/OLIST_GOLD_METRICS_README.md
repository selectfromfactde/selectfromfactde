# Olist Gold Metrics – Build Scripts (Local Postgres)

This folder contains idempotent SQL to materialize **gold**-layer metrics for the Olist pipeline.
The scripts assume you already loaded and lightly cleaned the raw data into a `silver` schema
(e.g., `silver.orders`, `silver.order_items`, `silver.order_payments`, `silver.order_reviews`, ...).

## What you get

| Object                     | Grain | Columns (high level)                                  | Purpose                          |
|----------------------------|-------|--------------------------------------------------------|----------------------------------|
| `gold.daily_sales`         | day   | `date, orders_cnt, items_qty, gmv, aov`                | GMV/AOV and volume over time     |
| `gold.daily_customer`      | day   | `date, new_customers, repeat_customers, repeat_rate`   | Acquisition vs retention         |
| `gold.delivery_kpis`       | day   | `date, on_time_rate, avg_delivery_lag_days`            | Logistics performance            |
| `gold.review_kpis`         | day   | `date, avg_review_score, n_reviews`                    | CX quality (post‑purchase)       |

All four objects are **materialized views** to make BI fast while keeping refresh simple.

## How to run

1. Ensure your Postgres has the *silver* tables from the Olist dataset.
2. From a shell with `psql` available, run each script:
   ```bash
   psql "$POSTGRES_URL" -f sql/01_gold_daily_sales.sql
   psql "$POSTGRES_URL" -f sql/02_gold_daily_customer.sql
   psql "$POSTGRES_URL" -f sql/03_gold_delivery_kpis.sql
   psql "$POSTGRES_URL" -f sql/04_gold_review_kpis.sql
   ```
   Where `POSTGRES_URL` looks like: `postgresql://user:pass@localhost:5432/olist`.

3. To refresh later (e.g., from Airflow), either rerun the scripts or call:
   ```sql
   REFRESH MATERIALIZED VIEW CONCURRENTLY gold.daily_sales;
   REFRESH MATERIALIZED VIEW CONCURRENTLY gold.daily_customer;
   REFRESH MATERIALIZED VIEW CONCURRENTLY gold.delivery_kpis;
   REFRESH MATERIALIZED VIEW CONCURRENTLY gold.review_kpis;
   ```
   > Tip: add simple date indexes already included in the scripts to speed up `WHERE date BETWEEN ...` filters from Superset.

## Metric definitions (align BI & SQL)

- **GMV**: Sum of `silver.order_payments.payment_value` per order; rolled up by purchase date.
- **AOV**: `GMV / distinct orders` per day.
- **On‑time Delivery %**: Share of delivered orders where `order_delivered_customer_date <= order_estimated_delivery_date` (by delivered date).
- **Avg Delivery Lag (days)**: `AVG(max(0, delivered_date - estimated_date))` (late deliveries only contribute positive days).
- **New Customers**: Customers whose first‑ever purchase date equals the day.
- **Repeat Customers**: Customers purchasing on a day **after** their first purchase.
- **Repeat Rate**: `repeat_customers / (new_customers + repeat_customers)` per day.
- **Avg Review Score**: Mean of `order_reviews.review_score` by review creation/answer date.

## Wire to Superset

In Superset, add four datasets pointing to the materialized views above (Database: your local Postgres; Schema: `gold`). Suggested visuals:
- `gold.daily_sales`: lines for GMV (sum) and AOV (avg) over time.
- `gold.daily_customer`: line for repeat_rate, bars for new vs repeat.
- `gold.delivery_kpis`: line for on_time_rate and area for avg_delivery_lag_days.
- `gold.review_kpis`: line for avg_review_score.

## Airflow (optional)

Your DAG can call `psql` directly or a tiny Python operator to `REFRESH MATERIALIZED VIEW` in order:
1) `gold.daily_sales` → 2) `gold.daily_customer` → 3) `gold.delivery_kpis` → 4) `gold.review_kpis`.

Example BashOperator task:
```bash
psql "$POSTGRES_URL" -c "REFRESH MATERIALIZED VIEW CONCURRENTLY gold.daily_sales;"
```

## Notes & assumptions

- We consider orders in statuses `delivered/shipped/invoiced/processing/approved` for sales metrics; adjust to your definition of “booked vs delivered” revenue.
- If your `silver` table names differ, update the `FROM silver.*` references.
- For very large data, consider switching these to partitioned tables fed by incremental jobs; keep the view on top for BI stability.
