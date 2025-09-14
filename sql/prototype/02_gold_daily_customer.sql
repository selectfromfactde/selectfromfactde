-- 02_gold_daily_customer.sql
-- Purpose: Build gold.daily_customer with new vs repeat customers and repeat_rate per day.
BEGIN;
CREATE SCHEMA IF NOT EXISTS gold;

DROP MATERIALIZED VIEW IF EXISTS gold.daily_customer;
CREATE MATERIALIZED VIEW gold.daily_customer AS
WITH o AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp::date AS dt
    FROM silver.orders o
    WHERE o.order_status IN ('delivered','shipped','invoiced','processing','approved')
),
firsts AS (
    SELECT customer_id, MIN(dt) AS first_dt
    FROM o
    GROUP BY customer_id
),
labeled AS (
    SELECT o.customer_id, o.dt, f.first_dt
    FROM o
    JOIN firsts f USING (customer_id)
)
SELECT
    dt AS date,
    COUNT(DISTINCT CASE WHEN dt = first_dt THEN customer_id END)::int   AS new_customers,
    COUNT(DISTINCT CASE WHEN dt > first_dt THEN customer_id END)::int   AS repeat_customers,
    ROUND(
        (COUNT(DISTINCT CASE WHEN dt > first_dt THEN customer_id END)::numeric /
         NULLIF(COUNT(DISTINCT customer_id),0)), 4
    ) AS repeat_rate
FROM labeled
GROUP BY dt
ORDER BY dt;

CREATE INDEX IF NOT EXISTS idx_gold_daily_customer_date ON gold.daily_customer(date);
COMMIT;
