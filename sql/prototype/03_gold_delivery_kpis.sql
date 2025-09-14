-- 03_gold_delivery_kpis.sql
-- Purpose: Build gold.delivery_kpis with on_time_rate and avg_delivery_lag_days per delivered date.
BEGIN;
CREATE SCHEMA IF NOT EXISTS gold;

DROP MATERIALIZED VIEW IF EXISTS gold.delivery_kpis;
CREATE MATERIALIZED VIEW gold.delivery_kpis AS
WITH d AS (
    SELECT
        o.order_id,
        o.order_delivered_customer_date::date AS delivered_dt,
        o.order_estimated_delivery_date::date AS est_dt
    FROM silver.orders o
    WHERE o.order_delivered_customer_date IS NOT NULL
)
SELECT
    delivered_dt AS date,
    ROUND(AVG(CASE WHEN delivered_dt <= est_dt THEN 1.0 ELSE 0.0 END)::numeric, 4) AS on_time_rate,
    -- Delivery lag only when late (days as integer); cast to numeric avg
    ROUND(AVG(GREATEST(0, (delivered_dt - est_dt)))::numeric, 4) AS avg_delivery_lag_days,
    COUNT(*)::int AS delivered_orders
FROM d
GROUP BY delivered_dt
ORDER BY delivered_dt;

CREATE INDEX IF NOT EXISTS idx_gold_delivery_kpis_date ON gold.delivery_kpis(date);
COMMIT;
