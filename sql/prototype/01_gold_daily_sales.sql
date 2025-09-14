-- 01_gold_daily_sales.sql
-- Purpose: Build gold.daily_sales with orders, items, GMV and AOV per purchase date.
-- Idempotent pattern: drop + recreate a materialized view. Postgres ≥9.3 required.
BEGIN;
CREATE SCHEMA IF NOT EXISTS gold;

DROP MATERIALIZED VIEW IF EXISTS gold.daily_sales;
CREATE MATERIALIZED VIEW gold.daily_sales AS
WITH o AS (
    SELECT
        o.order_id,
        o.order_purchase_timestamp::date AS dt
    FROM silver.orders o
    WHERE o.order_status IN ('delivered','shipped','invoiced','processing','approved')
),
items AS (
    SELECT oi.order_id, COUNT(*)::int AS items_qty
    FROM silver.order_items oi
    GROUP BY oi.order_id
),
pay AS (
    SELECT op.order_id, SUM(op.payment_value)::numeric(18,2) AS gmv
    FROM silver.order_payments op
    GROUP BY op.order_id
)
SELECT
    o.dt AS date,
    COUNT(DISTINCT o.order_id)::int                                   AS orders_cnt,
    COALESCE(SUM(items.items_qty), 0)::int                             AS items_qty,
    COALESCE(SUM(pay.gmv), 0)::numeric(18,2)                           AS gmv,
    -- Average order value (GMV / distinct orders per day)
    ROUND( (COALESCE(SUM(pay.gmv),0) / NULLIF(COUNT(DISTINCT o.order_id),0))::numeric, 2) AS aov
FROM o
LEFT JOIN items ON items.order_id = o.order_id
LEFT JOIN pay   ON pay.order_id   = o.order_id
GROUP BY o.dt
ORDER BY o.dt;

-- Helpful index to speed up date filters from BI tools
CREATE INDEX IF NOT EXISTS idx_gold_daily_sales_date ON gold.daily_sales(date);
COMMIT;
