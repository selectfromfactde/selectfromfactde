-- 04_gold_review_kpis.sql
-- Purpose: Build gold.review_kpis with average review score per day.
BEGIN;
CREATE SCHEMA IF NOT EXISTS gold;

DROP MATERIALIZED VIEW IF EXISTS gold.review_kpis;
CREATE MATERIALIZED VIEW gold.review_kpis AS
WITH r AS (
    SELECT
        orv.review_id,
        orv.order_id,
        orv.review_score::numeric AS review_score,
        COALESCE(orv.review_creation_date, orv.review_answer_timestamp)::date AS dt
    FROM silver.order_reviews orv
    WHERE orv.review_score IS NOT NULL
)
SELECT
    dt AS date,
    ROUND(AVG(review_score), 3) AS avg_review_score,
    COUNT(*)::int                AS n_reviews
FROM r
GROUP BY dt
ORDER BY dt;

CREATE INDEX IF NOT EXISTS idx_gold_review_kpis_date ON gold.review_kpis(date);
COMMIT;
