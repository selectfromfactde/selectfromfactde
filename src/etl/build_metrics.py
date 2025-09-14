"""
Build aggregate Gold metrics tables in Postgres for Superset.

Creates/updates tables under schema `gold`:
 - daily_sales (date_key pk)
 - daily_customer (date_key pk)
 - delivery_kpis (date_key pk)
 - review_kpis (date_key pk)
 - seller_kpis (date_key, seller_id pk)

Usage:
  python -m src.etl.build_metrics --conn postgresql+psycopg2://user:pass@localhost:5432/olist --date 2017-10-01
  python -m src.etl.build_metrics --conn postgresql+psycopg2://... --full

Assumes core fact tables exist in schema `public`:
  public.fact_orders, public.fact_order_items, public.order_reviews (optional for review KPIs)
"""
from __future__ import annotations
import argparse
from sqlalchemy import create_engine, text


DDL_SQL = [
    """
    CREATE SCHEMA IF NOT EXISTS gold;
    """,
    """
    CREATE TABLE IF NOT EXISTS gold.daily_sales(
      date_key date PRIMARY KEY,
      orders int,
      items int,
      gmv numeric,
      freight numeric,
      aov numeric
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS gold.daily_customer(
      date_key date PRIMARY KEY,
      new_customers int,
      repeat_customers int,
      repeat_rate numeric
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS gold.delivery_kpis(
      date_key date PRIMARY KEY,
      on_time_rate numeric,
      avg_delivery_lag_days numeric,
      late_orders int
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS gold.review_kpis(
      date_key date PRIMARY KEY,
      avg_review_score numeric,
      n_reviews int,
      pct_5_star numeric
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS gold.seller_kpis(
      date_key date,
      seller_id text,
      orders int,
      gmv numeric,
      aov numeric,
      avg_review_score numeric,
      PRIMARY KEY(date_key, seller_id)
    );
    """,
]


INS_DAILY_SALES = text(
    """
    DELETE FROM gold.daily_sales WHERE date_key = :ds;
    INSERT INTO gold.daily_sales (date_key, orders, items, gmv, freight, aov)
    SELECT
      fo.purchase_date AS date_key,
      COUNT(DISTINCT fo.order_id) AS orders,
      COUNT(oi.*) AS items,
      COALESCE(SUM(oi.price),0)::numeric AS gmv,
      COALESCE(SUM(oi.freight_value),0)::numeric AS freight,
      CASE WHEN COUNT(DISTINCT fo.order_id) = 0 THEN 0
           ELSE COALESCE(SUM(oi.price),0)::numeric / COUNT(DISTINCT fo.order_id) END AS aov
    FROM public.fact_orders fo
    LEFT JOIN public.fact_order_items oi USING (order_id)
    WHERE fo.purchase_date = CAST(:ds AS date)
    GROUP BY 1;
    """
)

INS_DELIVERY_KPIS = text(
    """
    DELETE FROM gold.delivery_kpis WHERE date_key = :ds;
    WITH base AS (
      SELECT
        fo.order_id,
        fo.purchase_date AS date_key,
        fo.order_delivered_customer_date,
        fo.order_estimated_delivery_date,
        GREATEST(0, (fo.order_delivered_customer_date::date - fo.order_estimated_delivery_date::date)) AS late_by_days,
        CASE WHEN fo.delivered_on_time = 1 THEN 1 ELSE 0 END AS on_time_flag
      FROM public.fact_orders fo
      WHERE fo.purchase_date = CAST(:ds AS date)
    )
    INSERT INTO gold.delivery_kpis(date_key, on_time_rate, avg_delivery_lag_days, late_orders)
    SELECT
      CAST(:ds AS date) AS date_key,
      CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(on_time_flag)::numeric / COUNT(*) END AS on_time_rate,
      AVG(late_by_days)::numeric AS avg_delivery_lag_days,
      SUM(CASE WHEN late_by_days > 0 THEN 1 ELSE 0 END) AS late_orders
    FROM base;
    """
)

INS_REVIEW_KPIS = text(
    """
    DELETE FROM gold.review_kpis WHERE date_key = :ds;
    INSERT INTO gold.review_kpis(date_key, avg_review_score, n_reviews, pct_5_star)
    SELECT
      CAST(:ds AS date) AS date_key,
      COALESCE(AVG(CASE WHEN r.review_score ~ '^[0-9.]+$' THEN r.review_score::numeric END), 0) AS avg_review_score,
      COUNT(*) AS n_reviews,
      CASE WHEN COUNT(*) = 0 THEN 0 ELSE SUM(CASE WHEN r.review_score::text = '5' THEN 1 ELSE 0 END)::numeric / COUNT(*) END AS pct_5_star
    FROM public.order_reviews r
    WHERE r.review_creation_date::date = CAST(:ds AS date);
    """
)

INS_DAILY_CUSTOMER = text(
    """
    DELETE FROM gold.daily_customer WHERE date_key = :ds;
    WITH per_day AS (
      SELECT fo.customer_id, fo.purchase_date AS date_key, COUNT(*) AS orders
      FROM public.fact_orders fo
      WHERE fo.purchase_date = CAST(:ds AS date)
      GROUP BY 1,2
    ),
    lifetime AS (
      SELECT fo.customer_id, fo.purchase_date,
             SUM(1) OVER (PARTITION BY fo.customer_id ORDER BY fo.purchase_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_orders
      FROM (
        SELECT DISTINCT customer_id, purchase_date FROM public.fact_orders
      ) fo
      WHERE fo.purchase_date <= CAST(:ds AS date)
    )
    INSERT INTO gold.daily_customer(date_key, new_customers, repeat_customers, repeat_rate)
    SELECT
      CAST(:ds AS date) AS date_key,
      SUM(CASE WHEN l.cum_orders = 1 AND l.purchase_date = CAST(:ds AS date) THEN 1 ELSE 0 END) AS new_customers,
      SUM(CASE WHEN l.cum_orders >= 2 AND l.purchase_date = CAST(:ds AS date) THEN 1 ELSE 0 END) AS repeat_customers,
      CASE WHEN COUNT(pd.customer_id) = 0 THEN 0 ELSE
        SUM(CASE WHEN l.cum_orders >= 2 AND l.purchase_date = CAST(:ds AS date) THEN 1 ELSE 0 END)::numeric / COUNT(pd.customer_id) END AS repeat_rate
    FROM lifetime l
    LEFT JOIN per_day pd ON pd.customer_id = l.customer_id AND pd.date_key = l.purchase_date
    WHERE l.purchase_date = CAST(:ds AS date);
    """
)

INS_SELLER_KPIS = text(
    """
    DELETE FROM gold.seller_kpis WHERE date_key = :ds;
    WITH base AS (
      SELECT
        fo.purchase_date AS date_key,
        oi.seller_id,
        fo.order_id,
        oi.price
      FROM public.fact_orders fo
      JOIN public.fact_order_items oi USING (order_id)
      WHERE fo.purchase_date = CAST(:ds AS date)
    ),
    by_seller AS (
      SELECT
        date_key,
        seller_id,
        COUNT(DISTINCT order_id) AS orders,
        SUM(price)::numeric AS gmv
      FROM base
      GROUP BY 1,2
    ),
    reviews AS (
      SELECT
        fo.purchase_date AS date_key,
        oi.seller_id,
        AVG(CASE WHEN r.review_score ~ '^[0-9.]+$' THEN r.review_score::numeric END) AS avg_review_score
      FROM public.order_reviews r
      JOIN public.fact_orders fo USING (order_id)
      JOIN public.fact_order_items oi USING (order_id)
      WHERE fo.purchase_date = CAST(:ds AS date)
      GROUP BY 1,2
    )
    INSERT INTO gold.seller_kpis(date_key, seller_id, orders, gmv, aov, avg_review_score)
    SELECT
      b.date_key, b.seller_id, b.orders, b.gmv,
      CASE WHEN b.orders = 0 THEN 0 ELSE b.gmv / b.orders END AS aov,
      COALESCE(rv.avg_review_score, 0)
    FROM by_seller b
    LEFT JOIN reviews rv ON rv.date_key = b.date_key AND rv.seller_id = b.seller_id;
    """
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conn", required=True, help="SQLAlchemy URI to Postgres")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="YYYY-MM-DD to (re)build")
    g.add_argument("--full", action="store_true", help="Rebuild for all dates in fact_orders")
    args = ap.parse_args()

    eng = create_engine(args.conn)
    with eng.begin() as conn:
        # ensure schema and tables
        for ddl in DDL_SQL:
            conn.exec_driver_sql(ddl)

        if args.full:
            date_rows = conn.execute(text("SELECT DISTINCT purchase_date FROM public.fact_orders ORDER BY 1"))
            dates = [r[0] for r in date_rows]
        else:
            dates = [args.date]

        for ds in dates:
            print(f"[metrics] building for {ds}")
            params = {"ds": str(ds)}
            conn.execute(INS_DAILY_SALES, params)
            conn.execute(INS_DELIVERY_KPIS, params)
            # review KPIs only if reviews table exists
            has_reviews = conn.execute(text("SELECT to_regclass('public.order_reviews') IS NOT NULL")).scalar()
            if has_reviews:
                conn.execute(INS_REVIEW_KPIS, params)
            conn.execute(INS_DAILY_CUSTOMER, params)
            if has_reviews:
                conn.execute(INS_SELLER_KPIS, params)
            print(f"[ok] {ds}")

    print("All done.")


if __name__ == "__main__":
    main()
