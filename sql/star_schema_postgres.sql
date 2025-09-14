
-- Optional Postgres DDL sketch if you want to load Gold to a warehouse
CREATE TABLE IF NOT EXISTS dim_dates(
  date date PRIMARY KEY,
  year int, quarter int, month int, day int, dow text
);

CREATE TABLE IF NOT EXISTS fact_orders(
  order_id text PRIMARY KEY,
  customer_id text,
  purchase_date date,
  total_items int,
  seller_count int,
  items_total numeric,
  freight_total numeric,
  payment_total numeric,
  delivered_on_time int
);

-- And so on for other dims/facts...
