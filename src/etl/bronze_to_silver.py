
"""
Bronze -> Silver pipeline for Olist.
- Bronze step is pass-through (raw CSVs live under data/raw/olist).
- Silver step reads raw CSVs, parses types, standardizes column names, and writes Parquet.
Run:
  python -m src.etl.bronze_to_silver --mode silver
"""
import argparse
from pathlib import Path
from pyspark.sql import functions as F, types as T
from src.utils.spark import get_spark
from src.utils.io import load_paths

def std_cols(df):
    # simple snake_case by lowercasing and replacing spaces/uppercase; Olist already snake_case
    return df

def read_csv(spark, path):
    return (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(path)))

def write_parquet(df, path):
    (df.write.mode("overwrite").parquet(str(path)))

def build_silver():
    cfg = load_paths()
    spark = get_spark()
    raw = Path(cfg["raw_dir"])

    orders = read_csv(spark, raw / "olist_orders_dataset.csv") \
        .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp")) \
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at")) \
        .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date")) \
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date")) \
        .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))

    items = read_csv(spark, raw / "olist_order_items_dataset.csv")
    payments = read_csv(spark, raw / "olist_order_payments_dataset.csv")
    customers = read_csv(spark, raw / "olist_customers_dataset.csv")
    reviews = read_csv(spark, raw / "olist_order_reviews_dataset.csv") \
        .withColumn("review_creation_date", F.to_timestamp("review_creation_date")) \
        .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
    products = read_csv(spark, raw / "olist_products_dataset.csv")
    sellers = read_csv(spark, raw / "olist_sellers_dataset.csv")
    geo = read_csv(spark, raw / "olist_geolocation_dataset.csv")

    # Write silver tables
    silver = Path(cfg["silver_dir"])
    write_parquet(orders, silver / "orders")
    write_parquet(items, silver / "order_items")
    write_parquet(payments, silver / "order_payments")
    write_parquet(customers, silver / "customers")
    write_parquet(reviews, silver / "order_reviews")
    write_parquet(products, silver / "products")
    write_parquet(sellers, silver / "sellers")
    write_parquet(geo, silver / "geolocation")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["bronze", "silver"], default="silver")
    args = parser.parse_args()
    if args.mode == "silver":
        build_silver()
    else:
        print("Bronze step is pass-through; place raw CSVs under data/raw/olist")

if __name__ == "__main__":
    main()
