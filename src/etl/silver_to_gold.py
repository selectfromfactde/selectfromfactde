
"""
Silver -> Gold transforms:
- Build star-schema facts/dims under data/gold/
- Compute daily marketplace metrics
"""
from pathlib import Path
from pyspark.sql import functions as F, Window as W
from src.utils.spark import get_spark
from src.utils.io import load_paths

def write_parquet(df, path):
    (df.write.mode("overwrite").parquet(str(path)))

def main():
    cfg = load_paths()
    spark = get_spark()
    silver = Path(cfg["silver_dir"])
    gold = Path(cfg["gold_dir"])

    orders = spark.read.parquet(str(silver / "orders"))
    items = spark.read.parquet(str(silver / "order_items"))
    pays  = spark.read.parquet(str(silver / "order_payments"))
    custs = spark.read.parquet(str(silver / "customers"))
    prods = spark.read.parquet(str(silver / "products"))
    sells = spark.read.parquet(str(silver / "sellers"))
    revs  = spark.read.parquet(str(silver / "order_reviews"))
    # Dates dim
    dates = (orders
             .select(F.to_date("order_purchase_timestamp").alias("date"))
             .distinct()
             .withColumn("year", F.year("date"))
             .withColumn("quarter", F.quarter("date"))
             .withColumn("month", F.month("date"))
             .withColumn("day", F.dayofmonth("date"))
             .withColumn("dow", F.date_format("date", "E"))
            )
    write_parquet(dates, gold / "dim_dates")

    # Dim customers/products/sellers (very light conformance here)
    write_parquet(custs, gold / "dim_customers")
    write_parquet(prods, gold / "dim_products")
    write_parquet(sells, gold / "dim_sellers")

    # fact_orders (one row per order)
    # Basic rollups from items and payments
    items_agg = (items
                 .groupBy("order_id")
                 .agg(F.count("*").alias("total_items"),
                      F.countDistinct("seller_id").alias("seller_count"),
                      F.sum("price").alias("items_total"),
                      F.sum("freight_value").alias("freight_total")))

    pays_agg = (pays
                .groupBy("order_id")
                .agg(F.sum("payment_value").alias("payment_total"),
                     F.count("*").alias("payment_instruments")))

    fact_orders = (orders
                   .join(items_agg, "order_id", "left")
                   .join(pays_agg, "order_id", "left")
                   .withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
                   .withColumn("delivered_on_time",
                        F.when(
                           (F.col("order_delivered_customer_date").isNotNull()) &
                           (F.col("order_estimated_delivery_date").isNotNull()) &
                           (F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date")),
                           F.lit(1)
                        ).otherwise(F.lit(0))
                   )
                  )
    write_parquet(fact_orders, gold / "fact_orders")

    # fact_order_items (row per item)
    foi = items.withColumn("purchase_date", F.to_date("shipping_limit_date"))
    write_parquet(foi, gold / "fact_order_items")

    # Daily metrics
    daily = (fact_orders
             .groupBy("purchase_date")
             .agg(
                F.countDistinct("order_id").alias("orders"),
                F.sum("payment_total").alias("gmv"),
                F.countDistinct("customer_id").alias("active_customers"),
                F.avg("delivered_on_time").alias("on_time_pct")
             )
             .withColumn("aov", F.col("gmv")/F.col("orders"))
            )

    # Avg review score by review creation day
    reviews_daily = (revs
                     .withColumn("review_day", F.to_date("review_creation_date"))
                     .groupBy("review_day")
                     .agg(F.avg("review_score").alias("avg_review"))
                    )

    daily = (daily.join(reviews_daily, daily.purchase_date == reviews_daily.review_day, "left")
                  .drop("review_day"))

    # Repeat purchase % (up to day): customers with >=2 lifetime orders up to that day / active customers that day
    cust_orders = (fact_orders
                   .select("customer_id", "purchase_date")
                   .groupBy("customer_id", "purchase_date").count())

    # cumulative orders per customer up to each date
    win = W.partitionBy("customer_id").orderBy("purchase_date") \
            .rowsBetween(W.unboundedPreceding, W.currentRow)
    lifetimes = (cust_orders
                 .withColumn("cum_orders", F.sum("count").over(win))
                 .groupBy("purchase_date")
                 .agg(F.sum(F.when(F.col("cum_orders") >= 2, 1).otherwise(0)).alias("repeat_customers"))
                )

    daily = (daily.join(lifetimes, "purchase_date", "left")
                  .withColumn("repeat_purchase_pct",
                              F.when(F.col("active_customers") > 0,
                                     F.col("repeat_customers")/F.col("active_customers"))
                               .otherwise(F.lit(0.0)))
                  .fillna({"avg_review": 0.0, "repeat_customers": 0, "repeat_purchase_pct": 0.0})
            )

    # Write partitioned by date for fast slicing
    (daily
     .repartition(1)
     .write
     .mode("overwrite")
     .partitionBy("purchase_date")
     .parquet(str(gold / "daily_marketplace_metrics")))

if __name__ == "__main__":
    main()
