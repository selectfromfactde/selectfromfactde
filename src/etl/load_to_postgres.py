"""
Load Gold layer Parquet tables into Postgres for BI.

Usage:
  source .venv/bin/activate
  python -m src.etl.load_to_postgres --conn postgresql+psycopg2://analytics:analytics@localhost:5432/olist

Notes:
  - Expects Gold outputs already produced by silver_to_gold.py
  - Uses pandas.to_sql (if_exists=replace) for simplicity.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
from sqlalchemy import create_engine
from src.utils.io import load_paths


GOLD_TABLES = [
    "dim_dates",
    "dim_customers",
    "dim_products",
    "dim_sellers",
    "fact_orders",
    "fact_order_items",
    # partitioned table
    "daily_marketplace_metrics",
]

# Optionally load a couple of Silver tables that downstream metrics may need.
SILVER_OPTIONAL = [
    ("order_reviews", "silver"),
]


def _read_parquet_any(path: Path) -> pd.DataFrame:
    """Read a Parquet directory or file into pandas.
    Handles Spark-style partitioned directories via pyarrow.dataset.
    """
    if path.is_dir():
        # Use dataset to capture partition columns (e.g., purchase_date=YYYY-MM-DD)
        dataset = ds.dataset(str(path), format="parquet")
        table = dataset.to_table()
        return table.to_pandas()
    else:
        return pd.read_parquet(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conn", required=True, help="SQLAlchemy URI, e.g. postgresql+psycopg2://user:pass@host:5432/db")
    ap.add_argument("--schema", default="public", help="Target schema")
    args = ap.parse_args()

    cfg = load_paths()
    gold_root = Path(cfg["gold_dir"]).resolve()
    silver_root = Path(cfg["silver_dir"]).resolve()

    engine = create_engine(args.conn)
    with engine.begin() as conn:
        for t in GOLD_TABLES:
            path = gold_root / t
            if not path.exists():
                print(f"[skip] {t}: not found at {path}")
                continue
            print(f"[load] {t} from {path}")
            df = _read_parquet_any(path)
            # Normalize column names to lower snake (already the case, but be safe)
            df.columns = [c.lower() for c in df.columns]
            # Write; replace table each run for determinism
            df.to_sql(t, conn, schema=args.schema, if_exists="replace", index=False, method=None)
            print(f"[ok] {t}: {len(df):,} rows")

        # Try loading optional Silver tables if present (e.g., reviews for metrics)
        for t, layer in SILVER_OPTIONAL:
            path = (silver_root if layer == "silver" else gold_root) / t
            if not path.exists():
                print(f"[skip] {t} (optional): not found at {path}")
                continue
            print(f"[load] {t} from {path}")
            df = _read_parquet_any(path)
            df.columns = [c.lower() for c in df.columns]
            df.to_sql(t, conn, schema=args.schema, if_exists="replace", index=False, method=None)
            print(f"[ok] {t}: {len(df):,} rows")

    print("All done.")


if __name__ == "__main__":
    main()
