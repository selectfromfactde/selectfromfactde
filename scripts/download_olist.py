"""
Helper to download the Olist dataset via Kaggle API.

Requires Kaggle credentials configured either as env vars or ~/.kaggle/kaggle.json
Docs: https://github.com/Kaggle/kaggle-api#api-credentials

Usage:
  source .venv/bin/activate
  python scripts/download_olist.py
"""
from pathlib import Path
import zipfile
import os
import sys

try:
    from kaggle import api as kaggle_api
except Exception as e:
    print("kaggle package not installed. Run: pip install kaggle")
    sys.exit(2)


DATASET = "olistbr/brazilian-ecommerce"


def main():
    raw_dir = Path("data/raw/olist").resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)

    print("Downloading Olist dataset from Kaggle (may take a few minutes)...")
    # Download to a temp zip in raw dir
    zip_path = raw_dir / "olist.zip"
    kaggle_api.dataset_download_files(DATASET, path=str(raw_dir), quiet=False)

    # The API names the zip as <slug>.zip in the target dir
    # Find the first *.zip in raw_dir
    zips = list(raw_dir.glob("*.zip"))
    if not zips:
        print("No zip file found in raw dir after download.")
        sys.exit(1)
    zip_path = zips[0]
    print(f"Extracting {zip_path} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(str(raw_dir))
    # Optionally remove zip
    try:
        zip_path.unlink()
    except Exception:
        pass

    print("Done. CSVs are under data/raw/olist/")


if __name__ == "__main__":
    main()

