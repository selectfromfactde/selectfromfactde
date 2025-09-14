
from pathlib import Path
import yaml

def load_paths(config_path: str = "config/paths.yaml"):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Normalize
    for k in ["data_root","raw_dir","bronze_dir","silver_dir","gold_dir"]:
        cfg[k] = str(Path(cfg[k]).resolve())
    return cfg
