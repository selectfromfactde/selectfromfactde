
import os
from pathlib import Path

def test_layout_exists():
    assert Path("config/paths.yaml").exists()
    for p in ["data/raw","data/bronze","data/silver","data/gold"]:
        assert Path(p).exists()
