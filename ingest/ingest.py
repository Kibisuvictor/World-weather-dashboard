"""
ingest/ingest.py
Downloads the GlobalWeatherRepository dataset from Kaggle,
saves raw CSV to data/raw/, then stamps a metadata file.
"""

import os
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
import kaggle  # pip install kaggle

# ---------------------------------------------------------------------------
DATASET    = "nelgiriyewithana/global-weather-repository"
RAW_DIR    = Path("data/raw")
META_FILE  = RAW_DIR / "ingest_meta.json"
TARGET_CSV = RAW_DIR / "GlobalWeatherRepository.csv"
# ---------------------------------------------------------------------------

def md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[{datetime.now()}] Authenticating with Kaggle API…")
    kaggle.api.authenticate()

    # Record file hash before download to detect changes
    old_hash = md5(TARGET_CSV) if TARGET_CSV.exists() else None

    print(f"[{datetime.now()}] Downloading dataset: {DATASET}")
    kaggle.api.dataset_download_files(
        DATASET,
        path=str(RAW_DIR),
        unzip=True,
        quiet=False,
        force=True      # always refresh
    )

    if not TARGET_CSV.exists():
        raise FileNotFoundError(
            f"Expected file not found after download: {TARGET_CSV}\n"
            "Check the dataset name or CSV filename on Kaggle."
        )

    new_hash = md5(TARGET_CSV)
    changed  = old_hash != new_hash

    # Write metadata for audit / cache-busting
    meta = {
        "dataset"        : DATASET,
        "downloaded_at"  : datetime.now(timezone.utc).isoformat(),
        "file"           : str(TARGET_CSV),
        "md5"            : new_hash,
        "data_changed"   : changed,
        "previous_md5"   : old_hash,
    }
    META_FILE.write_text(json.dumps(meta, indent=2))

    size_mb = TARGET_CSV.stat().st_size / 1_048_576
    print(f"[{datetime.now()}] Done.")
    print(f"  File   : {TARGET_CSV}  ({size_mb:.1f} MB)")
    print(f"  MD5    : {new_hash}")
    print(f"  Changed: {changed}")

if __name__ == "__main__":
    main()
