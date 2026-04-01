"""Refresh seed/ data snapshots from live sources. Run before deploying."""

import shutil
from pathlib import Path

from data_loader import load_brent_crude, load_fuel_futures, load_mso_weekly, load_tgp_data

SOURCES = [
    ("MSO weekly stocks",    load_mso_weekly,    "data/mso_weekly.json",   "seed/mso_weekly.json"),
    ("Brent crude (FRED)",   load_brent_crude,   "data/brent_prices.json", "seed/brent_prices.json"),
    ("Fuel futures (Yahoo)", load_fuel_futures,  "data/futures.json",      "seed/futures.json"),
    ("Terminal gate prices", load_tgp_data,      "data/aip_tgp.json",      "seed/aip_tgp.json"),
]

Path("seed").mkdir(exist_ok=True)

for label, fn, cache, seed in SOURCES:
    try:
        fn()
        shutil.copy2(cache, seed)
        print(f"  ok  {label}")
    except Exception as e:
        print(f"  FAIL {label}: {e}")

print("\nDone. Commit seed/ before deploying.")
