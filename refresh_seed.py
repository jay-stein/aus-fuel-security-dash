"""Refresh seed/ data snapshots from live sources. Run before deploying."""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from data_loader import load_brent_crude, load_fuel_futures, load_mso_weekly, load_tgp_data
from port_scraper import scrape_all_ports

Path("seed").mkdir(exist_ok=True)

# ── JSON data sources (fetched live) ──────────────────────────
SOURCES = [
    ("MSO weekly stocks",    load_mso_weekly,    "data/mso_weekly.json",   "seed/mso_weekly.json"),
    ("Brent crude (FRED)",   load_brent_crude,   "data/brent_prices.json", "seed/brent_prices.json"),
    ("Fuel futures (Yahoo)", load_fuel_futures,  "data/futures.json",      "seed/futures.json"),
    ("Terminal gate prices", load_tgp_data,      "data/aip_tgp.json",      "seed/aip_tgp.json"),
    ("Port schedules",       scrape_all_ports,   "data/port_schedule.json","seed/port_schedule.json"),
]

for label, fn, cache, seed in SOURCES:
    try:
        fn()
        shutil.copy2(cache, seed)
        print(f"  ok  {label}")
    except Exception as e:
        print(f"  FAIL {label}: {e}")

# ── APS workbook (download manually from data.gov.au if stale) ──
APS_SRC = Path("data/australian-petroleum-statistics.xlsx")
APS_SEED = Path("seed/australian-petroleum-statistics.xlsx")

if APS_SRC.exists():
    shutil.copy2(APS_SRC, APS_SEED)
    size_mb = APS_SRC.stat().st_size / 1_000_000
    print(f"  ok  APS workbook ({size_mb:.1f} MB)")
else:
    print(f"  SKIP APS workbook — not found at {APS_SRC}")
    print(f"       Download from data.gov.au and save to {APS_SRC}, then re-run.")

# ── Manifest ──────────────────────────────────────────────────
manifest = {"refreshed_at": datetime.now(tz=timezone.utc).isoformat()}
Path("seed/manifest.json").write_text(json.dumps(manifest, indent=2))
print(f"  ok  manifest ({manifest['refreshed_at']})")

print("\nDone. Commit seed/ before deploying.")
