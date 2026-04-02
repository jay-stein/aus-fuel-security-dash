"""Data Source Status — cache freshness and scrape status for all data sources."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import streamlit as st

from config import is_offline, seed_refreshed_at
from dashboard_utils import render_data_freshness_sidebar

st.set_page_config(page_title="Data Status", page_icon="📡", layout="wide")
render_data_freshness_sidebar()
st.title("Data Source Status")
st.caption("Cache state and freshness for every data source used by the dashboard.")

# ── Seed manifest ─────────────────────────────────────────────
seed_ts = seed_refreshed_at()
seed_date = seed_ts.strftime("%d %b %Y %H:%M UTC") if seed_ts else "unknown"
st.info(f"**Seed snapshot** last refreshed: **{seed_date}**  ·  "
        f"Run `uv run python refresh_seed.py` locally and commit `seed/` to update.")

st.divider()


# ── Helpers ───────────────────────────────────────────────────

def _read_json_cache(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _fetched_at(cache: dict, key: str = "fetched_at") -> datetime | None:
    try:
        return datetime.fromisoformat(cache[key])
    except Exception:
        return None


def _file_mtime(path: Path) -> datetime | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except Exception:
        return None


def _status(runtime: Path, seed: Path | None, ttl: timedelta | None) -> tuple[str, str]:
    """Return (icon, label) for this source."""
    now = datetime.now(tz=timezone.utc)

    if runtime.suffix == ".json":
        cache = _read_json_cache(runtime)
        if cache:
            # Try fetched_at field first, fall back to file mtime
            ts = _fetched_at(cache) or _file_mtime(runtime)
            if ts and ttl and (now - ts) < ttl:
                return "✅", "Live"
            return "⚠️", "Stale"
    elif runtime.exists():
        # Binary file (xlsx) — check mtime
        ts = _file_mtime(runtime)
        if ts and ttl and (now - ts) < ttl:
            return "✅", "Live"
        return "⚠️", "Stale"

    if seed and seed.exists():
        return "🌱", "Seed"
    return "❌", "Missing"


def _last_updated(runtime: Path, seed: Path | None) -> str:
    """Human-readable last-updated string."""
    now = datetime.now(tz=timezone.utc)

    def _fmt(ts: datetime) -> str:
        age = now - ts
        if age < timedelta(hours=1):
            return f"{int(age.total_seconds() / 60)}m ago"
        if age < timedelta(hours=48):
            return f"{int(age.total_seconds() / 3600)}h ago"
        return ts.strftime("%d %b %Y")

    # Check runtime cache
    if runtime.suffix == ".json":
        cache = _read_json_cache(runtime)
        if cache:
            ts = _fetched_at(cache) or _file_mtime(runtime)
            if ts:
                return _fmt(ts)
    elif runtime.exists():
        ts = _file_mtime(runtime)
        if ts:
            return _fmt(ts)

    # Fall back to seed manifest date
    if seed and seed.exists():
        if seed_ts:
            return f"{_fmt(seed_ts)} (seed)"
        return "seed"

    return "—"


# ── Source definitions ────────────────────────────────────────

SOURCES = [
    {
        "Source": "MSO Weekly Stocks",
        "Provider": "DCCEEW Power BI",
        "Frequency": "Weekly",
        "ttl": timedelta(hours=6),
        "runtime": Path("data/mso_weekly.json"),
        "seed": Path("seed/mso_weekly.json"),
    },
    {
        "Source": "Brent Crude",
        "Provider": "FRED (St. Louis Fed)",
        "Frequency": "6 h",
        "ttl": timedelta(hours=6),
        "runtime": Path("data/brent_prices.json"),
        "seed": Path("seed/brent_prices.json"),
    },
    {
        "Source": "Fuel Futures",
        "Provider": "Yahoo Finance",
        "Frequency": "6 h",
        "ttl": timedelta(hours=6),
        "runtime": Path("data/futures.json"),
        "seed": Path("seed/futures.json"),
    },
    {
        "Source": "Terminal Gate Prices",
        "Provider": "AIP",
        "Frequency": "24 h",
        "ttl": timedelta(hours=24),
        "runtime": Path("data/aip_tgp.json"),
        "seed": Path("seed/aip_tgp.json"),
    },
    {
        "Source": "Port Schedules",
        "Provider": "7 state port authorities",
        "Frequency": "3 h",
        "ttl": timedelta(hours=3),
        "runtime": Path("data/port_schedule.json"),
        "seed": Path("seed/port_schedule.json"),
    },
    {
        "Source": "AIS Vessel Positions",
        "Provider": "AISStream.io",
        "Frequency": "12 h",
        "ttl": timedelta(hours=12),
        "runtime": Path("data/ais_positions.json"),
        "seed": None,
    },
    {
        "Source": "APS Workbook",
        "Provider": "DCCEEW / data.gov.au",
        "Frequency": "Monthly",
        "ttl": timedelta(days=35),
        "runtime": Path("data/australian-petroleum-statistics.xlsx"),
        "seed": Path("seed/australian-petroleum-statistics.xlsx"),
    },
    {
        "Source": "Vessel Spec Cache",
        "Provider": "VesselFinder (on-demand)",
        "Frequency": "On demand",
        "ttl": None,
        "runtime": Path("data/vessel_cache.json"),
        "seed": None,
    },
]

# ── Build table ───────────────────────────────────────────────

rows = []
for s in SOURCES:
    icon, label = _status(s["runtime"], s["seed"], s["ttl"])
    last = _last_updated(s["runtime"], s["seed"])
    rows.append({
        "Status": f"{icon} {label}",
        "Source": s["Source"],
        "Provider": s["Provider"],
        "Refresh": s["Frequency"],
        "Last updated": last,
    })

df = pl.DataFrame(rows)
st.dataframe(df.to_pandas(), use_container_width=True, hide_index=True)

# ── Legend ────────────────────────────────────────────────────
st.caption(
    "✅ **Live** — runtime cache exists and is within TTL  ·  "
    "⚠️ **Stale** — cache exists but past TTL  ·  "
    "🌱 **Seed** — serving committed seed snapshot  ·  "
    "❌ **Missing** — no cache and no seed"
)

if is_offline():
    st.warning(
        "Running in **offline mode** — all sources serve seed data. "
        "Live fetches are disabled. To refresh, run `uv run python refresh_seed.py` "
        "locally and commit the updated `seed/` directory."
    )
