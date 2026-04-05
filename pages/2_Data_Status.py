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

# ── Plain-English explanation ─────────────────────────────────
with st.expander("How does data refresh work?", expanded=False):
    st.markdown("""
**Live cache** (`data/`) — fetched from the internet each time you load a page or hit Refresh.
Stored locally so the app doesn't re-fetch on every click. Expires after the TTL (e.g. 6 h).

**Seed snapshot** (`seed/`) — a committed baseline copy of all data.
Used as a fallback when there's no live cache, or when running in offline mode (Streamlit Cloud).
Stays fixed until you run `uv run python refresh_seed.py` and commit the result.

**Offline mode** (Streamlit Cloud) — live fetches are disabled; seed data is always served.
The "Seed snapshot" date is what users on the cloud deployment see.

**The Refresh button below** re-fetches all sources right now and updates the live cache.
It has no effect on the committed seed files.
""")

# ── Seed manifest ─────────────────────────────────────────────
seed_ts = seed_refreshed_at()
seed_date = seed_ts.strftime("%d %b %Y %H:%M UTC") if seed_ts else "unknown"
st.info(
    f"**Seed snapshot** (cloud baseline) last refreshed: **{seed_date}**  ·  "
    f"To update: run `uv run python refresh_seed.py` locally and commit `seed/`."
)

st.divider()

# ── Precision Shipping Positions (ShipNext) ───────────────────
st.subheader("🗺️ Precision Shipping Positions")
st.caption(
    "Fetch live vessel positions from ShipNext.com using real browser rendering. "
    "Each vessel takes ~5–10 seconds. With 50+ tankers, this can take 1+ hour. "
    "Results are cached for 5 minutes."
)

col_load, col_info = st.columns([1, 4])
with col_load:
    load_positions = st.button("⚡ Load All Positions", use_container_width=True)

with col_info:
    st.caption("Caches results to `data/shipnext_positions.json`")

if load_positions:
    from port_scraper import scrape_all_ports
    from vessel_lookup import VesselCache
    from shipnext_scraper import fetch_all_vessel_positions

    with st.spinner("Fetching port schedule and enriching vessel specs..."):
        try:
            port_df = scrape_all_ports(tankers_only=True)
            if port_df is None or len(port_df) == 0:
                st.error("Port scraper returned no data.")
                vessels = []
            else:
                # Enrich with vessel specs (IMO, GT, DWT, etc.)
                cache = VesselCache()
                port_df = cache.enrich_dataframe(port_df)
                vessels = [dict(row) for row in port_df.iter_rows(named=True)]
        except Exception as e:
            st.error(f"Failed to load port data: {e}")
            import traceback
            st.code(traceback.format_exc())
            vessels = []

    st.info(f"Port schedule returned {len(vessels)} total vessels (all types).")

    if vessels:
        # Debug: show what columns we have
        first_vessel = vessels[0]
        st.caption(f"Available fields: {', '.join(first_vessel.keys())}")

        with_imo = [v for v in vessels if (v.get("v_imo") or "").strip()]
        st.info(f"**{len(with_imo)} vessels with IMO numbers.** Starting fetch...")

        if with_imo:
            progress_bar = st.progress(0, text="Starting...")
            status_text = st.empty()

            def _progress(i, total, name):
                pct = (i + 1) / total
                progress_bar.progress(pct, text=f"{i + 1}/{total} · {name}")
                status_text.text(f"⏳ Fetching position for {name}...")

            results, errors = fetch_all_vessel_positions(with_imo, progress_callback=_progress)

            progress_bar.progress(1.0, text="✅ Complete!")
            status_text.text("")

            st.success(
                f"✅ **{len(results)} vessel positions loaded** "
                f"({len(errors)} failed to fetch). "
                f"Saved to `data/shipnext_positions.json` — now live on Incoming Tankers & Tanker Tracker pages."
            )

            # Show loaded vessels
            if results:
                with st.expander(f"✅ Loaded positions ({len(results)})"):
                    for imo, (lat, lon) in sorted(results.items()):
                        st.text(f"IMO {imo}: {lat:.4f}°, {lon:.4f}°")

            # Show errors
            if errors:
                with st.expander(f"⚠️ Failed fetches ({len(errors)})"):
                    for imo, name, reason in errors[:20]:  # Show first 20
                        st.text(f"**{name}** (IMO {imo}): {reason}")
                    if len(errors) > 20:
                        st.text(f"... and {len(errors) - 20} more")
        else:
            st.warning("No vessels with IMO numbers in the scraped data.")
    else:
        st.error("Port scraper returned empty list.")

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
    now = datetime.now(tz=timezone.utc)
    if runtime.suffix == ".json":
        cache = _read_json_cache(runtime)
        if cache:
            ts = _fetched_at(cache) or _file_mtime(runtime)
            if ttl is None or (ts and (now - ts) < ttl):
                return "✅", "Live"
            return "⚠️", "Stale"
    elif runtime.exists():
        ts = _file_mtime(runtime)
        if ttl is None or (ts and (now - ts) < ttl):
            return "✅", "Live"
        return "⚠️", "Stale"
    if seed and seed.exists():
        return "🌱", "Seed"
    return "❌", "Missing"


def _last_updated(runtime: Path, seed: Path | None) -> str:
    now = datetime.now(tz=timezone.utc)

    def _fmt(ts: datetime) -> str:
        age = now - ts
        if age < timedelta(hours=1):
            return f"{int(age.total_seconds() / 60)}m ago"
        if age < timedelta(hours=48):
            return f"{int(age.total_seconds() / 3600)}h ago"
        return ts.strftime("%d %b %Y")

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

    if seed and seed.exists() and seed_ts:
        return f"{_fmt(seed_ts)} (seed)"
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
        "fetch": lambda: __import__("data_loader").load_mso_weekly(),
    },
    {
        "Source": "Brent Crude",
        "Provider": "FRED (St. Louis Fed)",
        "Frequency": "6 h",
        "ttl": timedelta(hours=6),
        "runtime": Path("data/brent_prices.json"),
        "seed": Path("seed/brent_prices.json"),
        "fetch": lambda: __import__("data_loader").load_brent_crude(),
    },
    {
        "Source": "Fuel Futures",
        "Provider": "Yahoo Finance",
        "Frequency": "6 h",
        "ttl": timedelta(hours=6),
        "runtime": Path("data/futures.json"),
        "seed": Path("seed/futures.json"),
        "fetch": lambda: __import__("data_loader").load_fuel_futures(),
    },
    {
        "Source": "Terminal Gate Prices",
        "Provider": "AIP",
        "Frequency": "24 h",
        "ttl": timedelta(hours=24),
        "runtime": Path("data/aip_tgp.json"),
        "seed": Path("seed/aip_tgp.json"),
        "fetch": lambda: __import__("data_loader").load_tgp_data(),
    },
    {
        "Source": "Port Schedules",
        "Provider": "7 state port authorities",
        "Frequency": "3 h",
        "ttl": timedelta(hours=3),
        "runtime": Path("data/port_schedule.json"),
        "seed": Path("seed/port_schedule.json"),
        "fetch": lambda: __import__("port_scraper").scrape_all_ports(),
    },
    {
        "Source": "AIS Vessel Positions",
        "Provider": "AISStream.io",
        "Frequency": "12 h",
        "ttl": timedelta(hours=12),
        "runtime": Path("data/ais_positions.json"),
        "seed": None,
        "fetch": lambda: __import__("ais_tracker").fetch_ais_snapshot(),
    },
    {
        "Source": "APS Workbook",
        "Provider": "DCCEEW / data.gov.au",
        "Frequency": "Monthly",
        "ttl": timedelta(days=35),
        "runtime": Path("data/australian-petroleum-statistics.xlsx"),
        "seed": Path("seed/australian-petroleum-statistics.xlsx"),
        "fetch": None,  # copied from seed; no live fetch
    },
    {
        "Source": "Vessel Spec Cache",
        "Provider": "VesselFinder (on-demand)",
        "Frequency": "On demand",
        "ttl": None,
        "runtime": Path("data/vessel_cache.json"),
        "seed": None,
        "fetch": None,  # populated automatically when vessels are looked up
    },
]


# ── Refresh button ────────────────────────────────────────────

def _do_refresh():
    """Delete stale caches and re-fetch all refreshable sources."""
    refreshable = [s for s in SOURCES if s["fetch"] is not None]
    progress = st.progress(0, text="Starting refresh…")
    results = {}
    for i, s in enumerate(refreshable):
        label = s["Source"]
        progress.progress(i / len(refreshable), text=f"Fetching {label}…")
        # Delete cache so the loader fetches fresh regardless of TTL
        if s["runtime"].exists() and s["runtime"].suffix == ".json":
            s["runtime"].unlink(missing_ok=True)
        try:
            s["fetch"]()
            results[label] = "✅"
        except Exception as e:
            results[label] = f"❌ {e}"
    progress.progress(1.0, text="Done")
    # Clear Streamlit's in-memory cache so pages re-read from disk
    st.cache_data.clear()
    return results


col_refresh, col_note = st.columns([1, 4])
with col_refresh:
    refresh_disabled = is_offline()
    if st.button("🔄 Refresh all data", disabled=refresh_disabled, use_container_width=True):
        results = _do_refresh()
        for label, status in results.items():
            if status == "✅":
                st.success(f"{status} {label}")
            else:
                st.error(f"{status} {label}")
        st.rerun()

with col_note:
    if is_offline():
        st.caption("Refresh is disabled in offline mode — data is served from seed snapshot.")
    else:
        st.caption("Deletes stale caches and re-fetches all sources now. AIS takes ~45 s.")


# ── Status table ──────────────────────────────────────────────

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

st.caption(
    "✅ **Live** — runtime cache within TTL  ·  "
    "⚠️ **Stale** — cache past TTL  ·  "
    "🌱 **Seed** — serving committed baseline  ·  "
    "❌ **Missing** — no cache and no seed"
)

# ── Per-port scrape breakdown ─────────────────────────────────
st.subheader("Port Schedules — per-port detail")

from port_scraper import get_port_scrape_status  # noqa: E402

# Primary URL shown in the clickable link column (one per scraper)
_PORT_PRIMARY_URL = {
    "NSW":       "https://www.portauthoritynsw.com.au/port-operations/",
    "VIC":       "https://geelongport.powerappsportals.com/Shipping/",
    "Melbourne": "https://ports.vic.gov.au/marine-operations/ship-movements/",
    "WA":        "https://www3.fremantleports.com.au/vtmis",
    "QLD":       "https://qships.tmr.qld.gov.au/webx",
    "SA":        "https://portmis.flindersports.com.au",
    "NT":        "https://portinfo.darwinport.com.au/webx",
    "TAS":       "https://tasports.com.au",
}

# Full list of pages scraped per scraper (for the expander)
_PORT_ALL_URLS = {
    "NSW": [
        ("Sydney Harbour daily movements", "https://www.portauthoritynsw.com.au/port-operations/sydney-harbour/sydney-harbour-daily-vessel-movements"),
        ("Port Botany daily movements", "https://www.portauthoritynsw.com.au/port-operations/port-botany/port-botany-daily-vessel-movements"),
        ("Port Kembla daily movements", "https://www.portauthoritynsw.com.au/port-operations/port-kembla/port-kembla-daily-vessel-movements"),
        ("Newcastle daily movements", "https://www.portauthoritynsw.com.au/port-operations/newcastle-harbour/newcastle-harbour-daily-vessel-movements"),
    ],
    "VIC": [
        ("Geelong Port shipping schedule (Viva Energy PowerApps)", "https://geelongport.powerappsportals.com/Shipping/"),
    ],
    "Melbourne": [
        ("Ports Victoria ship movements (Melbourne + Geelong)", "https://ports.vic.gov.au/marine-operations/ship-movements/"),
    ],
    "WA": [
        ("Fremantle Ports VTMIS vessel movements", "https://www3.fremantleports.com.au/vtmis"),
    ],
    "QLD": [
        ("QShips — Maritime Safety Queensland (all QLD ports)", "https://qships.tmr.qld.gov.au/webx"),
    ],
    "SA": [
        ("Flinders Ports PortMIS — expected movements", "https://portmis.flindersports.com.au"),
        ("Flinders Ports PortMIS — in-port vessels", "https://portmis.flindersports.com.au"),
    ],
    "NT": [
        ("Darwin Port schedule", "https://portinfo.darwinport.com.au/webx"),
        ("Darwin Port in-port vessels", "https://portinfo.darwinport.com.au/webx"),
    ],
    "TAS": [
        ("TasPorts expected shipping", "https://tasports.com.au"),
        ("TasPorts vessels in port", "https://tasports.com.au"),
    ],
}

port_status = get_port_scrape_status()
if port_status:
    import pandas as pd
    port_rows = []
    for state, info in port_status.items():
        n_pages = len(_PORT_ALL_URLS.get(state, []))
        page_note = f"{n_pages} page{'s' if n_pages > 1 else ''}" if n_pages else ""
        if info.get("ok"):
            port_rows.append({
                "State": state,
                "Status": "✅ OK",
                "Vessels loaded": info.get("count", "?"),
                "Pages scraped": page_note,
                "URL": _PORT_PRIMARY_URL.get(state, ""),
                "Error": "",
            })
        else:
            port_rows.append({
                "State": state,
                "Status": "❌ Failed",
                "Vessels loaded": 0,
                "Pages scraped": page_note,
                "URL": _PORT_PRIMARY_URL.get(state, ""),
                "Error": info.get("error", "unknown error"),
            })
    port_df = pd.DataFrame(port_rows)
    st.dataframe(
        port_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "URL": st.column_config.LinkColumn("Website", display_text="Open ↗"),
        },
    )

    with st.expander("All scraped URLs"):
        for state, pages in _PORT_ALL_URLS.items():
            st.markdown(f"**{state}**")
            for label, url in pages:
                st.markdown(f"- [{label}]({url})")

    st.caption("Status from the last scrape run. Hit **Refresh all data** above to re-scrape.")
else:
    st.info("No scrape run recorded yet — hit **Refresh all data** above to populate port status.")
