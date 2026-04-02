"""Australian Fuel Security Dashboard — Energy Minister's Decision Support Tool.

Multi-page Streamlit app. This file is the entry point / landing page.
Individual pages are in the pages/ directory.
"""

import streamlit as st
from config import is_offline, seed_refreshed_at
from dashboard_utils import render_data_freshness_sidebar

st.set_page_config(
    page_title="Australian Fuel Security Dashboard",
    page_icon="⛽",
    layout="wide",
)

# Landing page content (shown on the main app.py)
st.title("Australian Fuel Security Dashboard")
st.caption("Decision support for national fuel supply security")

render_data_freshness_sidebar()

if is_offline():
    ts = seed_refreshed_at()
    date_str = ts.strftime("%-d %b %Y") if ts else "unknown"
    st.info(
        f"**Offline mode** — displaying pre-loaded data current as at **{date_str}**. "
        "No live data is being fetched."
    )

st.markdown("""
Navigate using the sidebar to access:

- **Situation Room** — RAG status overview, alerts, key indicators at a glance
- **Supply Security** — Consumption cover days, stock levels, drawdown trends
- **IEA Obligation** — 90-day net import cover compliance tracking
- **Import Risk** — Source country concentration, HHI index, chokepoint analysis
- **Incoming Tankers** — Live tanker movements from 7 state port authorities
- **Wholesale Prices** — Brent crude, terminal gate prices, fuel futures
- **Tanker Tracker** — Live AIS vessel tracking (coming soon)
""")

# ── Offline pre-cache ─────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.markdown("**Offline / Pre-cache**")
if st.sidebar.button("Cache all data now", help="Fetch and save all data sources to disk so the dashboard works without internet"):
    from data_loader import load_brent_crude, load_fuel_futures, load_tgp_data, load_mso_weekly
    from port_scraper import scrape_all_ports
    from ais_tracker import fetch_ais_snapshot

    results = {}
    progress = st.sidebar.progress(0, text="Starting…")

    steps = [
        ("Brent crude (FRED)",   lambda: load_brent_crude()),
        ("Fuel futures (Yahoo)", lambda: load_fuel_futures()),
        ("Terminal gate prices", lambda: load_tgp_data()),
        ("MSO weekly stocks",    lambda: load_mso_weekly()),
        ("Port schedules",       lambda: scrape_all_ports()),
        ("AIS tanker positions", lambda: fetch_ais_snapshot()),
    ]

    for i, (label, fn) in enumerate(steps):
        progress.progress(i / len(steps), text=f"Fetching {label}…")
        try:
            fn()
            results[label] = "ok"
        except Exception as e:
            results[label] = f"failed: {e}"

    progress.progress(1.0, text="Done")

    ok = [k for k, v in results.items() if v == "ok"]
    failed = {k: v for k, v in results.items() if v != "ok"}
    if ok:
        st.sidebar.success(f"Cached: {', '.join(ok)}")
    if failed:
        for k, v in failed.items():
            st.sidebar.warning(f"{k}: {v}")

# Sidebar footer
st.sidebar.markdown("---")
st.sidebar.caption(
    "Data: [DCCEEW Australian Petroleum Statistics](https://data.gov.au/data/dataset/australian-petroleum-statistics) "
    "| Port Authority feeds (NSW, VIC, WA, QLD, SA, NT, TAS) "
    "| FRED | AIP | Yahoo Finance"
)
