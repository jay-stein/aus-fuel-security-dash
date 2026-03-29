"""Australian Fuel Security Dashboard — Energy Minister's Decision Support Tool.

Multi-page Streamlit app. This file is the entry point / landing page.
Individual pages are in the pages/ directory.
"""

import streamlit as st

st.set_page_config(
    page_title="Australian Fuel Security Dashboard",
    page_icon="⛽",
    layout="wide",
)

# Landing page content (shown on the main app.py)
st.title("Australian Fuel Security Dashboard")
st.caption("Decision support for national fuel supply security")

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

# Sidebar footer
st.sidebar.markdown("---")
st.sidebar.caption(
    "Data: [DCCEEW Australian Petroleum Statistics](https://data.gov.au/data/dataset/australian-petroleum-statistics) "
    "| Port Authority feeds (NSW, VIC, WA, QLD, SA, NT, TAS) "
    "| FRED | AIP | Yahoo Finance"
)
