"""Wholesale Prices & Benchmarks — Brent, TGP, fuel futures."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from data_loader import load_brent_crude, load_fuel_futures, load_tgp_data
from dashboard_utils import BRENT_AMBER, BRENT_RED
import polars as pl

st.set_page_config(page_title="Wholesale Prices", page_icon="💲", layout="wide")
from dashboard_utils import render_data_freshness_sidebar, render_page_data_freshness
render_data_freshness_sidebar()
st.title("Wholesale Prices & Benchmarks")
render_page_data_freshness([
    ("Brent", "data/brent_prices.json", 6),
    ("Futures", "data/futures.json", 6),
    ("TGP", "data/aip_tgp.json", 24),
])
st.caption(
    "Brent crude drives global oil prices. Australian wholesale fuel is priced off "
    "Singapore benchmarks (MOGAS 95 for petrol, Gasoil 10ppm for diesel) with a ~10-day lag. "
    "TGP = Terminal Gate Price (AIP wholesale, inc. GST)."
)

two_weeks_view = st.toggle("Last 2 weeks only", value=False)
days_window = 14 if two_weeks_view else 180

# Load all price data
with st.spinner("Fetching wholesale price data..."):
    try:
        brent = load_brent_crude(days=180)
    except Exception as e:
        brent = None
        st.warning(f"Could not load Brent crude data: {e}")
    try:
        futures = load_fuel_futures(days=180)
    except Exception as e:
        futures = None
        st.warning(f"Could not load fuel futures data: {e}")
    try:
        petrol_tgp, diesel_tgp = load_tgp_data(days=180)
    except Exception as e:
        petrol_tgp = diesel_tgp = None
        st.warning(f"Could not load AIP TGP data: {e}")

# Apply 2-week filter
if two_weeks_view:
    cutoff = datetime.now().date() - timedelta(days=14)
    if brent is not None:
        brent = brent.filter(pl.col("date") >= cutoff)
    if futures is not None:
        futures = futures.filter(pl.col("date") >= cutoff)
    if petrol_tgp is not None:
        petrol_tgp = petrol_tgp.filter(pl.col("date") >= cutoff)
    if diesel_tgp is not None:
        diesel_tgp = diesel_tgp.filter(pl.col("date") >= cutoff)

# Key Metrics
m1, m2, m3, m4 = st.columns(4)
with m1:
    if brent is not None and len(brent) > 0:
        latest_b = brent["brent_usd"].to_list()[-1]
        prev_b = brent["brent_usd"].to_list()[-min(10, len(brent))]
        st.metric("Brent Crude", f"${latest_b:.2f}/bbl", delta=f"{latest_b - prev_b:+.2f}")
with m2:
    if petrol_tgp is not None and len(petrol_tgp) > 0:
        latest_p = petrol_tgp["National Average"].to_list()[-1]
        prev_p = petrol_tgp["National Average"].to_list()[-min(10, len(petrol_tgp))]
        st.metric("AU Petrol TGP", f"{latest_p:.1f} c/L", delta=f"{latest_p - prev_p:+.1f}")
with m3:
    if diesel_tgp is not None and len(diesel_tgp) > 0:
        latest_d = diesel_tgp["National Average"].to_list()[-1]
        prev_d = diesel_tgp["National Average"].to_list()[-min(10, len(diesel_tgp))]
        st.metric("AU Diesel TGP", f"{latest_d:.1f} c/L", delta=f"{latest_d - prev_d:+.1f}")
with m4:
    if futures is not None and len(futures) > 0:
        latest_r = futures["rbob_usd"].to_list()[-1]
        prev_r = futures["rbob_usd"].to_list()[-min(10, len(futures))]
        st.metric("RBOB Gasoline", f"${latest_r:.3f}/gal", delta=f"{latest_r - prev_r:+.3f}")

st.divider()

# Brent Crude Oil with threshold lines
if brent is not None and len(brent) > 0:
    st.subheader("Brent Crude Oil (USD/barrel)")
    fig_brent = px.line(
        brent.to_pandas(), x="date", y="brent_usd",
        labels={"brent_usd": "USD/barrel", "date": ""},
    )
    # Add threshold lines
    fig_brent.add_hline(y=BRENT_AMBER, line_dash="dash", line_color="orange",
                        annotation_text=f"${BRENT_AMBER:.0f} (elevated)")
    fig_brent.add_hline(y=BRENT_RED, line_dash="dash", line_color="red",
                        annotation_text=f"${BRENT_RED:.0f} (crisis)")
    if not two_weeks_view:
        two_wk_dt = datetime.now() - timedelta(days=14)
        fig_brent.add_vline(x=two_wk_dt.timestamp() * 1000, line_dash="dot",
                            line_color="gray", annotation_text="2 weeks ago")
    fig_brent.update_layout(hovermode="x unified", height=350, showlegend=False)
    fig_brent.update_traces(line_color="#1f77b4")
    st.plotly_chart(fig_brent, use_container_width=True)

    # Price spike detection
    if len(brent) >= 14:
        price_14d_ago = brent["brent_usd"].to_list()[-14]
        current_price = brent["brent_usd"].to_list()[-1]
        change_pct = (current_price - price_14d_ago) / price_14d_ago * 100
        if abs(change_pct) > 10:
            st.warning(f"**Price spike alert**: Brent has moved {change_pct:+.1f}% in the last 14 days.")

# Australian Wholesale TGP
if petrol_tgp is not None and diesel_tgp is not None:
    st.subheader("Australian Wholesale Terminal Gate Prices (c/L inc. GST)")
    tgp_left, tgp_right = st.columns(2)
    with tgp_left:
        st.markdown("**Petrol (ULP)**")
        fig_p = px.line(petrol_tgp.to_pandas(), x="date", y="National Average",
                        labels={"National Average": "c/L", "date": ""})
        fig_p.update_layout(hovermode="x unified", height=300, showlegend=False)
        fig_p.update_traces(line_color="#2ca02c")
        st.plotly_chart(fig_p, use_container_width=True)
    with tgp_right:
        st.markdown("**Diesel**")
        fig_d = px.line(diesel_tgp.to_pandas(), x="date", y="National Average",
                        labels={"National Average": "c/L", "date": ""})
        fig_d.update_layout(hovermode="x unified", height=300, showlegend=False)
        fig_d.update_traces(line_color="#d62728")
        st.plotly_chart(fig_d, use_container_width=True)

    with st.expander("Compare by city"):
        cities = ["Sydney", "Melbourne", "Brisbane", "Adelaide", "Perth", "Darwin", "Hobart"]
        fuel_type = st.radio("Fuel", ["Petrol", "Diesel"], horizontal=True)
        tgp_df = petrol_tgp if fuel_type == "Petrol" else diesel_tgp
        city_cols = [c for c in cities if c in tgp_df.columns]
        melted_city = tgp_df.select(["date"] + city_cols).unpivot(
            index="date", variable_name="city", value_name="price"
        )
        fig_city = px.line(melted_city.to_pandas(), x="date", y="price", color="city",
                           labels={"price": "c/L", "date": "", "city": "City"})
        fig_city.update_layout(hovermode="x unified", height=350)
        st.plotly_chart(fig_city, use_container_width=True)

# Global Fuel Futures
if futures is not None and len(futures) > 0:
    st.subheader("US Fuel Futures (proxies for Singapore benchmarks)")
    st.caption(
        "RBOB Gasoline ~ petrol benchmark, Heating Oil ~ diesel benchmark. "
        "Singapore MOGAS 95 / Gasoil 10ppm (Platts) are subscription-only."
    )
    fig_fut = go.Figure()
    fig_fut.add_trace(go.Scatter(
        x=futures["date"].to_list(), y=futures["rbob_usd"].to_list(),
        name="RBOB Gasoline ($/gal)", line=dict(color="#ff7f0e"),
    ))
    fig_fut.add_trace(go.Scatter(
        x=futures["date"].to_list(), y=futures["heating_oil_usd"].to_list(),
        name="Heating Oil ($/gal)", line=dict(color="#9467bd"), yaxis="y2",
    ))
    fig_fut.update_layout(
        yaxis=dict(title="RBOB ($/gal)", side="left"),
        yaxis2=dict(title="Heating Oil ($/gal)", side="right", overlaying="y"),
        hovermode="x unified", height=350, legend=dict(orientation="h", y=1.12),
    )
    st.plotly_chart(fig_fut, use_container_width=True)
