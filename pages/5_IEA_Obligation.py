"""IEA 90-Day Obligation — Australia's compliance with international fuel reserves requirement."""

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go

from data_loader import (
    load_iea_net_import_cover,
    load_iea_days_incl_on_way,
    load_stocks_incl_on_way,
)
from dashboard_utils import IEA_OBLIGATION_DAYS, rag_status, rag_color, rag_icon

st.set_page_config(page_title="IEA 90-Day Obligation", page_icon="🌐", layout="wide")
st.title("IEA 90-Day Net Import Cover Obligation")
st.caption(
    "As an IEA member, Australia is required to hold oil stocks equivalent to "
    "at least 90 days of net imports. Australia has been consistently below this target."
)

# ── Load data ──
try:
    iea_strict = load_iea_net_import_cover()
except Exception as e:
    st.error(f"Could not load IEA net import cover data: {e}")
    st.stop()

# Find the IEA days column
iea_days_col = [c for c in iea_strict.columns if "iea days" in c.lower()]
net_imports_col = [c for c in iea_strict.columns if "net import" in c.lower() and "iea" not in c.lower()]

if not iea_days_col:
    st.error("IEA days column not found in data.")
    st.stop()

iea_days_col = iea_days_col[0]
net_imports_col = net_imports_col[0] if net_imports_col else None

# ── Row 1: Big Metrics ──
latest = iea_strict.tail(1)
prev = iea_strict.tail(2).head(1)
latest_month = latest["month"].to_list()[0]
iea_val = latest[iea_days_col].to_list()[0]
prev_val = prev[iea_days_col].to_list()[0] if len(prev) > 0 else None

# Try to load inclusive (on-way) data
try:
    iea_incl = load_iea_days_incl_on_way()
    # Find the total IEA days column (inclusive)
    incl_days_cols = [c for c in iea_incl.columns if "iea" in c.lower() and "days" in c.lower()]
    # The last column is usually the total inclusive
    incl_total_col = incl_days_cols[-1] if incl_days_cols else None
    incl_val = iea_incl.tail(1)[incl_total_col].to_list()[0] if incl_total_col else None
except Exception:
    incl_val = None

m1, m2, m3 = st.columns(3)

with m1:
    status = rag_status(iea_val, IEA_OBLIGATION_DAYS, 70) if iea_val else "red"
    icon = rag_icon(iea_val, IEA_OBLIGATION_DAYS, 70) if iea_val else "🔴"
    delta = f"{iea_val - prev_val:+.0f}" if iea_val and prev_val else None
    st.markdown(f"### {icon} Strict Measure")
    st.metric(
        "IEA Days (on-land stocks only)",
        f"{iea_val:.0f} days" if iea_val else "N/A",
        delta=delta,
    )
    if iea_val and iea_val < IEA_OBLIGATION_DAYS:
        shortfall = IEA_OBLIGATION_DAYS - iea_val
        st.markdown(f":red[**{shortfall:.0f} days short** of 90-day obligation]")

with m2:
    if incl_val:
        incl_status = rag_status(incl_val, IEA_OBLIGATION_DAYS, 70)
        incl_icon = rag_icon(incl_val, IEA_OBLIGATION_DAYS, 70)
        st.markdown(f"### {incl_icon} Inclusive Measure")
        st.metric(
            "IEA Days (incl. on-the-way stocks)",
            f"{incl_val:.0f} days",
        )
        if incl_val < IEA_OBLIGATION_DAYS:
            shortfall = IEA_OBLIGATION_DAYS - incl_val
            st.markdown(f":red[**{shortfall:.0f} days short** even with on-the-way stocks]")
        elif incl_val >= IEA_OBLIGATION_DAYS and iea_val and iea_val < IEA_OBLIGATION_DAYS:
            st.markdown(":orange[Compliant only when counting stocks still at sea]")
    else:
        st.markdown("### Inclusive Measure")
        st.metric("IEA Days (incl. on-the-way)", "N/A")

with m3:
    if net_imports_col:
        net_imp = latest[net_imports_col].to_list()[0]
        st.markdown("### Daily Net Imports")
        st.metric(
            "Net Imports",
            f"{net_imp:.1f} kT/day" if net_imp else "N/A",
            help="Daily net oil imports (thousand tonnes/day)",
        )
    st.caption(f"Data as of {latest_month.strftime('%B %Y')}")

st.divider()

# ── Compliance Timeline ──
st.subheader("IEA Compliance Timeline")

fig_timeline = go.Figure()

# Strict measure
fig_timeline.add_trace(go.Scatter(
    x=iea_strict["month"].to_list(),
    y=iea_strict[iea_days_col].to_list(),
    name="Strict (on-land only)",
    mode="lines",
    line=dict(color="#1f77b4", width=2),
))

# Inclusive measure if available
if incl_val is not None and incl_total_col:
    fig_timeline.add_trace(go.Scatter(
        x=iea_incl["month"].to_list(),
        y=iea_incl[incl_total_col].to_list(),
        name="Inclusive (on-land + on-the-way)",
        mode="lines",
        line=dict(color="#2ca02c", width=2),
    ))

# 90-day obligation line
fig_timeline.add_hline(
    y=IEA_OBLIGATION_DAYS, line_dash="dash", line_color="red",
    annotation_text=f"{IEA_OBLIGATION_DAYS}-day IEA obligation",
)

fig_timeline.update_layout(
    height=450, hovermode="x unified",
    yaxis_title="Days of Net Import Cover",
    legend=dict(orientation="h", y=-0.15),
)
st.plotly_chart(fig_timeline, use_container_width=True)

st.divider()

# ── Stock Location Breakdown ──
st.subheader("Where Are Australia's Oil Stocks?")

try:
    stocks_otw = load_stocks_incl_on_way()
    rename_map = {}
    for c in stocks_otw.columns:
        if "on land" in c.lower():
            rename_map[c] = "On Land in Australia"
        elif "onboard" in c.lower() or "at sea" in c.lower():
            rename_map[c] = "At Sea (en route to AU)"
        elif "overseas" in c.lower() and "awaiting" in c.lower():
            rename_map[c] = "Overseas (awaiting delivery)"

    if rename_map:
        display_cols = list(rename_map.keys())
        otw_display = stocks_otw.select(["month"] + display_cols).rename(rename_map)

        melted = otw_display.unpivot(
            index="month", variable_name="location", value_name="volume_ml"
        )
        fig_loc = px.area(
            melted.to_pandas(),
            x="month", y="volume_ml", color="location",
            labels={"volume_ml": "Volume (ML)", "month": ""},
            color_discrete_map={
                "On Land in Australia": "#1f77b4",
                "At Sea (en route to AU)": "#ff7f0e",
                "Overseas (awaiting delivery)": "#d62728",
            },
        )
        fig_loc.update_layout(hovermode="x unified", height=400,
                              legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig_loc, use_container_width=True)

        # Latest breakdown
        latest_otw = otw_display.tail(1)
        cols_met = st.columns(len(rename_map))
        for i, col_name in enumerate(rename_map.values()):
            if col_name in latest_otw.columns:
                val = latest_otw[col_name].to_list()[0]
                with cols_met[i]:
                    st.metric(col_name, f"{val:,.0f} ML" if val else "N/A")

except Exception as e:
    st.warning(f"On-the-way stock breakdown not available: {e}")

st.divider()

# ── Context Panel ──
st.subheader("Background: Australia's IEA Obligation")

st.markdown("""
**The IEA 90-day obligation** requires member countries to hold emergency oil stocks
equivalent to at least 90 days of net imports. This is designed to provide a buffer
against supply disruptions.

**Australia's position:**
- Australia has been **consistently below 90 days** on the strict (on-land) measure for over a decade
- The inclusive measure (counting oil on ships and held overseas) sometimes approaches compliance
- In 2021, the Australian Government announced a **Fuel Security Package** including:
  - A minimum stockholding obligation for refiners
  - Government purchase of "ticket" reserves held in the US and Europe
  - Support for domestic refining through the Fuel Security Services Payment

**Key risk:** Much of Australia's counted stock is physically weeks away, on ships transiting
from the Middle East or Asia. In a rapid-onset crisis, only on-land stocks are immediately available.

**Current refineries:** Only 2 remain operational — Lytton (QLD, Ampol) and Geelong (VIC, Viva Energy).
All other states depend entirely on imported refined products.
""")
