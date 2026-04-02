"""Situation Room — the Minister's landing page. RAG status at a glance."""

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go

from data_loader import (
    load_consumption_cover,
    load_iea_net_import_cover,
    load_brent_crude,
    load_stocks,
    load_imports_volume,
    load_imports_by_country,
    load_refinery_production,
)
from port_scraper import scrape_all_ports
from dashboard_utils import (
    COVER_COLS, FUEL_COLS,
    COVER_GREEN, COVER_AMBER,
    IEA_OBLIGATION_DAYS, BRENT_AMBER, BRENT_RED,
    CONCENTRATION_ALERT,
    rag_status, rag_color, rag_icon,
)


@st.cache_data(ttl=3 * 3600, show_spinner=False)
def _cached_scrape_all(tankers_only: bool = False):
    return scrape_all_ports(tankers_only=tankers_only)


st.set_page_config(page_title="Situation Room", page_icon="🎯", layout="wide")
from dashboard_utils import render_data_freshness_sidebar
render_data_freshness_sidebar()
st.title("Fuel Security Situation Room")
st.caption("Real-time status overview for Australian fuel supply security")

# ── Load data ──
try:
    cover = load_consumption_cover()
except Exception as e:
    st.error(f"Australian Petroleum Statistics data is unavailable: {e}")
    st.info("The workbook is downloaded automatically on first load. Try refreshing in a minute.")
    st.stop()
avail_cover = {k: v for k, v in COVER_COLS.items() if k in cover.columns}
latest_cover = cover.tail(1)
cover_month = latest_cover["month"].to_list()[0]

try:
    iea = load_iea_net_import_cover()
    latest_iea = iea.tail(1)
    iea_days_col = [c for c in iea.columns if "iea days" in c.lower()][0]
    iea_days_val = latest_iea[iea_days_col].to_list()[0]
except Exception:
    iea_days_val = None

try:
    brent = load_brent_crude(days=180)
    brent_val = brent["brent_usd"].to_list()[-1] if len(brent) > 0 else None
except Exception:
    brent = None
    brent_val = None

# ── Row 1: RAG Status Cards ──
st.subheader("Status Overview")

r1c1, r1c2, r1c3, r1c4 = st.columns(4)

# Total consumption cover
total_cover_col = None
for c in cover.columns:
    if "total" in c.lower() and "days" in c.lower():
        total_cover_col = c
        break
total_cover_val = latest_cover[total_cover_col].to_list()[0] if total_cover_col else None

with r1c1:
    status = rag_status(total_cover_val, COVER_GREEN, COVER_AMBER) if total_cover_val else "red"
    icon = rag_icon(total_cover_val, COVER_GREEN, COVER_AMBER) if total_cover_val else "🔴"
    color = rag_color(total_cover_val, COVER_GREEN, COVER_AMBER) if total_cover_val else "#dc3545"
    st.markdown(f"### {icon} Total Cover")
    st.metric("Days of Consumption Cover",
              f"{total_cover_val:.0f} days" if total_cover_val else "N/A",
              help=f"Green >= {COVER_GREEN}, Amber >= {COVER_AMBER}, Red < {COVER_AMBER}")
    if total_cover_val and total_cover_val < COVER_GREEN:
        st.markdown(f":{status}[Below {COVER_GREEN}-day IEA guideline]")

# Diesel cover (most critical fuel)
diesel_cover_col = None
for c in cover.columns:
    if "diesel" in c.lower() and "days" in c.lower():
        diesel_cover_col = c
        break
diesel_cover_val = latest_cover[diesel_cover_col].to_list()[0] if diesel_cover_col else None

with r1c2:
    status = rag_status(diesel_cover_val, COVER_GREEN, COVER_AMBER) if diesel_cover_val else "red"
    icon = rag_icon(diesel_cover_val, COVER_GREEN, COVER_AMBER) if diesel_cover_val else "🔴"
    st.markdown(f"### {icon} Diesel Cover")
    st.metric("Diesel Days of Cover",
              f"{diesel_cover_val:.0f} days" if diesel_cover_val else "N/A")
    if diesel_cover_val and diesel_cover_val < COVER_GREEN:
        st.markdown(f":{status}[Below {COVER_GREEN}-day threshold]")

with r1c3:
    iea_status = rag_status(iea_days_val, IEA_OBLIGATION_DAYS, 70) if iea_days_val else "red"
    iea_icon = rag_icon(iea_days_val, IEA_OBLIGATION_DAYS, 70) if iea_days_val else "🔴"
    st.markdown(f"### {iea_icon} IEA 90-Day")
    st.metric("IEA Net Import Cover",
              f"{iea_days_val:.0f} days" if iea_days_val else "N/A",
              help="IEA obligation: 90 days of net imports")
    if iea_days_val and iea_days_val < IEA_OBLIGATION_DAYS:
        st.markdown(f":{iea_status}[Below {IEA_OBLIGATION_DAYS}-day obligation]")

with r1c4:
    brent_status = rag_status(brent_val, BRENT_AMBER, BRENT_RED) if brent_val else "amber"
    brent_icon = rag_icon(brent_val, BRENT_AMBER, BRENT_RED) if brent_val else "⚠️"
    st.markdown(f"### {brent_icon} Brent Crude")
    st.metric("Brent Crude Oil",
              f"${brent_val:.2f}/bbl" if brent_val else "N/A",
              help=f"Green < ${BRENT_AMBER:.0f}, Amber < ${BRENT_RED:.0f}, Red >= ${BRENT_RED:.0f}")

st.divider()

# ── Row 2: Secondary Metrics ──
r2c1, r2c2, r2c3, r2c4 = st.columns(4)

# Tanker counts
try:
    with st.spinner("Loading tanker data..."):
        tankers = _cached_scrape_all(tankers_only=True)
    tanker_arrivals = tankers.filter(
        pl.col("movement").str.to_lowercase().str.contains("arrival|removal|shift|in port|external")
    )
    intl_tankers = tanker_arrivals.filter(pl.col("origin_type") == "International")
    total_tankers = len(tanker_arrivals.unique(subset=["vessel"], keep="first"))
    intl_count = len(intl_tankers.unique(subset=["vessel"], keep="first"))
except Exception:
    total_tankers = 0
    intl_count = 0

with r2c1:
    st.metric("Tankers Inbound", total_tankers,
              help="Unique fuel tankers with arrival/in-port movements")

with r2c2:
    st.metric("International", intl_count,
              help="Tankers from overseas origins")

# Import dependency
try:
    imports_vol = load_imports_volume()
    refinery = load_refinery_production()
    merged = imports_vol.join(refinery, on="month", suffix="_ref")
    imp_col = "Total refined petroleum products (ML)"
    ref_col = "Total (ML)"
    if imp_col in merged.columns and ref_col in merged.columns:
        latest_merged = merged.sort("month").tail(1)
        imp_val = latest_merged[imp_col].to_list()[0]
        ref_val = latest_merged[ref_col].to_list()[0]
        dep_pct = imp_val / (imp_val + ref_val) * 100 if imp_val and ref_val else None
    else:
        dep_pct = None
except Exception:
    dep_pct = None

with r2c3:
    st.metric("Import Dependency",
              f"{dep_pct:.0f}%" if dep_pct else "N/A",
              help="Refined product imports / (imports + refinery production)")

# Stock trend (3-month)
try:
    stocks = load_stocks()
    total_stock_col = None
    for c in stocks.columns:
        if "total" in c.lower() and "ml" in c.lower():
            total_stock_col = c
            break
    if total_stock_col:
        recent_stocks = stocks.sort("month").tail(4)[total_stock_col].to_list()
        if len(recent_stocks) >= 4:
            three_mo_change = recent_stocks[-1] - recent_stocks[0]
            pct_change = three_mo_change / recent_stocks[0] * 100
        else:
            three_mo_change = None
            pct_change = None
    else:
        three_mo_change = None
        pct_change = None
except Exception:
    three_mo_change = None
    pct_change = None

with r2c4:
    if pct_change is not None:
        trend_label = "Building" if pct_change > 1 else "Declining" if pct_change < -1 else "Stable"
        st.metric("Stock Trend (3mo)",
                  trend_label,
                  delta=f"{pct_change:+.1f}%")
    else:
        st.metric("Stock Trend (3mo)", "N/A")

st.divider()

# ── Row 3: Sparkline Charts ──
spark_l, spark_r = st.columns(2)

with spark_l:
    st.subheader("Consumption Cover (12 months)")
    cover_12 = cover.tail(12)
    if diesel_cover_col:
        fig_spark = go.Figure()
        for raw_col, label in avail_cover.items():
            fig_spark.add_trace(go.Scatter(
                x=cover_12["month"].to_list(),
                y=cover_12[raw_col].to_list(),
                name=label, mode="lines",
            ))
        fig_spark.add_hline(y=COVER_GREEN, line_dash="dash", line_color="orange",
                            annotation_text=f"{COVER_GREEN}d")
        fig_spark.update_layout(height=250, margin=dict(l=0, r=0, t=10, b=0),
                                hovermode="x unified", legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_spark, use_container_width=True)

with spark_r:
    st.subheader("Brent Crude (6 months)")
    if brent is not None and len(brent) > 0:
        fig_brent = go.Figure()
        fig_brent.add_trace(go.Scatter(
            x=brent["date"].to_list(), y=brent["brent_usd"].to_list(),
            name="Brent", mode="lines", line=dict(color="#1f77b4"),
        ))
        fig_brent.add_hline(y=BRENT_AMBER, line_dash="dash", line_color="orange",
                            annotation_text=f"${BRENT_AMBER:.0f}")
        fig_brent.add_hline(y=BRENT_RED, line_dash="dash", line_color="red",
                            annotation_text=f"${BRENT_RED:.0f}")
        fig_brent.update_layout(height=250, margin=dict(l=0, r=0, t=10, b=0),
                                hovermode="x unified", showlegend=False)
        st.plotly_chart(fig_brent, use_container_width=True)
    else:
        st.info("Brent crude data not available.")

st.divider()

# ── Row 4: Active Alerts ──
st.subheader("Active Alerts")

alerts = []

# Consumption cover alerts
for raw_col, label in avail_cover.items():
    val = latest_cover[raw_col].to_list()[0]
    if val is not None:
        if val < COVER_AMBER:
            alerts.append(("error", f"**{label}**: {val:.0f} days of cover — CRITICAL (below {COVER_AMBER} days)"))
        elif val < COVER_GREEN:
            alerts.append(("warning", f"**{label}**: {val:.0f} days of cover — below IEA {COVER_GREEN}-day guideline"))

# IEA compliance
if iea_days_val is not None and iea_days_val < IEA_OBLIGATION_DAYS:
    alerts.append(("error", f"**IEA 90-Day Obligation**: Currently at {iea_days_val:.0f} days — non-compliant"))

# Brent price
if brent_val is not None:
    if brent_val >= BRENT_RED:
        alerts.append(("error", f"**Brent Crude**: ${brent_val:.2f}/bbl — crisis-level pricing"))
    elif brent_val >= BRENT_AMBER:
        alerts.append(("warning", f"**Brent Crude**: ${brent_val:.2f}/bbl — elevated pricing"))

# Import concentration (check latest 3 months)
try:
    by_country = load_imports_by_country()
    recent = by_country.filter(
        pl.col("month") >= by_country["month"].max() - pl.duration(days=90)
    )
    for fuel_raw, fuel_label in list(FUEL_COLS.items())[:4]:
        if fuel_raw in recent.columns:
            totals = (
                recent.group_by("country")
                .agg(pl.col(fuel_raw).sum().alias("total"))
                .filter(pl.col("total") > 0)
            )
            grand_total = totals["total"].sum()
            if grand_total > 0:
                totals = totals.with_columns(
                    (pl.col("total") / grand_total).alias("share")
                )
                max_share = totals["share"].max()
                if max_share > CONCENTRATION_ALERT:
                    top_country = totals.sort("share", descending=True)["country"].to_list()[0]
                    alerts.append((
                        "warning",
                        f"**{fuel_label} concentration**: {top_country} supplies "
                        f"{max_share * 100:.0f}% of imports (>{CONCENTRATION_ALERT * 100:.0f}% threshold)"
                    ))
except Exception:
    pass

# Stock drawdown
if three_mo_change is not None and pct_change is not None and pct_change < -5:
    alerts.append(("warning", f"**Stock drawdown**: Total stocks declined {pct_change:.1f}% over 3 months"))

if alerts:
    for level, msg in alerts:
        if level == "error":
            st.error(msg)
        else:
            st.warning(msg)
else:
    st.success("No active alerts. All indicators within normal parameters.")

# Footer
st.divider()
st.caption(
    f"Consumption cover data as of {cover_month.strftime('%B %Y')}. "
    "Tanker data refreshed every 3 hours from port authority feeds. "
    "Brent crude from FRED (daily)."
)
