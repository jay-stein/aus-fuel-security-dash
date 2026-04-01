"""Supply Security — consumption cover, stock levels, drawdown, import dependency."""

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go

from data_loader import (
    load_consumption_cover,
    load_stocks,
    load_imports_volume,
    load_refinery_production,
    load_stocks_incl_on_way,
)
from dashboard_utils import (
    COVER_COLS, STOCK_COLS,
    COVER_GREEN, COVER_AMBER, COVER_RED,
    rag_color, rag_status,
)

st.set_page_config(page_title="Supply Security", page_icon="🛡️", layout="wide")
st.title("Australian Fuel Supply Security")

# ── Section 1: Consumption Cover — big bold metrics at top ──
try:
    cover = load_consumption_cover()
except Exception as e:
    st.error(f"Australian Petroleum Statistics data is unavailable: {e}")
    st.info("The workbook is downloaded automatically on first load. Try refreshing in a minute.")
    st.stop()
avail_cover = {k: v for k, v in COVER_COLS.items() if k in cover.columns}

latest = cover.tail(1)
prev = cover.tail(2).head(1)
latest_month = latest["month"].to_list()[0]

st.subheader("Days of Consumption Cover")
st.caption(f"As of {latest_month.strftime('%B %Y')}")

cover_metrics = st.columns(len(avail_cover))
for col_widget, (raw_col, label) in zip(cover_metrics, avail_cover.items()):
    current_val = latest[raw_col].to_list()[0]
    prev_val = prev[raw_col].to_list()[0]
    delta = round(current_val - prev_val, 1) if current_val and prev_val else None
    with col_widget:
        st.metric(
            label=label,
            value=f"{current_val:.0f} days" if current_val else "N/A",
            delta=f"{delta:+.0f}" if delta is not None else None,
        )
        if current_val:
            status = rag_status(current_val, COVER_GREEN, COVER_AMBER)
            if status == "red":
                st.markdown(f":red[**CRITICAL** — below {COVER_AMBER}-day threshold]")
            elif status == "amber":
                st.markdown(f":orange[Below IEA {COVER_GREEN}-day guideline]")

# Consumption cover time series with multi-tier thresholds
melted = cover.select(["month"] + list(avail_cover.keys())).unpivot(
    index="month", variable_name="product_raw", value_name="days"
)
melted = melted.with_columns(
    pl.col("product_raw").replace(avail_cover).alias("product")
)
fig_cover = px.line(
    melted.to_pandas(),
    x="month", y="days", color="product",
    labels={"days": "Days of Cover", "month": "Month"},
)
fig_cover.add_hline(y=COVER_GREEN, line_dash="dash", line_color="orange",
                    annotation_text=f"{COVER_GREEN} days (IEA guideline)")
fig_cover.add_hline(y=COVER_AMBER, line_dash="dash", line_color="red",
                    annotation_text=f"{COVER_AMBER} days (critical)")
fig_cover.add_hline(y=COVER_RED, line_dash="solid", line_color="darkred",
                    annotation_text=f"{COVER_RED} days (emergency)")
fig_cover.update_layout(hovermode="x unified", height=400)
st.plotly_chart(fig_cover, use_container_width=True)

st.divider()

# ── Section 2: Stock Levels ──
st.subheader("Stock Levels")
stocks = load_stocks()
avail_stocks = {k: v for k, v in STOCK_COLS.items() if k in stocks.columns}
melted_s = stocks.select(["month"] + list(avail_stocks.keys())).unpivot(
    index="month", variable_name="product_raw", value_name="volume_ml"
)
melted_s = melted_s.with_columns(
    pl.col("product_raw").replace(avail_stocks).alias("product")
)
fig_stocks = px.area(
    melted_s.to_pandas(),
    x="month", y="volume_ml", color="product",
    labels={"volume_ml": "Stock Volume (ML)", "month": "Month"},
    color_discrete_sequence=px.colors.qualitative.Set2,
)
fig_stocks.update_layout(hovermode="x unified", height=400)
st.plotly_chart(fig_stocks, use_container_width=True)

# ── Stock Drawdown Rate ──
st.subheader("Stock Drawdown Rate (Month-on-Month)")
# Use total stock column
total_stock_col = None
for c in stocks.columns:
    if "total" in c.lower() and "ml" in c.lower():
        total_stock_col = c
        break

if total_stock_col:
    drawdown = stocks.select(["month", total_stock_col]).sort("month").drop_nulls()
    drawdown = drawdown.with_columns(
        (pl.col(total_stock_col) - pl.col(total_stock_col).shift(1)).alias("change_ml"),
        ((pl.col(total_stock_col) - pl.col(total_stock_col).shift(1))
         / pl.col(total_stock_col).shift(1) * 100).alias("change_pct"),
    ).drop_nulls()

    # Last 24 months
    drawdown_recent = drawdown.tail(24)
    fig_dd = px.bar(
        drawdown_recent.to_pandas(),
        x="month", y="change_ml",
        labels={"change_ml": "Stock Change (ML)", "month": "Month"},
        color=drawdown_recent["change_ml"].to_pandas().apply(
            lambda x: "Building" if x >= 0 else "Drawing Down"
        ),
        color_discrete_map={"Building": "#28a745", "Drawing Down": "#dc3545"},
    )
    fig_dd.update_layout(height=350, showlegend=True)
    st.plotly_chart(fig_dd, use_container_width=True)

    # Check for consecutive drawdown warning
    recent_3 = drawdown.tail(3)["change_pct"].to_list()
    consecutive_drawdown = sum(1 for v in recent_3 if v < -5)
    if consecutive_drawdown >= 2:
        st.error(
            f"**Stock drawdown alert**: {consecutive_drawdown} of the last 3 months "
            "show >5% stock decline."
        )

st.divider()

# ── Section 3: On-Water Stocks (ABS official data) ──
st.subheader("Stock Location Breakdown (incl. On-the-Way)")
try:
    stocks_otw = load_stocks_incl_on_way()
    # Rename long column names for display
    rename_map = {}
    for c in stocks_otw.columns:
        if "on land" in c.lower():
            rename_map[c] = "On Land (AU)"
        elif "onboard" in c.lower() or "at sea" in c.lower():
            rename_map[c] = "At Sea (en route)"
        elif "overseas" in c.lower() and "awaiting" in c.lower():
            rename_map[c] = "Overseas (awaiting)"

    display_cols = list(rename_map.keys())
    if display_cols:
        otw_display = stocks_otw.select(["month"] + display_cols).rename(rename_map)
        melted_otw = otw_display.unpivot(
            index="month", variable_name="location", value_name="volume_ml"
        )
        fig_otw = px.area(
            melted_otw.to_pandas(),
            x="month", y="volume_ml", color="location",
            labels={"volume_ml": "Volume (ML)", "month": "Month"},
            color_discrete_map={
                "On Land (AU)": "#1f77b4",
                "At Sea (en route)": "#ff7f0e",
                "Overseas (awaiting)": "#d62728",
            },
        )
        fig_otw.update_layout(hovermode="x unified", height=400)
        st.plotly_chart(fig_otw, use_container_width=True)

        # Latest breakdown metrics
        latest_otw = otw_display.tail(1)
        cols_otw = st.columns(3)
        for i, col_name in enumerate(["On Land (AU)", "At Sea (en route)", "Overseas (awaiting)"]):
            if col_name in latest_otw.columns:
                val = latest_otw[col_name].to_list()[0]
                with cols_otw[i]:
                    st.metric(col_name, f"{val:,.0f} ML" if val else "N/A")
except Exception as e:
    st.warning(f"On-water stock data not available: {e}")

st.divider()

# ── Section 4: Import Dependency ──
st.subheader("Imports vs Domestic Refinery Production")
imports_vol = load_imports_volume()
refinery = load_refinery_production()
merged = imports_vol.join(refinery, on="month", suffix="_ref")

imp_col = "Total refined petroleum products (ML)"
ref_col = "Total (ML)"

if imp_col in merged.columns and ref_col in merged.columns:
    compare = merged.select([
        "month",
        pl.col(imp_col).alias("Imports (refined products)"),
        pl.col(ref_col).alias("Refinery production"),
    ])
    melted_d = compare.unpivot(
        index="month", variable_name="source", value_name="volume_ml"
    )
    fig_dep = px.area(
        melted_d.to_pandas(),
        x="month", y="volume_ml", color="source",
        labels={"volume_ml": "Volume (ML)", "month": "Month"},
    )
    fig_dep.update_layout(hovermode="x unified", height=400)
    st.plotly_chart(fig_dep, use_container_width=True)

    # Import dependency ratio
    dep = merged.with_columns(
        (pl.col(imp_col) / (pl.col(imp_col) + pl.col(ref_col)) * 100)
        .alias("import_pct")
    ).select(["month", "import_pct"]).drop_nulls()

    st.subheader("Import Dependency Ratio (refined products)")
    fig_ratio = px.line(
        dep.to_pandas(),
        x="month", y="import_pct",
        labels={"import_pct": "Import Dependency (%)", "month": "Month"},
    )
    fig_ratio.add_hline(y=50, line_dash="dash", line_color="red", annotation_text="50%")
    fig_ratio.update_layout(height=350)
    st.plotly_chart(fig_ratio, use_container_width=True)
