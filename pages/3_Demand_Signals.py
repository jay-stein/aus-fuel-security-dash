"""Demand Signals — weekly MSO stock drawdown, monthly consumption trends."""

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go

from data_loader import load_mso_weekly, load_sales, load_sales_by_state
from dashboard_utils import (
    STATE_SALES_COLS,
    MSO_SURPLUS_GREEN, MSO_SURPLUS_AMBER,
    rag_status, rag_color,
)

st.set_page_config(page_title="Demand Signals", page_icon="📊", layout="wide")
st.title("Fuel Demand Signals")

# ═══════════════════════════════════════════════════════════════
# Section 1 — Weekly MSO Stock Data (DCCEEW)
# ═══════════════════════════════════════════════════════════════

st.header("Weekly Fuel Stocks (MSO)")
st.caption("Source: DCCEEW Minimum Stockholding Obligation — updated weekly (Saturdays)")

try:
    mso = load_mso_weekly()
except Exception as e:
    st.error(f"Could not load MSO data: {e}")
    mso = None

if mso is not None and len(mso) >= 2:
    latest = mso.tail(1)
    prev = mso.tail(2).head(1)
    latest_week = latest["week_ending"].to_list()[0]

    st.subheader("Days of Supply")
    st.caption(f"Week ending {latest_week.strftime('%d %B %Y')}")

    fuels = [
        ("Diesel", "diesel_days", "diesel_surplus_pct"),
        ("Jet Fuel", "jet_fuel_days", "jet_fuel_surplus_pct"),
        ("Petrol", "petrol_days", "petrol_surplus_pct"),
    ]

    cols = st.columns(len(fuels))
    for col_widget, (label, days_col, surplus_col) in zip(cols, fuels):
        current = latest[days_col].to_list()[0]
        previous = prev[days_col].to_list()[0]
        surplus = latest[surplus_col].to_list()[0] if surplus_col in latest.columns else None
        delta = round(current - previous, 1) if current and previous else None

        with col_widget:
            st.metric(
                label=label,
                value=f"{current:.0f} days" if current else "N/A",
                delta=f"{delta:+.1f}" if delta is not None else None,
            )
            if surplus is not None:
                status = rag_status(surplus, MSO_SURPLUS_GREEN, MSO_SURPLUS_AMBER)
                color = rag_color(surplus, MSO_SURPLUS_GREEN, MSO_SURPLUS_AMBER)
                pct_str = f"{surplus * 100:.0f}%"
                if status == "red":
                    st.markdown(f":red[MSO surplus: **{pct_str}** — below minimum]")
                elif status == "amber":
                    st.markdown(f":orange[MSO surplus: **{pct_str}**]")
                else:
                    st.markdown(f":green[MSO surplus: **{pct_str}**]")

    # 4-week drawdown summary
    if len(mso) >= 5:
        four_wk_ago = mso.tail(5).head(1)
        st.markdown("---")
        draw_cols = st.columns(len(fuels))
        for col_widget, (label, days_col, _) in zip(draw_cols, fuels):
            current = latest[days_col].to_list()[0]
            four_wk = four_wk_ago[days_col].to_list()[0]
            change = round(current - four_wk, 1) if current and four_wk else None
            with col_widget:
                if change is not None:
                    direction = "up" if change > 0 else "down" if change < 0 else "flat"
                    arrow = {"up": ":green[Building]", "down": ":red[Drawing down]", "flat": "Stable"}[direction]
                    st.caption(f"4-week trend: {arrow} ({change:+.1f} days)")

    # ── Stock Days Trend Chart ──
    st.subheader("Stock Days Trend")
    weeks_back = st.slider("Weeks to display", 12, len(mso), min(52, len(mso)), key="mso_weeks")
    mso_recent = mso.tail(weeks_back)

    fig_days = go.Figure()
    colors = {"Diesel": "#1f77b4", "Jet Fuel": "#ff7f0e", "Petrol": "#2ca02c"}
    for label, days_col, _ in fuels:
        fig_days.add_trace(go.Scatter(
            x=mso_recent["week_ending"].to_list(),
            y=mso_recent[days_col].to_list(),
            name=label,
            mode="lines+markers",
            marker=dict(size=4),
            line=dict(color=colors[label]),
        ))
    fig_days.update_layout(
        yaxis_title="Days of Supply",
        xaxis_title="Week Ending",
        hovermode="x unified",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_days, use_container_width=True)

    # ── MSO Surplus Trend Chart ──
    surplus_cols_available = [c for _, _, c in fuels if c in mso.columns]
    if surplus_cols_available:
        st.subheader("MSO Surplus Trend")
        fig_surplus = go.Figure()
        surplus_labels = {"diesel_surplus_pct": "Diesel", "jet_fuel_surplus_pct": "Jet Fuel", "petrol_surplus_pct": "Petrol"}
        for surplus_col in surplus_cols_available:
            label = surplus_labels.get(surplus_col, surplus_col)
            vals = mso_recent[surplus_col].to_list()
            fig_surplus.add_trace(go.Scatter(
                x=mso_recent["week_ending"].to_list(),
                y=[v * 100 if v is not None else None for v in vals],
                name=label,
                mode="lines",
                line=dict(color=colors.get(label, "#999")),
            ))
        fig_surplus.add_hline(y=0, line_dash="dash", line_color="red", annotation_text="MSO Minimum")
        fig_surplus.update_layout(
            yaxis_title="Surplus Above MSO (%)",
            xaxis_title="Week Ending",
            hovermode="x unified",
            height=350,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_surplus, use_container_width=True)

    # ── Weekly Volume Trend ──
    vol_cols = [c for c in ["diesel_volume_ml", "jet_fuel_volume_ml", "petrol_volume_ml"] if c in mso.columns]
    if vol_cols:
        st.subheader("Reported Stock Volumes")
        fig_vol = go.Figure()
        vol_labels = {"diesel_volume_ml": "Diesel", "jet_fuel_volume_ml": "Jet Fuel", "petrol_volume_ml": "Petrol"}
        for vc in vol_cols:
            label = vol_labels.get(vc, vc)
            fig_vol.add_trace(go.Scatter(
                x=mso_recent["week_ending"].to_list(),
                y=mso_recent[vc].to_list(),
                name=label,
                mode="lines",
                fill="tonexty" if vc != vol_cols[0] else "tozeroy",
                line=dict(color=colors.get(label, "#999")),
            ))
        fig_vol.update_layout(
            yaxis_title="Volume (ML)",
            xaxis_title="Week Ending",
            hovermode="x unified",
            height=350,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_vol, use_container_width=True)


# ═══════════════════════════════════════════════════════════════
# Section 2 — Monthly Consumption (from Australian Petroleum Statistics)
# ═══════════════════════════════════════════════════════════════

st.markdown("---")
st.header("Monthly Fuel Consumption")
st.caption("Source: Australian Petroleum Statistics — national sales by product")

try:
    sales = load_sales()
except Exception as e:
    st.error(f"Could not load sales data: {e}")
    sales = None

if sales is not None:
    # Map raw columns to display names for key fuel types
    sales_fuel_cols = {
        "Automotive gasoline: total (ML)": "Petrol",
        "Diesel oil: total (ML)": "Diesel",
        "Aviation turbine fuel (ML)": "Jet Fuel",
    }
    avail_sales = {k: v for k, v in sales_fuel_cols.items() if k in sales.columns}

    if avail_sales:
        # Last 24 months
        sales_recent = sales.tail(24)
        melted = sales_recent.select(["month"] + list(avail_sales.keys())).unpivot(
            index="month", variable_name="product_raw", value_name="volume_ml"
        )
        melted = melted.with_columns(
            pl.col("product_raw").replace(avail_sales).alias("product")
        )

        fig_sales = px.line(
            melted.to_pandas(), x="month", y="volume_ml", color="product",
            title="Monthly Sales Volume (last 24 months)",
            labels={"volume_ml": "Volume (ML)", "month": "Month"},
            color_discrete_map={"Diesel": "#1f77b4", "Jet Fuel": "#ff7f0e", "Petrol": "#2ca02c"},
        )
        fig_sales.update_layout(
            hovermode="x unified",
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_sales, use_container_width=True)

        # Year-on-year comparison for latest month
        if len(sales) >= 13:
            latest_sales = sales.tail(1)
            yoy_sales = sales.tail(13).head(1)
            latest_month = latest_sales["month"].to_list()[0]

            st.subheader(f"Year-on-Year Change ({latest_month.strftime('%B %Y')})")
            yoy_cols = st.columns(len(avail_sales))
            for col_widget, (raw_col, label) in zip(yoy_cols, avail_sales.items()):
                current = latest_sales[raw_col].to_list()[0]
                prev_yr = yoy_sales[raw_col].to_list()[0]
                if current and prev_yr and prev_yr > 0:
                    change_pct = (current - prev_yr) / prev_yr * 100
                    with col_widget:
                        st.metric(
                            label=label,
                            value=f"{current:,.0f} ML",
                            delta=f"{change_pct:+.1f}% YoY",
                        )
                else:
                    with col_widget:
                        st.metric(label=label, value=f"{current:,.0f} ML" if current else "N/A")

# ═══════════════════════════════════════════════════════════════
# Section 3 — State Demand Breakdown
# ═══════════════════════════════════════════════════════════════

st.markdown("---")
st.header("Demand by State")
st.caption("Source: Australian Petroleum Statistics — sales by state and territory")

try:
    state_sales = load_sales_by_state()
except Exception as e:
    st.error(f"Could not load state sales data: {e}")
    state_sales = None

if state_sales is not None:
    avail_state = {k: v for k, v in STATE_SALES_COLS.items() if k in state_sales.columns}

    if avail_state:
        latest_month_state = state_sales["month"].max()
        latest_state = state_sales.filter(pl.col("month") == latest_month_state)

        st.caption(f"As of {latest_month_state.strftime('%B %Y')}")

        melted_state = latest_state.select(["state"] + list(avail_state.keys())).unpivot(
            index="state", variable_name="product_raw", value_name="volume_ml"
        )
        melted_state = melted_state.with_columns(
            pl.col("product_raw").replace(avail_state).alias("product")
        ).filter(pl.col("product") != "Other")

        fig_state = px.bar(
            melted_state.to_pandas(), x="state", y="volume_ml", color="product",
            barmode="group",
            title="Monthly Sales by State",
            labels={"volume_ml": "Volume (ML)", "state": "State"},
            color_discrete_map={"Diesel": "#1f77b4", "Jet Fuel": "#ff7f0e", "Petrol": "#2ca02c"},
        )
        fig_state.update_layout(
            hovermode="x unified",
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_state, use_container_width=True)
