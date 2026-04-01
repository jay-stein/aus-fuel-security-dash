"""Import Concentration & Risk — source country analysis, HHI, chokepoint risk."""

import streamlit as st
import polars as pl
import plotly.express as px

from data_loader import load_imports_by_country
from dashboard_utils import (
    FUEL_COLS, CONCENTRATION_ALERT, CHOKEPOINT_ROUTES,
    HHI_GREEN, HHI_AMBER, compute_hhi, rag_color,
)

st.set_page_config(page_title="Import Concentration & Risk", page_icon="🌍", layout="wide")
st.title("Import Concentration & Supply Risk")

try:
    by_country = load_imports_by_country()
except Exception as e:
    st.error(f"Australian Petroleum Statistics data is unavailable: {e}")
    st.info("The workbook is downloaded automatically on first load. Try refreshing in a minute.")
    st.stop()

# Date range
min_date = by_country["month"].min()
max_date = by_country["month"].max()
col1, col2 = st.columns(2)
with col1:
    start = st.date_input("From", min_date, min_value=min_date, max_value=max_date)
with col2:
    end = st.date_input("To", max_date, min_value=min_date, max_value=max_date)

filtered = by_country.filter(
    (pl.col("month") >= start) & (pl.col("month") <= end)
)

available_fuels = {k: v for k, v in FUEL_COLS.items() if k in filtered.columns}
product_choice = st.selectbox("Product", list(available_fuels.values()))
raw_col = {v: k for k, v in available_fuels.items()}[product_choice]

# ── HHI Concentration Index ──
st.subheader("Import Source Concentration (HHI)")

# Compute HHI for all products over selected period
hhi_data = []
for fuel_raw, fuel_label in available_fuels.items():
    totals = (
        filtered.group_by("country")
        .agg(pl.col(fuel_raw).sum().alias("total"))
        .filter(pl.col("total") > 0)
    )
    grand_total = totals["total"].sum()
    if grand_total > 0:
        shares = (totals["total"] / grand_total * 100).to_list()
        hhi_val = compute_hhi(shares)
        top_country = totals.sort("total", descending=True)["country"].to_list()[0]
        top_share = max(shares)
        status = "Diversified" if hhi_val < HHI_GREEN else "Moderate" if hhi_val < HHI_AMBER else "Concentrated"
        hhi_data.append({
            "Product": fuel_label,
            "HHI": int(hhi_val),
            "Status": status,
            "Top Source": top_country,
            "Top Share %": f"{top_share:.1f}%",
        })

if hhi_data:
    import pandas as pd
    hhi_df = pd.DataFrame(hhi_data)

    # Color-code the status
    def _color_status(val):
        if val == "Diversified":
            return "background-color: #d4edda"
        elif val == "Moderate":
            return "background-color: #fff3cd"
        return "background-color: #f8d7da"

    st.dataframe(
        hhi_df.style.applymap(_color_status, subset=["Status"]),
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "HHI < 1,500 = Well-diversified | "
        "1,500-2,500 = Moderately concentrated | "
        "> 2,500 = Highly concentrated"
    )

# ── Single-Country Dominance Alerts ──
st.subheader("Single-Country Dominance Alerts")
dominance_alerts = []
for fuel_raw, fuel_label in list(available_fuels.items())[:4]:
    totals = (
        filtered.group_by("country")
        .agg(pl.col(fuel_raw).sum().alias("total"))
        .filter(pl.col("total") > 0)
    )
    grand_total = totals["total"].sum()
    if grand_total > 0:
        totals = totals.with_columns((pl.col("total") / grand_total).alias("share"))
        dominant = totals.filter(pl.col("share") > CONCENTRATION_ALERT).sort("share", descending=True)
        for row in dominant.iter_rows(named=True):
            dominance_alerts.append(
                f"**{fuel_label}**: {row['country']} supplies {row['share']*100:.0f}% "
                f"of imports (>{CONCENTRATION_ALERT*100:.0f}% threshold)"
            )

if dominance_alerts:
    for alert in dominance_alerts:
        st.warning(alert)
else:
    st.success("No single country exceeds the concentration threshold for any major product.")

st.divider()

# ── Top Source Countries (existing) ──
top = (
    filtered.group_by("country")
    .agg(pl.col(raw_col).sum().alias("total"))
    .filter(pl.col("total") > 0)
    .sort("total", descending=True)
    .head(15)
)

col_l, col_r = st.columns(2)

with col_l:
    st.subheader(f"Top 15 Source Countries — {product_choice}")
    fig = px.bar(
        top.to_pandas(), x="total", y="country", orientation="h",
        labels={"total": "Total Volume (ML)", "country": "Country"},
        color="total", color_continuous_scale="YlOrRd",
    )
    fig.update_layout(yaxis=dict(autorange="reversed"), height=500, showlegend=False)
    fig.update_coloraxes(showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with col_r:
    st.subheader(f"Share of {product_choice} Imports")
    top10 = top.head(10)
    fig2 = px.pie(top10.to_pandas(), values="total", names="country", hole=0.4)
    fig2.update_layout(height=500)
    st.plotly_chart(fig2, use_container_width=True)

# Over time
st.subheader(f"{product_choice} Imports Over Time by Country")
top5_countries = top.head(5)["country"].to_list()
time_data = filtered.filter(pl.col("country").is_in(top5_countries))
time_agg = (
    time_data.with_columns(
        (pl.col("month").dt.year().cast(pl.String) + "-Q" +
         ((pl.col("month").dt.month() - 1) // 3 + 1).cast(pl.String)).alias("quarter")
    )
    .group_by(["quarter", "country"])
    .agg(pl.col(raw_col).sum().alias("volume"))
    .sort("quarter")
)
fig3 = px.bar(
    time_agg.to_pandas(), x="quarter", y="volume", color="country",
    labels={"volume": "Volume (ML)", "quarter": "Quarter"},
    barmode="stack",
)
fig3.update_layout(height=450, xaxis_tickangle=-45)
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Chokepoint Risk Analysis ──
st.subheader("Shipping Chokepoint Risk")
st.caption(
    "Estimated share of imports transiting major maritime chokepoints, "
    "based on source country geography. Ships may use alternative routes."
)

# Calculate chokepoint exposure for selected product
product_totals = (
    filtered.group_by("country")
    .agg(pl.col(raw_col).sum().alias("total"))
    .filter(pl.col("total") > 0)
)
grand_total = product_totals["total"].sum()

if grand_total > 0:
    country_shares = dict(zip(
        product_totals["country"].to_list(),
        (product_totals["total"] / grand_total * 100).to_list()
    ))

    chokepoint_data = []
    for chokepoint, countries in CHOKEPOINT_ROUTES.items():
        exposure = sum(country_shares.get(c, 0) for c in countries)
        if exposure > 0:
            chokepoint_data.append({
                "Chokepoint": chokepoint,
                "Exposure %": round(exposure, 1),
                "Key Countries": ", ".join(c for c in countries if c in country_shares),
            })

    if chokepoint_data:
        import pandas as pd
        cp_df = pd.DataFrame(chokepoint_data).sort_values("Exposure %", ascending=False)

        fig_cp = px.bar(
            cp_df, x="Exposure %", y="Chokepoint", orientation="h",
            labels={"Exposure %": f"% of {product_choice} Imports", "Chokepoint": ""},
            color="Exposure %", color_continuous_scale="OrRd",
        )
        fig_cp.update_layout(
            yaxis=dict(autorange="reversed"), height=300, showlegend=False,
        )
        fig_cp.update_coloraxes(showscale=False)
        st.plotly_chart(fig_cp, use_container_width=True)

        st.dataframe(cp_df, use_container_width=True, hide_index=True)
    else:
        st.info("No chokepoint exposure data available for this product/period.")
