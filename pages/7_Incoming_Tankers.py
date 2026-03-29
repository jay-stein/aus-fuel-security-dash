"""Incoming Tankers — live tanker movements scraped from Australian port authorities."""

import streamlit as st
import polars as pl
import plotly.express as px
from datetime import datetime

from port_scraper import scrape_all_ports, COUNTRY_COORDS, AU_PORT_COORDS
from vessel_lookup import VesselCache


@st.cache_data(ttl=3 * 3600, show_spinner=False)
def _cached_scrape_all(tankers_only: bool = False):
    return scrape_all_ports(tankers_only=tankers_only)


st.set_page_config(page_title="Incoming Tankers", page_icon="🚢", layout="wide")
st.title("Incoming Fuel Tankers")
st.caption(
    "Tanker arrivals at Australian ports — filtered to fuel/oil vessels only. "
    "Volume estimates flagged as **High** (from cargo tonnage) or **Rough** (from vessel length)."
)

with st.spinner("Scraping vessel movements from port authorities..."):
    all_vessels = _cached_scrape_all(tankers_only=True)

if len(all_vessels) == 0:
    st.warning("No tanker data retrieved. Port authority sites may be unavailable.")
else:
    # Filter to arrivals + removals (in-port repositioning)
    arrivals = all_vessels.filter(
        pl.col("movement").str.to_lowercase().str.contains("arrival|removal|shift|in port|external")
    )
    if len(arrivals) == 0:
        st.info("No arrival movements found in current data. Showing all tanker movements instead.")
        arrivals = all_vessels

    # Enrich with vessel specs from cache
    cache = VesselCache()
    arrivals = cache.enrich_dataframe(arrivals)
    uncached = sum(
        1 for n in arrivals.filter(pl.col("is_tanker"))["vessel"].unique().to_list()
        if n and cache.get(n) is None
    )
    if uncached > 0:
        st.info(
            f"{uncached} tankers not yet in cache. "
            "Run `uv run python vessel_lookup.py` to look up vessel specs."
        )

    # Upgrade volume estimates using DWT + cargo type
    _VOLUME_PARAMS = {
        "Crude Oil":    (1.165, 0.95, "crude oil"),
        "Oil Products": (1.190, 0.95, "refined products"),
        "Chemical/Oil": (1.150, 0.85, "chemical/oil"),
        "LPG":          (1.800, 0.90, "LPG"),
        "Bitumen":      (0.980, 0.95, "bitumen"),
    }

    def _estimate_volume(cargo_cat: str, dwt: float, gt: float) -> tuple[str, str]:
        if cargo_cat == "LNG" and gt and gt > 0:
            vol_m3 = gt * 0.60
            litres = vol_m3 * 1000
            return f"{litres / 1_000_000:.1f} ML", "Medium (from GT, LNG)"
        if dwt and dwt > 0:
            params = _VOLUME_PARAMS.get(cargo_cat)
            if params:
                stowage, load_factor, label = params
                litres = dwt * load_factor * stowage * 1000
                return f"{litres / 1_000_000:.1f} ML", f"High (DWT x {label})"
            else:
                litres = dwt * 0.95 * 1.15 * 1000
                return f"{litres / 1_000_000:.1f} ML", "Medium (DWT, type unknown)"
        return None, None

    if "v_dwt" in arrivals.columns:
        new_vols = []
        new_confs = []
        for row in arrivals.iter_rows(named=True):
            old_vol = row.get("est_volume", "")
            old_conf = row.get("est_confidence", "")
            if old_conf and "Rough" not in old_conf and "from tonnage" in old_conf.lower():
                new_vols.append(old_vol)
                new_confs.append(old_conf)
                continue
            vol, conf = _estimate_volume(
                row.get("cargo_category", ""), row.get("v_dwt"), row.get("v_gt"),
            )
            if vol:
                new_vols.append(vol)
                new_confs.append(conf)
            else:
                new_vols.append(old_vol)
                new_confs.append(old_conf)
        arrivals = arrivals.with_columns([
            pl.Series("est_volume", new_vols, dtype=pl.String),
            pl.Series("est_confidence", new_confs, dtype=pl.String),
        ])
        arrivals = arrivals.with_columns(
            pl.col("est_volume")
            .str.replace(" ML", "").str.replace("~", "").str.replace("Unknown", "")
            .str.strip_chars().cast(pl.Float64, strict=False).alias("est_volume_ml")
        )
        arrivals = arrivals.with_columns(
            (pl.col("est_volume_ml") * 6.29).round(0).alias("est_kbbl")
        )

    # Cargo classification
    _CARGO_CAT_MAP = {
        "crude oil": "Crude Oil", "oil products": "Oil Products",
        "chemical/oil": "Chemical/Oil", "bitumen": "Bitumen",
        "lng": "LNG", "lpg": "LPG",
    }

    def _cargo_category(ship_type: str, vessel_type: str) -> str:
        combined = f"{ship_type} {vessel_type}".lower()
        for key, cat in _CARGO_CAT_MAP.items():
            if key in combined:
                return cat
        if "tanker" in combined:
            return "Tanker (other)"
        return ""

    if "v_ship_type" in arrivals.columns:
        arrivals = arrivals.with_columns(
            pl.when(pl.col("v_ship_type").is_not_null() & (pl.col("v_ship_type") != ""))
            .then(pl.col("v_ship_type"))
            .otherwise(pl.col("vessel_type"))
            .alias("ship_type_detail")
        )
        arrivals = arrivals.with_columns(
            pl.struct(["v_ship_type", "vessel_type"])
            .map_elements(
                lambda r: _cargo_category(r["v_ship_type"] or "", r["vessel_type"] or ""),
                return_dtype=pl.String,
            ).alias("cargo_category")
        )
    else:
        arrivals = arrivals.with_columns([
            pl.col("vessel_type").alias("ship_type_detail"),
            pl.lit("").alias("cargo_category"),
        ])

    # Flag images
    _FLAG_CODES = {
        "Australia": "au", "Bahamas": "bs", "Bangladesh": "bd",
        "Belgium": "be", "Brazil": "br", "China": "cn",
        "Cyprus": "cy", "Denmark": "dk", "France": "fr",
        "Germany": "de", "Greece": "gr", "Hong Kong": "hk",
        "India": "in", "Indonesia": "id", "Italy": "it",
        "Japan": "jp", "Korea": "kr", "South Korea": "kr",
        "Liberia": "lr", "Malaysia": "my", "Malta": "mt",
        "Marshall Islands": "mh", "Nauru": "nr", "Netherlands": "nl",
        "Norway": "no", "Panama": "pa", "Philippines": "ph",
        "Portugal": "pt", "Saudi Arabia": "sa", "Singapore": "sg",
        "Spain": "es", "Sweden": "se", "Taiwan": "tw",
        "Thailand": "th", "Turkey": "tr", "UAE": "ae",
        "UK": "gb", "USA": "us", "Vietnam": "vn",
        "Bermuda": "bm", "Cayman Islands": "ky",
        "Isle of Man": "im", "Tuvalu": "tv",
    }

    def _flag_url(flag_name: str) -> str:
        if not flag_name:
            return None
        code = _FLAG_CODES.get(flag_name)
        return f"https://flagcdn.com/w40/{code}.png" if code else None

    if "v_flag" in arrivals.columns:
        arrivals = arrivals.with_columns(
            pl.col("v_flag").map_elements(_flag_url, return_dtype=pl.String).alias("flag_img")
        )
    else:
        arrivals = arrivals.with_columns(pl.lit(None).cast(pl.String).alias("flag_img"))

    # ETA columns
    now = datetime.now()

    def _eta_from_now(dt_str: str) -> str:
        if not dt_str:
            return ""
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError:
            return ""
        delta = dt - now
        total_seconds = delta.total_seconds()
        if total_seconds < 0:
            hours_ago = int(abs(total_seconds) // 3600)
            if hours_ago < 24:
                return f"{hours_ago}h ago"
            days_ago = hours_ago // 24
            return f"{days_ago}d {hours_ago % 24}h ago"
        days = int(total_seconds // 86400)
        hours = int((total_seconds % 86400) // 3600)
        if days > 0:
            return f"{days}d {hours}h"
        return f"{hours}h"

    def _eta_days(dt_str: str) -> float:
        if not dt_str:
            return None
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError:
            return None
        delta = dt - now
        return round(max(delta.total_seconds() / 86400, 0.0), 2)

    arrivals = arrivals.with_columns([
        pl.col("date_time").map_elements(_eta_from_now, return_dtype=pl.String).alias("eta"),
        pl.col("date_time").map_elements(_eta_days, return_dtype=pl.Float64).alias("eta_days"),
    ])

    # In-port ETA override
    is_in_port = pl.col("movement").str.to_lowercase().str.contains("removal|shift|in port|external")
    arrivals = arrivals.with_columns([
        pl.when(is_in_port).then(pl.lit("In Port")).otherwise(pl.col("eta")).alias("eta"),
        pl.when(is_in_port).then(pl.lit(0.0)).otherwise(pl.col("eta_days")).alias("eta_days"),
    ])

    # Deduplicate by vessel name — keep row closest to arrival
    arrivals = (
        arrivals.sort("eta_days", nulls_last=True)
        .unique(subset=["vessel"], keep="first")
    )

    # ── Supply Pipeline Summary ──
    st.subheader("Supply Pipeline")
    intl_tankers = arrivals.filter(pl.col("origin_type") == "International")
    total_vol_ml = intl_tankers["est_volume_ml"].sum() if "est_volume_ml" in intl_tankers.columns else 0
    total_vol_ml = total_vol_ml or 0

    p1, p2, p3, p4 = st.columns(4)
    p1.metric("International Tankers Inbound", len(intl_tankers))
    p2.metric("Est. Volume On Water", f"{total_vol_ml:,.0f} ML")
    p3.metric("Est. Volume (kbbl)", f"{total_vol_ml * 6.29:,.0f}")
    domestic_tankers = arrivals.filter(pl.col("origin_type") == "Domestic")
    p4.metric("Domestic Transfers", len(domestic_tankers))

    st.divider()

    # Summary metrics
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Incoming Tankers", len(arrivals))
    c2.metric("Domestic", len(domestic_tankers))
    c3.metric("International", len(intl_tankers))
    with_vol = arrivals.filter(pl.col("est_volume") != "")
    c4.metric("With Volume Est.", len(with_vol))
    enriched_count = arrivals.filter(pl.col("v_imo").is_not_null() & (pl.col("v_imo") != "")).height
    c5.metric("With Vessel Specs", enriched_count)

    # LNG toggle
    lng_col1, lng_col2 = st.columns([1, 3])
    with lng_col1:
        include_lng = st.toggle("Include LNG tankers", value=True, key="lng_toggle")

    # Fuzzy vessel name search
    vessel_search = st.text_input(
        "Search vessel name (fuzzy matching)",
        placeholder="e.g. Grace Acacia, Pacfic Vangard",
        key="vessel_search",
    )

    if vessel_search:
        from thefuzz import process
        vessel_names = arrivals["vessel"].unique().to_list()
        matches = process.extract(vessel_search, vessel_names, limit=50)
        matched_names = [m[0] for m in matches if m[1] >= 55]
        arrivals = arrivals.filter(pl.col("vessel").is_in(matched_names))

    if not include_lng:
        arrivals = arrivals.filter(pl.col("cargo_category") != "LNG")

    # Filters
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        origin_opts = arrivals["origin_type"].unique().sort().to_list()
        origin_filter = st.multiselect("Origin Type", origin_opts, default=origin_opts)
    with col2:
        country_opts = (
            arrivals.filter(pl.col("origin_country") != "")
            ["origin_country"].unique().sort().to_list()
        )
        country_filter = st.multiselect("Country", country_opts, default=country_opts)
    with col3:
        port_opts = arrivals["port"].unique().sort().to_list()
        port_filter = st.multiselect("Port", port_opts, default=port_opts, key="inc_port")
    with col4:
        cargo_opts = arrivals.filter(pl.col("cargo_category") != "")["cargo_category"].unique().sort().to_list()
        cargo_filter = st.multiselect("Cargo Category", cargo_opts, default=cargo_opts) if cargo_opts else []

    filtered = arrivals.filter(
        pl.col("origin_type").is_in(origin_filter)
        & pl.col("port").is_in(port_filter)
        & (pl.col("origin_country").is_in(country_filter) | (pl.col("origin_country") == ""))
        & (pl.col("cargo_category").is_in(cargo_filter) | (pl.col("cargo_category") == ""))
    )

    def _col_max(col: str, default: float) -> float:
        if col in filtered.columns:
            v = filtered[col].max()
            if v is not None:
                return float(v)
        return default

    max_gt = _col_max("v_gt", 200000.0)
    max_dwt = _col_max("v_dwt", 320000.0)
    max_length = _col_max("v_length_m", 350.0)
    max_beam = _col_max("v_beam_m", 60.0)

    # Main table
    table_cols = [
        "vessel", "ship_type_detail", "cargo_category",
        "flag_img", "v_flag", "v_gt", "v_dwt",
        "port", "state", "eta", "eta_days", "origin_country", "origin_detail",
        "v_imo", "v_year_built", "v_length_m", "v_beam_m",
        "est_volume_ml", "est_kbbl", "est_confidence", "from_location",
    ]
    st.subheader(f"Tanker Arrivals ({len(filtered)})")
    st.dataframe(
        filtered.select([c for c in table_cols if c in filtered.columns]).to_pandas(),
        use_container_width=True,
        height=500,
        column_config={
            "vessel": "Vessel",
            "ship_type_detail": "Ship Type",
            "cargo_category": "Category",
            "flag_img": st.column_config.ImageColumn("Flag", width="small"),
            "v_flag": "Registry",
            "v_gt": st.column_config.ProgressColumn("Gross Tonnes", min_value=0, max_value=max_gt, format="%.0f"),
            "v_dwt": st.column_config.ProgressColumn("DWT (t)", min_value=0, max_value=max_dwt, format="%.0f"),
            "port": "Port", "state": "State",
            "eta": "ETA",
            "eta_days": st.column_config.NumberColumn("Days", format="%.1f"),
            "origin_country": "Country", "origin_detail": "From (detail)",
            "v_imo": "IMO", "v_year_built": "Built",
            "v_length_m": st.column_config.ProgressColumn("Length (m)", min_value=0, max_value=max_length, format="%.0f"),
            "v_beam_m": st.column_config.ProgressColumn("Beam (m)", min_value=0, max_value=max_beam, format="%.1f"),
            "est_volume_ml": st.column_config.NumberColumn("Vol (ML)", format="%.1f"),
            "est_kbbl": st.column_config.NumberColumn("kbbl", format="%.0f"),
            "est_confidence": "Confidence",
            "from_location": "Last Port",
        },
    )

    # Origin Bubble Map
    st.subheader("Tanker Origins")
    map_rows = []
    for row in filtered.iter_rows(named=True):
        if row["origin_type"] == "International" and row["origin_country"]:
            coords = COUNTRY_COORDS.get(row["origin_country"])
            if coords:
                map_rows.append({"label": row["origin_country"], "type": "International", "lat": coords[0], "lon": coords[1]})
        elif row["origin_type"] == "Domestic" and row["from_location"]:
            loc = row["from_location"].lower().strip()
            coords = AU_PORT_COORDS.get(loc)
            if coords:
                map_rows.append({"label": row["from_location"], "type": "Domestic", "lat": coords[0], "lon": coords[1]})

    if map_rows:
        import pandas as pd
        map_df = pd.DataFrame(map_rows)
        map_agg = (
            map_df.groupby(["label", "type", "lat", "lon"], as_index=False)
            .size().rename(columns={"size": "tankers"})
            .sort_values("tankers", ascending=False)
        )
        fig_map = px.scatter_geo(
            map_agg, lat="lat", lon="lon", size="tankers",
            hover_name="label", hover_data={"tankers": True, "type": True, "lat": False, "lon": False},
            color="type", color_discrete_map={"International": "#d62728", "Domestic": "#1f77b4"},
            size_max=40, projection="natural earth",
        )
        fig_map.update_geos(
            showcountries=True, countrycolor="lightgrey",
            showcoastlines=True, coastlinecolor="grey",
            showland=True, landcolor="#f0f0f0",
            showocean=True, oceancolor="#e6f2ff",
            lataxis_range=[-50, 55], lonaxis_range=[20, 200],
        )
        fig_map.update_layout(height=500, margin=dict(l=0, r=0, t=30, b=0), legend_title_text="Origin Type")
        st.plotly_chart(fig_map, use_container_width=True)

    # Charts row
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("By Origin")
        by_origin = filtered.group_by("origin_type").len().sort("len", descending=True)
        fig = px.pie(by_origin.to_pandas(), values="len", names="origin_type",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("By Country (Top 15)")
        by_country = (
            filtered.filter(pl.col("origin_country") != "")
            .group_by("origin_country").len()
            .sort("len", descending=True).head(15)
        )
        if len(by_country) > 0:
            fig2 = px.bar(
                by_country.to_pandas(), x="len", y="origin_country", orientation="h",
                labels={"len": "Tankers", "origin_country": "Country"},
                color="len", color_continuous_scale="Oranges",
            )
            fig2.update_layout(yaxis=dict(autorange="reversed"), showlegend=False, height=350)
            fig2.update_coloraxes(showscale=False)
            st.plotly_chart(fig2, use_container_width=True)

    # Tankers by port
    st.subheader("Tanker Arrivals by Port")
    by_port = filtered.group_by(["port", "origin_type"]).len().sort("port")
    fig3 = px.bar(
        by_port.to_pandas(), x="port", y="len", color="origin_type",
        labels={"len": "Tankers", "port": "Port", "origin_type": "Origin"},
        barmode="stack",
    )
    fig3.update_layout(height=350)
    st.plotly_chart(fig3, use_container_width=True)

    # Volume breakdown
    st.divider()
    st.subheader("Volume Breakdown (kbbl)")
    vol_col_l, vol_col_r = st.columns(2)
    with vol_col_l:
        st.subheader("Volume by Origin (kbbl)")
        vol_origin = filtered.group_by("origin_type").agg(pl.col("est_kbbl").sum().alias("kbbl")).sort("kbbl", descending=True)
        fig_vo = px.pie(vol_origin.to_pandas(), values="kbbl", names="origin_type",
                        color_discrete_sequence=px.colors.qualitative.Set2)
        fig_vo.update_layout(height=350)
        st.plotly_chart(fig_vo, use_container_width=True)

    with vol_col_r:
        st.subheader("Volume by Country (Top 15, kbbl)")
        vol_country = (
            filtered.filter(pl.col("origin_country") != "")
            .group_by("origin_country").agg(pl.col("est_kbbl").sum().alias("kbbl"))
            .sort("kbbl", descending=True).head(15)
        )
        if len(vol_country) > 0:
            fig_vc = px.bar(
                vol_country.to_pandas(), x="kbbl", y="origin_country", orientation="h",
                labels={"kbbl": "Volume (kbbl)", "origin_country": "Country"},
                color="kbbl", color_continuous_scale="Oranges",
            )
            fig_vc.update_layout(yaxis=dict(autorange="reversed"), showlegend=False, height=350)
            fig_vc.update_coloraxes(showscale=False)
            st.plotly_chart(fig_vc, use_container_width=True)

    st.subheader("Volume by Port (kbbl)")
    vol_port = filtered.group_by(["port", "origin_type"]).agg(pl.col("est_kbbl").sum().alias("kbbl")).sort("port")
    fig_vp = px.bar(
        vol_port.to_pandas(), x="port", y="kbbl", color="origin_type",
        labels={"kbbl": "Volume (kbbl)", "port": "Port", "origin_type": "Origin"},
        barmode="stack",
    )
    fig_vp.update_layout(height=350)
    st.plotly_chart(fig_vp, use_container_width=True)
