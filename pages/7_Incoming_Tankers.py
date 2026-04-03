"""Incoming Tankers — live tanker movements scraped from Australian port authorities."""

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

from port_scraper import scrape_all_ports, COUNTRY_COORDS, AU_PORT_COORDS
from vessel_lookup import VesselCache


@st.cache_data(ttl=3 * 3600, show_spinner=False)
def _cached_scrape_all(tankers_only: bool = False):
    return scrape_all_ports(tankers_only=tankers_only)


st.set_page_config(page_title="Incoming Tankers", page_icon="🚢", layout="wide")
from dashboard_utils import render_data_freshness_sidebar, render_page_data_freshness
render_data_freshness_sidebar()
st.title("Incoming Fuel Tankers")
render_page_data_freshness([("Ports", "data/port_schedule.json", 3)])
st.caption(
    "Tanker arrivals at Australian ports — filtered to fuel/oil vessels only. "
    "Volume estimates flagged as **High** (from cargo tonnage) or **Rough** (from vessel length)."
)

_col_refresh, _col_gap = st.columns([1, 5])
with _col_refresh:
    if st.button("🔄 Refresh", use_container_width=True):
        from pathlib import Path
        Path("data/port_schedule.json").unlink(missing_ok=True)
        _cached_scrape_all.clear()
        st.rerun()

with st.spinner("Loading vessel movements..."):
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

    def _raw_eta_days(dt_str: str) -> float:
        """Unclamped delta in days — negative means the event is in the past."""
        if not dt_str:
            return None
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError:
            return None
        return (dt - now).total_seconds() / 86400

    arrivals = arrivals.with_columns([
        pl.col("date_time").map_elements(_eta_from_now, return_dtype=pl.String).alias("eta"),
        pl.col("date_time").map_elements(_eta_days, return_dtype=pl.Float64).alias("eta_days"),
        pl.col("date_time").map_elements(_raw_eta_days, return_dtype=pl.Float64).alias("_raw_eta_days"),
    ])

    # In-port ETA override — only applies when the movement is current/recent
    # (within 12 hours past or future). Future removals/shifts are NOT "In Port".
    is_movement_type = pl.col("movement").str.to_lowercase().str.contains("removal|shift|in port|external")
    is_current = pl.col("_raw_eta_days").is_null().not_() & (pl.col("_raw_eta_days") <= 0.5)
    is_in_port = is_movement_type & is_current
    arrivals = arrivals.with_columns([
        pl.when(is_in_port).then(pl.lit("In Port")).otherwise(pl.col("eta")).alias("eta"),
        pl.when(is_in_port).then(pl.lit(0.0)).otherwise(pl.col("eta_days")).alias("eta_days"),
    ]).drop("_raw_eta_days")

    # Deduplicate by vessel name — keep row closest to arrival
    arrivals = (
        arrivals.sort("eta_days", nulls_last=True)
        .unique(subset=["vessel"], keep="first")
    )

    # Trade direction icons
    _TRADE_DIR_ICON = {
        "Import":  "🟢 Import",
        "Export":  "🔴 Export",
        "Coastal": "🔵 Coastal",
        "Unknown": "⚪ Unknown",
        "":        "",
    }
    if "trade_direction" in arrivals.columns:
        arrivals = arrivals.with_columns(
            pl.col("trade_direction")
            .replace(_TRADE_DIR_ICON)
            .alias("trade_dir_icon")
        )

    # ── Supply Pipeline Summary ──
    st.subheader("Supply Pipeline")
    import_tankers  = arrivals.filter(pl.col("trade_direction") == "Import") if "trade_direction" in arrivals.columns else arrivals.filter(pl.col("origin_type") == "International")
    export_tankers  = arrivals.filter(pl.col("trade_direction") == "Export") if "trade_direction" in arrivals.columns else arrivals.head(0)
    coastal_tankers = arrivals.filter(pl.col("trade_direction") == "Coastal") if "trade_direction" in arrivals.columns else arrivals.filter(pl.col("origin_type") == "Domestic")

    import_vol_ml = import_tankers["est_volume_ml"].sum() if "est_volume_ml" in import_tankers.columns else 0
    import_vol_ml = import_vol_ml or 0

    p1, p2, p3, p4 = st.columns(4)
    p1.metric("🟢 Imports Inbound",    len(import_tankers))
    p2.metric("🔴 Export (loading)",   len(export_tankers))
    p3.metric("🔵 Coastal Transfers",  len(coastal_tankers))
    p4.metric("Est. Import Volume",    f"{import_vol_ml:,.0f} ML")

    st.divider()

    # Summary metrics
    intl_tankers = arrivals.filter(pl.col("origin_type") == "International")
    domestic_tankers = arrivals.filter(pl.col("origin_type") == "Domestic")
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
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        dir_opts = sorted([d for d in arrivals["trade_direction"].unique().to_list() if d]) if "trade_direction" in arrivals.columns else []
        dir_filter = st.multiselect("Trade Direction", dir_opts, default=dir_opts) if dir_opts else dir_opts
    with col2:
        origin_opts = arrivals["origin_type"].unique().sort().to_list()
        origin_filter = st.multiselect("Origin Type", origin_opts, default=origin_opts)
    with col3:
        country_opts = (
            arrivals.filter(pl.col("origin_country") != "")
            ["origin_country"].unique().sort().to_list()
        )
        country_filter = st.multiselect("Country", country_opts, default=country_opts)
    with col4:
        port_opts = arrivals["port"].unique().sort().to_list()
        port_filter = st.multiselect("Port", port_opts, default=port_opts, key="inc_port")
    with col5:
        cargo_opts = arrivals.filter(pl.col("cargo_category") != "")["cargo_category"].unique().sort().to_list()
        cargo_filter = st.multiselect("Cargo Category", cargo_opts, default=cargo_opts) if cargo_opts else []

    filtered = arrivals.filter(
        pl.col("origin_type").is_in(origin_filter)
        & pl.col("port").is_in(port_filter)
        & (pl.col("origin_country").is_in(country_filter) | (pl.col("origin_country") == ""))
        & (pl.col("cargo_category").is_in(cargo_filter) | (pl.col("cargo_category") == ""))
    )
    if dir_filter and "trade_direction" in filtered.columns:
        filtered = filtered.filter(pl.col("trade_direction").is_in(dir_filter))

    # ── ETA Arrival Window Histogram ──────────────────────────────
    import pandas as pd

    ETA_BUCKETS = [f"{i} to {i+2}" for i in range(0, 30, 2)] + ["30+"]

    def _eta_bucket(eta_d) -> str:
        if eta_d is None or eta_d > 30:
            return "30+"
        for i in range(0, 30, 2):
            if eta_d <= i + 2:
                return f"{i} to {i+2}"
        return "30+"

    filtered_bucketed = filtered.with_columns(
        pl.col("eta_days").map_elements(_eta_bucket, return_dtype=pl.String).alias("eta_bucket")
    )

    CARGO_COLORS = {
        "Crude Oil":    "#8B0000",
        "Oil Products": "#1f77b4",
        "Chemical/Oil": "#ff7f0e",
        "LPG":          "#9467bd",
        "LNG":          "#2ca02c",
        "Bitumen":      "#7f7f7f",
        "Tanker (other)": "#bcbd22",
    }
    # All cargo categories present in the filtered data (for legend ordering)
    all_cargo_cats = [c for c in CARGO_COLORS if c in (
        filtered_bucketed["cargo_category"].unique().to_list()
    )]
    # Include any categories not in our CARGO_COLORS dict
    extra_cats = [
        c for c in filtered_bucketed["cargo_category"].unique().to_list()
        if c and c not in CARGO_COLORS and c not in all_cargo_cats
    ]
    all_cargo_cats += extra_cats

    _vol_agg = (
        pl.col("est_volume_ml").sum().alias("volume_ml")
        if "est_volume_ml" in filtered_bucketed.columns
        else pl.lit(0.0).alias("volume_ml")
    )
    # Group by bucket AND cargo category for stacked bars
    bucket_cargo_agg = (
        filtered_bucketed
        .group_by(["eta_bucket", "cargo_category"])
        .agg([pl.len().alias("count"), _vol_agg])
    )
    bucket_cargo_pd = bucket_cargo_agg.to_pandas()
    bucket_cargo_pd["_order"] = bucket_cargo_pd["eta_bucket"].map(
        {b: i for i, b in enumerate(ETA_BUCKETS)}
    )
    bucket_cargo_pd = bucket_cargo_pd.sort_values("_order").drop(columns=["_order"])

    # Histogram header row
    hist_title_col, hist_clear_col = st.columns([5, 1])
    with hist_title_col:
        st.subheader("Arrival Window")
    with hist_clear_col:
        selected_bucket = st.session_state.get("eta_bucket_selected")
        if selected_bucket:
            if st.button(f"✕ {selected_bucket} days", key="clear_bucket", help="Clear ETA filter"):
                st.session_state["eta_bucket_selected"] = None
                st.rerun()

    hist_chart_col, hist_opt_col = st.columns([5, 1])
    with hist_opt_col:
        hist_metric = st.radio("Show", ["Count", "Volume (ML)"], key="hist_metric")

    with hist_chart_col:
        selected_bucket = st.session_state.get("eta_bucket_selected")
        y_col = "count" if hist_metric == "Count" else "volume_ml"
        y_label = "Tankers" if hist_metric == "Count" else "Volume (ML)"

        fig_hist = go.Figure()
        fallback_colors = px.colors.qualitative.Set1
        for i, cat in enumerate(all_cargo_cats):
            cat_data = bucket_cargo_pd[bucket_cargo_pd["cargo_category"] == cat]
            # Merge against full bucket list to get zeros for missing buckets
            cat_merged = pd.DataFrame({"eta_bucket": ETA_BUCKETS}).merge(
                cat_data[["eta_bucket", y_col]], on="eta_bucket", how="left"
            ).fillna(0)
            color = CARGO_COLORS.get(cat, fallback_colors[i % len(fallback_colors)])
            # Dim non-selected buckets when a filter is active
            if selected_bucket:
                opacities = [1.0 if b == selected_bucket else 0.25 for b in cat_merged["eta_bucket"]]
            else:
                opacities = [1.0] * len(cat_merged)
            fig_hist.add_trace(go.Bar(
                name=cat or "Unknown",
                x=cat_merged["eta_bucket"],
                y=cat_merged[y_col],
                marker=dict(color=color, opacity=opacities),
                hovertemplate=(
                    f"<b>%{{x}} days</b><br>{cat}<br>"
                    + y_label + ": %{y:,.0f}<extra></extra>"
                ),
            ))
        fig_hist.update_layout(
            barmode="stack",
            xaxis_title="Days to Arrival",
            yaxis_title=y_label,
            height=300,
            margin=dict(t=5, b=5, l=0, r=0),
            xaxis=dict(
                type="category",
                categoryorder="array",
                categoryarray=ETA_BUCKETS,
            ),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            bargap=0.1,
        )
        event = st.plotly_chart(
            fig_hist, use_container_width=True,
            on_select="rerun", selection_mode="points",
            key="eta_hist",
        )
        # Handle click — update session state and rerun to apply filter
        if event and hasattr(event, "selection"):
            pts = (event.selection or {}).get("points", [])
            if pts:
                clicked = pts[0].get("x")
                if clicked != st.session_state.get("eta_bucket_selected"):
                    st.session_state["eta_bucket_selected"] = clicked
                    st.rerun()

    # Apply bucket cross-filter
    selected_bucket = st.session_state.get("eta_bucket_selected")
    if selected_bucket:
        display_df = filtered_bucketed.filter(pl.col("eta_bucket") == selected_bucket)
        st.info(
            f"Filtered to **{selected_bucket} days** — showing {len(display_df)} of "
            f"{len(filtered)} tankers. Click the same bar or **✕** above to clear."
        )
    else:
        display_df = filtered_bucketed

    def _col_max(col: str, default: float) -> float:
        if col in display_df.columns:
            v = display_df[col].max()
            if v is not None:
                return float(v)
        return default

    max_gt = _col_max("v_gt", 200000.0)
    max_dwt = _col_max("v_dwt", 320000.0)
    max_length = _col_max("v_length_m", 350.0)
    max_beam = _col_max("v_beam_m", 60.0)

    # Main table
    table_cols = [
        "vessel", "trade_dir_icon", "ship_type_detail", "cargo_category",
        "flag_img", "v_flag", "v_gt", "v_dwt",
        "port", "state", "eta", "eta_days", "origin_country", "origin_detail",
        "v_imo", "v_year_built", "v_length_m", "v_beam_m",
        "est_volume_ml", "est_kbbl", "est_confidence", "from_location",
    ]
    st.subheader(f"Tanker Arrivals ({len(display_df)})")
    st.dataframe(
        display_df.select([c for c in table_cols if c in display_df.columns]).to_pandas(),
        use_container_width=True,
        height=500,
        column_config={
            "vessel": "Vessel",
            "trade_dir_icon": "Direction",
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

    st.caption(
        "🟢 **Import** — delivering imported fuel to Australia  ·  "
        "🔴 **Export** — arriving in ballast to load Australian LNG/crude for export  ·  "
        "🔵 **Coastal** — moving product between Australian ports  ·  "
        "Classification is based on origin port and destination berth; not guaranteed."
    )

    # Origin Bubble Map
    st.subheader("Tanker Origins")
    map_rows = []
    for row in display_df.iter_rows(named=True):
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
        by_origin = display_df.group_by("origin_type").len().sort("len", descending=True)
        fig = px.pie(by_origin.to_pandas(), values="len", names="origin_type",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("By Country (Top 15)")
        by_country = (
            display_df.filter(pl.col("origin_country") != "")
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
    by_port = display_df.group_by(["port", "origin_type"]).len().sort("port")
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
        vol_origin = display_df.group_by("origin_type").agg(pl.col("est_kbbl").sum().alias("kbbl")).sort("kbbl", descending=True)
        fig_vo = px.pie(vol_origin.to_pandas(), values="kbbl", names="origin_type",
                        color_discrete_sequence=px.colors.qualitative.Set2)
        fig_vo.update_layout(height=350)
        st.plotly_chart(fig_vo, use_container_width=True)

    with vol_col_r:
        st.subheader("Volume by Country (Top 15, kbbl)")
        vol_country = (
            display_df.filter(pl.col("origin_country") != "")
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
    vol_port = display_df.group_by(["port", "origin_type"]).agg(pl.col("est_kbbl").sum().alias("kbbl")).sort("port")
    fig_vp = px.bar(
        vol_port.to_pandas(), x="port", y="kbbl", color="origin_type",
        labels={"kbbl": "Volume (kbbl)", "port": "Port", "origin_type": "Origin"},
        barmode="stack",
    )
    fig_vp.update_layout(height=350)
    st.plotly_chart(fig_vp, use_container_width=True)
