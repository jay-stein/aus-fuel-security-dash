"""Fuel Tanker Tracker — dead-reckoned en-route positions + port-schedule confirmed in-port vessels."""

import json
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime, timezone
from pathlib import Path

from port_scraper import scrape_all_ports, AU_PORT_COORDS, COUNTRY_COORDS, FROM_PORT_COORDS
from vessel_lookup import VesselCache, CACHE_PATH as VESSEL_CACHE_PATH
from ais_tracker import (
    haversine_nm,
    estimate_eta_hours,
    format_eta,
    get_route_waypoints,
    get_port_color,
    PORT_COLORS,
    voyage_progress,
)


def _load_shipnext_cache() -> dict[str, dict]:
    """Load cached ShipNext positions from file.

    Returns dict mapping IMO → {"lat", "lon", "cached_at", "ais_updated_at"}.
    """
    cache_file = Path("data/shipnext_positions.json")
    if not cache_file.exists():
        return {}
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        result = {}
        for key, entry in data.items():
            if key.startswith("imo_") and "position" in entry:
                imo = key.replace("imo_", "")
                pos = entry["position"]
                if isinstance(pos, (list, tuple)) and len(pos) == 2:
                    result[imo] = {
                        "lat": pos[0],
                        "lon": pos[1],
                        "cached_at": entry.get("cached_at"),
                        "ais_updated_at": entry.get("ais_updated_at"),
                    }
        return result
    except Exception:
        return {}

st.set_page_config(page_title="Fuel Tanker Tracker", page_icon="🗺️", layout="wide")
from dashboard_utils import render_data_freshness_sidebar, render_page_data_freshness
render_data_freshness_sidebar()
st.title("Fuel Tanker Tracker")
render_page_data_freshness([
    ("Ports", "data/port_schedule.json", 3),
    ("Vessels", "data/vessel_cache.json", None),
])
st.caption(
    "Fuel tanker positions derived from port authority schedules and dead-reckoned from ETA. "
    "🟢 Import · 🔴 Export · 🔵 Coastal · ⚫ In Port."
)

# ── Helpers ────────────────────────────────────────────────────

MAJOR_FUEL_PORTS = [
    "kwinana", "fremantle", "geelong", "port botany", "brisbane",
    "darwin", "adelaide", "gladstone", "hobart", "townsville",
    "newcastle", "port kembla", "whyalla", "melbourne",
]


def _gt_to_marker_size(gt) -> int:
    """Scale Gross Tonnage to marker pixel size (8–20)."""
    try:
        gt = float(gt)
    except (TypeError, ValueError):
        return 10
    if gt < 10_000:
        return 8
    if gt > 100_000:
        return 20
    return int(8 + (gt - 10_000) / (100_000 - 10_000) * 12)


# Trade direction → marker fill colour
_TRADE_COLORS = {
    "Import":  "#2ca02c",  # green
    "Export":  "#d62728",  # red
    "Coastal": "#1f77b4",  # blue
    "Unknown": "#888888",
    "":        "#888888",
}

# ── Load data ──────────────────────────────────────────────────

@st.cache_data(ttl=3 * 3600, show_spinner=False)
def _get_port_data():
    return scrape_all_ports(tankers_only=True)


vessel_cache_data: dict = {}
if VESSEL_CACHE_PATH.exists():
    try:
        vessel_cache_data = json.loads(VESSEL_CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        pass

with st.spinner("Loading port schedule data..."):
    try:
        port_df = _get_port_data()
    except Exception:
        port_df = None

# ── Build vessel list ──────────────────────────────────────────

vessels: list[dict] = []
seen_names: set[str] = set()

# Layer 1: In-Port vessels (confirmed by port schedule)
if port_df is not None and len(port_df) > 0:
    for row in port_df.iter_rows(named=True):
        if (row.get("in_port") or "").strip().lower() != "yes":
            continue
        name = (row.get("vessel") or "").strip().upper()
        if not name or name in seen_names:
            continue
        port_name = (row.get("port") or "").lower().strip()
        port_coords = AU_PORT_COORDS.get(port_name)
        if not port_coords:
            continue
        specs = vessel_cache_data.get(name, {})
        gt = specs.get("gt", "")
        trade_dir = (row.get("trade_direction") or "").strip()
        vessels.append({
            "name": name,
            "imo": specs.get("imo", ""),
            "ship_type": specs.get("ship_type", row.get("vessel_type", "")),
            "flag": specs.get("flag", ""),
            "year_built": specs.get("year_built", ""),
            "gt": gt,
            "dwt": specs.get("dwt", ""),
            "length_m": specs.get("length_m", ""),
            "lat": port_coords[0],
            "lon": port_coords[1],
            "dest_port": port_name,
            "dest_state": row.get("state", ""),
            "cargo": row.get("cargo_type", ""),
            "origin": row.get("from_location", ""),
            "origin_country": row.get("origin_country", ""),
            "trade_direction": trade_dir,
            "eta_hours": None,
            "eta_display": "In Port",
            "pct_complete": None,
            "dist_remaining_nm": None,
            "marker_size": _gt_to_marker_size(gt),
            "timestamp": row.get("date_time", ""),
            "pos_source": "port_schedule",
            "shipnext_cached_at": None,
            "shipnext_ais_updated_at": None,
            "source": "port_schedule",
        })
        seen_names.add(name)

# Layer 2: En-Route vessels (use ShipNext live positions if available, else dead-reckoned from ETA)
# Load cached ShipNext positions once
_shipnext_cache = _load_shipnext_cache()

if port_df is not None and len(port_df) > 0:
    for row in port_df.iter_rows(named=True):
        if (row.get("in_port") or "").strip().lower() == "yes":
            continue
        name = (row.get("vessel") or "").strip().upper()
        if not name or name in seen_names:
            continue
        date_time_str = (row.get("date_time") or "").strip()
        if not date_time_str:
            continue
        try:
            eta_dt = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            hours_to_go = (eta_dt - datetime.now(timezone.utc)).total_seconds() / 3600
        except ValueError:
            continue
        if hours_to_go < 6 or hours_to_go > 30 * 24:
            continue  # skip past ETAs and vessels too far out
        dest_port_name = (row.get("port") or "").lower().strip()
        dest_coords = AU_PORT_COORDS.get(dest_port_name)
        if not dest_coords:
            continue
        origin_country = (row.get("origin_country") or "").strip()
        from_location = (row.get("from_location") or "").strip()
        origin_coords = (
            FROM_PORT_COORDS.get(from_location.lower())
            or COUNTRY_COORDS.get(origin_country)
            or COUNTRY_COORDS.get(from_location)
        )
        if not origin_coords:
            continue

        specs = vessel_cache_data.get(name, {})
        imo = specs.get("imo", "")

        # Check for cached ShipNext position first (gold-standard live data)
        est_lat = None
        est_lon = None
        pct = None
        dist_rem = None
        pos_source = "dead_reckoned"
        shipnext_cached_at = None
        shipnext_ais_updated_at = None

        sn_entry = _shipnext_cache.get(imo) if imo else None
        if sn_entry:
            est_lat = sn_entry["lat"]
            est_lon = sn_entry["lon"]
            shipnext_cached_at = sn_entry.get("cached_at")
            shipnext_ais_updated_at = sn_entry.get("ais_updated_at")
            pos_source = "shipnext"
        else:
            # Fall back to dead-reckoned position from voyage_progress
            try:
                prog = voyage_progress(
                    origin_coords[0], origin_coords[1],
                    dest_port_name, dest_coords[0], dest_coords[1],
                    hours_to_go,
                )
                est_lat = prog["current_lat"]
                est_lon = prog["current_lon"]
                pct = round(prog["pct_complete"], 1)
                dist_rem = round(prog["dist_remaining_nm"], 0)
            except Exception:
                continue

        gt = specs.get("gt", "")
        trade_dir = (row.get("trade_direction") or "").strip()
        vessels.append({
            "name": name,
            "imo": imo,
            "ship_type": specs.get("ship_type", row.get("vessel_type", "")),
            "flag": specs.get("flag", ""),
            "year_built": specs.get("year_built", ""),
            "gt": gt,
            "dwt": specs.get("dwt", ""),
            "length_m": specs.get("length_m", ""),
            "lat": est_lat,
            "lon": est_lon,
            "dest_port": dest_port_name,
            "dest_state": row.get("state", ""),
            "cargo": row.get("cargo_type", ""),
            "origin": from_location,
            "origin_country": origin_country,
            "trade_direction": trade_dir,
            "eta_hours": hours_to_go,
            "eta_display": format_eta(hours_to_go),
            "pct_complete": pct,
            "dist_remaining_nm": dist_rem,
            "marker_size": _gt_to_marker_size(gt),
            "timestamp": "",
            "pos_source": pos_source,
            "shipnext_cached_at": shipnext_cached_at,
            "shipnext_ais_updated_at": shipnext_ais_updated_at,
            "source": "en_route",
        })
        seen_names.add(name)


# ── Sidebar filters ────────────────────────────────────────────

st.sidebar.markdown("### Filters")

all_trade_dirs = sorted(set(v["trade_direction"] for v in vessels if v["trade_direction"]))
if all_trade_dirs:
    dir_filter = st.sidebar.multiselect("Trade Direction", all_trade_dirs, default=all_trade_dirs)
else:
    dir_filter = []

all_cargos = sorted(set(v["cargo"] for v in vessels if v["cargo"]))
if all_cargos:
    cargo_filter = st.sidebar.multiselect("Cargo Type", all_cargos, default=all_cargos)
else:
    cargo_filter = []

show_routes = st.sidebar.checkbox("Show route lines", value=True)

# Apply filters
filtered = [
    v for v in vessels
    if (not dir_filter or not v["trade_direction"] or v["trade_direction"] in dir_filter)
    and (not all_cargos or not v["cargo"] or v["cargo"] in cargo_filter)
]


# ── Metrics row ────────────────────────────────────────────────

en_route_count   = sum(1 for v in filtered if v["source"] == "en_route")
in_port_count    = sum(1 for v in filtered if v["source"] == "port_schedule")
import_count     = sum(1 for v in filtered if v["trade_direction"] == "Import")
export_count     = sum(1 for v in filtered if v["trade_direction"] == "Export")
shipnext_count   = sum(1 for v in filtered if v.get("pos_source") == "shipnext")

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Vessels Tracked", len(filtered))
m2.metric("En Route", en_route_count)
m3.metric("In Port", in_port_count)
m4.metric("🟢 Imports", import_count)
m5.metric("🔴 Exports", export_count)
m6.metric("📡 Live (ShipNext)", shipnext_count)


# ── Build map ──────────────────────────────────────────────────

fig = go.Figure()

# Port markers
port_names_major = [p for p in AU_PORT_COORDS if p in MAJOR_FUEL_PORTS]
port_names_minor = [p for p in AU_PORT_COORDS if p not in MAJOR_FUEL_PORTS]

fig.add_trace(go.Scattergeo(
    lat=[AU_PORT_COORDS[p][0] for p in port_names_major],
    lon=[AU_PORT_COORDS[p][1] for p in port_names_major],
    text=[p.title() for p in port_names_major],
    mode="markers+text", textposition="top center",
    marker=dict(size=7, color="red", symbol="diamond"),
    textfont=dict(size=9, color="darkred"),
    hoverinfo="text", showlegend=False,
))
fig.add_trace(go.Scattergeo(
    lat=[AU_PORT_COORDS[p][0] for p in port_names_minor],
    lon=[AU_PORT_COORDS[p][1] for p in port_names_minor],
    text=[p.title() for p in port_names_minor],
    mode="markers",
    marker=dict(size=4, color="darkred", symbol="diamond", opacity=0.5),
    hoverinfo="text", showlegend=False,
))

# Route lines (drawn before vessel markers)
if show_routes and filtered:
    for v in filtered:
        if v["source"] == "port_schedule":
            continue  # no route line for in-port vessels
        dest = v["dest_port"]
        dest_coords = AU_PORT_COORDS.get(dest)
        if not dest_coords:
            continue
        waypoints = get_route_waypoints(v["lat"], v["lon"], dest, dest_coords[0], dest_coords[1])
        route_color = _TRADE_COLORS.get(v["trade_direction"], "#888888")
        fig.add_trace(go.Scattergeo(
            lat=[wp[0] for wp in waypoints],
            lon=[wp[1] for wp in waypoints],
            mode="lines",
            line=dict(width=1.2, color=route_color, dash="dot"),
            hoverinfo="skip", showlegend=False,
        ))

# Vessel markers
if filtered:
    for v in filtered:
        source = v["source"]
        trade_dir = v["trade_direction"]
        color = _TRADE_COLORS.get(trade_dir, "#888888")

        if source == "port_schedule":
            symbol = "circle"
            opacity = 0.7
            line_color = "#555555"
            line_width = 1
            fill_color = "#888888"
        else:  # en_route
            symbol = "circle"
            opacity = 0.85
            line_color = "white"
            line_width = 1
            fill_color = color

        # Hover text
        hover_lines = [f"<b>🚢 {v['name'] or 'Unknown'}</b>"]

        id_parts = []
        if v["imo"]:
            id_parts.append(f"IMO: {v['imo']}")
        if id_parts:
            hover_lines.append(" | ".join(id_parts))

        type_parts = []
        if v["ship_type"]:
            type_parts.append(f"Type: {v['ship_type']}")
        if v["flag"]:
            type_parts.append(f"Flag: {v['flag']}")
        if v["year_built"]:
            type_parts.append(f"Built: {v['year_built']}")
        if type_parts:
            hover_lines.append(" | ".join(type_parts))

        size_parts = []
        if v["gt"]:
            try:
                size_parts.append(f"GT: {float(v['gt']):,.0f} t")
            except (ValueError, TypeError):
                size_parts.append(f"GT: {v['gt']}")
        if v["dwt"]:
            try:
                size_parts.append(f"DWT: {float(v['dwt']):,.0f} t")
            except (ValueError, TypeError):
                size_parts.append(f"DWT: {v['dwt']} t")
        if v["length_m"]:
            try:
                size_parts.append(f"L: {float(v['length_m']):.0f}m")
            except (ValueError, TypeError):
                size_parts.append(f"L: {v['length_m']}m")
        if size_parts:
            hover_lines.append(" | ".join(size_parts))

        dest_label = v["dest_port"].title()
        if v["dest_state"]:
            dest_label += f" ({v['dest_state']})"
        hover_lines.append(f"Destination: {dest_label}")
        hover_lines.append(f"ETA: {v['eta_display']}")

        # Voyage progress (en-route only)
        if source == "en_route" and v["pct_complete"] is not None:
            hover_lines.append(
                f"Progress: {v['pct_complete']:.0f}% complete  ·  "
                f"{v['dist_remaining_nm']:,.0f} nm remaining"
            )

        if v["cargo"]:
            hover_lines.append(f"Cargo: {v['cargo']}")
        if v["origin"]:
            origin_str = v["origin"]
            if v["origin_country"]:
                origin_str += f" ({v['origin_country']})"
            hover_lines.append(f"Origin: {origin_str}")
        if trade_dir:
            dir_label = {"Import": "🟢 Import", "Export": "🔴 Export", "Coastal": "🔵 Coastal"}.get(trade_dir, trade_dir)
            hover_lines.append(f"Direction: {dir_label}")

        hover_lines.append("─────────────────────")
        # Position source line
        if source == "port_schedule":
            pos_label = "📍 In Port — port schedule confirmed"
            if v["timestamp"]:
                pos_label += f"  ·  {v['timestamp']}"
        elif v.get("pos_source") == "shipnext":
            pos_label = "📡 Position: ShipNext live AIS"
        else:
            pos_label = "📍 Position: dead-reckoned from ETA"
        hover_lines.append(pos_label)

        # Lat/lon + last-updated (for all positioned vessels)
        if v["lat"] is not None and v["lon"] is not None:
            hover_lines.append(f"Coords: {v['lat']:.4f}°, {v['lon']:.4f}°")

        if v.get("pos_source") == "shipnext":
            # Show AIS observation time if available, else fall back to our fetch time
            ais_ts = v.get("shipnext_ais_updated_at")
            fetch_ts = v.get("shipnext_cached_at")
            if ais_ts:
                try:
                    dt = datetime.fromisoformat(str(ais_ts).replace("Z", "+00:00"))
                    hover_lines.append(f"AIS updated: {dt.strftime('%Y-%m-%d %H:%M UTC')}")
                except Exception:
                    hover_lines.append(f"AIS updated: {ais_ts}")
            elif fetch_ts:
                try:
                    dt = datetime.fromisoformat(str(fetch_ts).replace("Z", "+00:00"))
                    hover_lines.append(f"Fetched from ShipNext: {dt.strftime('%Y-%m-%d %H:%M UTC')}")
                except Exception:
                    hover_lines.append(f"Fetched from ShipNext: {fetch_ts}")

        hover_text = "<br>".join(hover_lines)

        fig.add_trace(go.Scattergeo(
            lat=[v["lat"]],
            lon=[v["lon"]],
            mode="markers",
            marker=dict(
                size=v["marker_size"],
                color=fill_color,
                symbol=symbol,
                line=dict(width=line_width, color=line_color),
                opacity=opacity,
            ),
            hovertemplate=hover_text + "<extra></extra>",
            showlegend=False,
        ))

fig.update_geos(
    projection_type="natural earth",
    showcoastlines=True, coastlinecolor="gray",
    showland=True, landcolor="rgb(243,243,243)",
    showocean=True, oceancolor="rgb(204,224,245)",
    showcountries=True, countrycolor="lightgray",
    center=dict(lat=-20, lon=125),
    lataxis_range=[-50, 15],
    lonaxis_range=[60, 185],
)
fig.update_layout(height=700, margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
st.plotly_chart(fig, use_container_width=True)

# Legend
legend_cols = st.columns(6)
with legend_cols[0]:
    st.markdown("<span style='color:#2ca02c'>●</span> **Import** (En Route)", unsafe_allow_html=True)
with legend_cols[1]:
    st.markdown("<span style='color:#d62728'>●</span> **Export** (En Route)", unsafe_allow_html=True)
with legend_cols[2]:
    st.markdown("<span style='color:#1f77b4'>●</span> **Coastal** (En Route)", unsafe_allow_html=True)
with legend_cols[3]:
    st.markdown("<span style='color:#888888'>●</span> **In Port** (schedule)", unsafe_allow_html=True)
with legend_cols[4]:
    st.markdown(":red[◆] Major Fuel Port")
with legend_cols[5]:
    st.markdown("Size = Gross Tonnage")

st.divider()

# ── Vessel detail table ────────────────────────────────────────

st.subheader("Vessel Details")

if filtered:
    import pandas as pd

    table_data = []
    for v in filtered:
        gt_str = ""
        if v["gt"]:
            try:
                gt_str = f"{float(v['gt']):,.0f}"
            except (ValueError, TypeError):
                gt_str = str(v["gt"])
        dwt_str = ""
        if v["dwt"]:
            try:
                dwt_str = f"{float(v['dwt']):,.0f}"
            except (ValueError, TypeError):
                dwt_str = str(v["dwt"])

        dest_label = v["dest_port"].title()
        if v["dest_state"]:
            dest_label += f" ({v['dest_state']})"

        dir_icons = {"Import": "🟢 Import", "Export": "🔴 Export", "Coastal": "🔵 Coastal"}
        source_labels = {"en_route": "En Route (est.)", "port_schedule": "In Port"}
        pct_str = f"{v['pct_complete']:.0f}%" if v["pct_complete"] is not None else ""
        dist_str = f"{v['dist_remaining_nm']:,.0f}" if v["dist_remaining_nm"] is not None else ""

        table_data.append({
            "Vessel": v["name"],
            "Direction": dir_icons.get(v["trade_direction"], v["trade_direction"] or "⚪"),
            "Flag": v["flag"],
            "Type": v["ship_type"],
            "GT": gt_str,
            "DWT": dwt_str,
            "Destination": dest_label,
            "Cargo": v["cargo"],
            "ETA": v["eta_display"],
            "Progress": v["pct_complete"],
            "Dist. left (nm)": v["dist_remaining_nm"],
            "Origin": v["origin_country"] or v["origin"],
            "Position": source_labels.get(v["source"], v["source"]),
        })

    table_df = pd.DataFrame(table_data)

    # Sort: In Port first, then by eta_hours ascending
    eta_sort = []
    for v in filtered:
        if v["source"] == "port_schedule":
            eta_sort.append(-1)
        elif v["eta_hours"] is not None:
            eta_sort.append(v["eta_hours"])
        else:
            eta_sort.append(999999)
    table_df["_sort"] = eta_sort
    table_df = table_df.sort_values("_sort").drop(columns=["_sort"])

    max_pct = 100.0
    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
        height=min(500, 35 * len(table_df) + 38),
        column_config={
            "Progress": st.column_config.ProgressColumn("Progress", min_value=0, max_value=max_pct, format="%.0f%%"),
            "Dist. left (nm)": st.column_config.NumberColumn("Dist. left (nm)", format="%.0f"),
        },
    )
else:
    st.info("No vessels to display with current filters.")
