"""Fuel Tanker Tracker — live AIS positions of fuel tankers approaching Australia."""

import json
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime, timezone
from pathlib import Path

from port_scraper import scrape_all_ports, AU_PORT_COORDS, COUNTRY_COORDS
from vessel_lookup import VesselCache, CACHE_PATH as VESSEL_CACHE_PATH
from ais_tracker import (
    get_api_key,
    fetch_ais_snapshot,
    load_cached_positions,
    load_target_mmsis,
    haversine_nm,
    estimate_eta_hours,
    estimate_position_on_route,
    format_eta,
    get_route_waypoints,
    get_port_color,
    PORT_COLORS,
    DEFAULT_ROUTE_COLOR,
)

st.set_page_config(page_title="Fuel Tanker Tracker", page_icon="🗺️", layout="wide")
from dashboard_utils import render_data_freshness_sidebar
render_data_freshness_sidebar()
st.title("Fuel Tanker Tracker")
st.caption(
    "Real-time AIS positions of fuel tankers approaching or berthed at Australian ports. "
    "Data from AISStream.io — refreshes every 5 minutes."
)

# ── Helpers ────────────────────────────────────────────────────

MAJOR_FUEL_PORTS = [
    "kwinana", "fremantle", "geelong", "port botany", "brisbane",
    "darwin", "adelaide", "gladstone", "hobart", "townsville",
    "newcastle", "port kembla", "whyalla",
]


def _nearest_port(lat: float, lon: float) -> tuple[str, float]:
    """Find the nearest Australian port and distance in nm."""
    best_name, best_dist = "", float("inf")
    for name, (plat, plon) in AU_PORT_COORDS.items():
        d = haversine_nm(lat, lon, plat, plon)
        if d < best_dist:
            best_dist = d
            best_name = name
    return best_name, best_dist


def _classify_status(speed: float, lat: float, lon: float) -> tuple[str, str]:
    """Return (status_label, nearest_port) for a vessel."""
    port_name, port_dist = _nearest_port(lat, lon)
    if speed < 0.5 and port_dist < 5:
        return "In Port", port_name
    if speed >= 0.5:
        return "Approaching", port_name
    return "Stationary", port_name


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


STATUS_COLORS = {
    "In Port": "#2ca02c",      # green
    "Approaching": "#ff7f0e",  # orange
    "Stationary": "#1f77b4",   # blue
}


# ── API key check ──────────────────────────────────────────────

api_key = get_api_key()
if not api_key:
    st.warning(
        "**AIS tracking not configured.** Set the `AISSTREAM_API` environment variable "
        "with your AISStream.io API key, or add it to `.streamlit/secrets.toml`:\n\n"
        "```toml\n[aisstream]\napi_key = \"your-key-here\"\n```"
    )
    # Show placeholder port-only map
    fig = go.Figure()
    port_names = list(AU_PORT_COORDS.keys())
    fig.add_trace(go.Scattergeo(
        lat=[AU_PORT_COORDS[p][0] for p in port_names],
        lon=[AU_PORT_COORDS[p][1] for p in port_names],
        text=port_names, mode="markers+text", textposition="top center",
        marker=dict(size=8, color="red", symbol="diamond"),
        textfont=dict(size=9),
    ))
    fig.update_geos(
        projection_type="natural earth", showcoastlines=True, coastlinecolor="gray",
        showland=True, landcolor="rgb(243,243,243)",
        showocean=True, oceancolor="rgb(204,224,245)",
        center=dict(lat=-25, lon=134), lataxis_range=[-50, 10], lonaxis_range=[90, 180],
    )
    fig.update_layout(height=600, margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    st.stop()


# ── Fetch AIS data ─────────────────────────────────────────────

@st.cache_data(ttl=43200, show_spinner=False)
def _fetch_positions():
    return fetch_ais_snapshot()


@st.cache_data(ttl=3 * 3600, show_spinner=False)
def _get_port_data():
    return scrape_all_ports(tankers_only=True)


with st.spinner("Fetching live AIS positions (~45 seconds on first load)..."):
    positions = _fetch_positions()

# Load vessel cache for specs
vessel_cache_data = {}
if VESSEL_CACHE_PATH.exists():
    try:
        vessel_cache_data = json.loads(VESSEL_CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        pass

# Build MMSI → vessel_name map
mmsi_map = load_target_mmsis()

# Load port scraper data for destination/cargo info
try:
    port_df = _get_port_data()
except Exception:
    port_df = None

# Build vessel_name → port scraper info map
port_info_map: dict[str, dict] = {}
if port_df is not None and len(port_df) > 0:
    for row in port_df.iter_rows(named=True):
        name = (row.get("vessel") or "").strip().upper()
        if name:
            port_info_map[name] = {
                "dest_port": (row.get("port") or "").lower().strip(),
                "cargo_type": row.get("cargo_type") or "",
                "origin": row.get("from_location") or "",
                "origin_country": row.get("origin_country") or "",
                "movement": row.get("movement") or "",
                "state": row.get("state") or "",
            }


# ── Merge data ─────────────────────────────────────────────────

vessels = []
for mmsi, ais in positions.items():
    name = ais.get("vessel_name", "").strip().upper()
    # Fall back to MMSI map name
    if not name and mmsi in mmsi_map:
        name = mmsi_map[mmsi]

    # Get specs from vessel cache
    specs = vessel_cache_data.get(name, {})

    # Get port scraper info
    pinfo = port_info_map.get(name, {})

    lat = ais.get("lat", 0)
    lon = ais.get("lon", 0)
    speed = ais.get("speed_knots", 0)
    course = ais.get("course", 0)
    heading = ais.get("heading", 0)

    status, nearest = _classify_status(speed, lat, lon)

    # Destination: prefer port scraper data, fall back to nearest port
    dest_port = pinfo.get("dest_port") or nearest
    dest_state = pinfo.get("state", "")

    # Get port coords for ETA
    dest_coords = AU_PORT_COORDS.get(dest_port)
    eta_h = None
    if dest_coords:
        eta_h = estimate_eta_hours(lat, lon, dest_coords[0], dest_coords[1], speed)

    gt = specs.get("gt", "")
    dwt = specs.get("dwt", "")

    vessels.append({
        "name": name,
        "mmsi": mmsi,
        "imo": specs.get("imo", ""),
        "call_sign": specs.get("call_sign", ""),
        "ship_type": specs.get("ship_type", ""),
        "flag": specs.get("flag", ""),
        "year_built": specs.get("year_built", ""),
        "gt": gt,
        "dwt": dwt,
        "length_m": specs.get("length_m", ""),
        "beam_m": specs.get("beam_m", ""),
        "speed": speed,
        "course": course,
        "heading": heading,
        "lat": lat,
        "lon": lon,
        "status": status,
        "dest_port": dest_port,
        "dest_state": dest_state,
        "cargo": pinfo.get("cargo_type", ""),
        "origin": pinfo.get("origin", ""),
        "origin_country": pinfo.get("origin_country", ""),
        "eta_hours": eta_h,
        "eta_display": format_eta(eta_h),
        "marker_size": _gt_to_marker_size(gt),
        "timestamp": ais.get("timestamp_utc", ""),
        "source": "ais",
    })

# Names already accounted for by AIS — used to deduplicate port schedule
ais_names = {v["name"] for v in vessels if v["name"]}

# ── Port-schedule overlay: vessels confirmed "In Port" ─────────
# These appear at port coordinates but have no live AIS signal.
if port_df is not None and len(port_df) > 0:
    for row in port_df.iter_rows(named=True):
        if (row.get("in_port") or "").strip().lower() != "yes":
            continue
        name = (row.get("vessel") or "").strip().upper()
        if not name or name in ais_names:
            continue
        port_name = (row.get("port") or "").lower().strip()
        port_coords = AU_PORT_COORDS.get(port_name)
        if not port_coords:
            continue
        specs = vessel_cache_data.get(name, {})
        gt = specs.get("gt", "")
        vessels.append({
            "name": name,
            "mmsi": "",
            "imo": specs.get("imo", ""),
            "call_sign": specs.get("call_sign", ""),
            "ship_type": specs.get("ship_type", row.get("vessel_type", "")),
            "flag": specs.get("flag", ""),
            "year_built": specs.get("year_built", ""),
            "gt": gt,
            "dwt": specs.get("dwt", ""),
            "length_m": specs.get("length_m", ""),
            "beam_m": specs.get("beam_m", ""),
            "speed": 0.0,
            "course": 0.0,
            "heading": 0,
            "lat": port_coords[0],
            "lon": port_coords[1],
            "status": "In Port",
            "dest_port": port_name,
            "dest_state": row.get("state", ""),
            "cargo": row.get("cargo_type", ""),
            "origin": row.get("from_location", ""),
            "origin_country": row.get("origin_country", ""),
            "eta_hours": None,
            "eta_display": "In Port",
            "marker_size": _gt_to_marker_size(gt),
            "timestamp": row.get("date_time", ""),
            "source": "port_schedule",
        })
        ais_names.add(name)

# ── Estimated-position overlay: incoming vessels with future ETA ─
# Dead-reckoned from approximate origin + hours-to-go.
# Only show if ETA > 12 h (closer vessels should be in AIS range).
if port_df is not None and len(port_df) > 0:
    for row in port_df.iter_rows(named=True):
        if (row.get("in_port") or "").strip().lower() == "yes":
            continue
        name = (row.get("vessel") or "").strip().upper()
        if not name or name in ais_names:
            continue
        date_time_str = (row.get("date_time") or "").strip()
        if not date_time_str:
            continue
        try:
            eta_dt = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            hours_to_go = (eta_dt - datetime.now(timezone.utc)).total_seconds() / 3600
        except ValueError:
            continue
        if hours_to_go < 12 or hours_to_go > 30 * 24:
            continue  # skip past ETAs and vessels too far out
        dest_port_name = (row.get("port") or "").lower().strip()
        dest_coords = AU_PORT_COORDS.get(dest_port_name)
        if not dest_coords:
            continue
        origin_country = (row.get("origin_country") or "").strip()
        from_location = (row.get("from_location") or "").strip()
        origin_coords = COUNTRY_COORDS.get(origin_country) or COUNTRY_COORDS.get(from_location)
        if not origin_coords:
            continue
        try:
            est_lat, est_lon = estimate_position_on_route(
                origin_coords[0], origin_coords[1],
                dest_port_name, dest_coords[0], dest_coords[1],
                hours_to_go,
            )
        except Exception:
            continue
        specs = vessel_cache_data.get(name, {})
        gt = specs.get("gt", "")
        vessels.append({
            "name": name,
            "mmsi": "",
            "imo": specs.get("imo", ""),
            "call_sign": "",
            "ship_type": specs.get("ship_type", row.get("vessel_type", "")),
            "flag": specs.get("flag", ""),
            "year_built": specs.get("year_built", ""),
            "gt": gt,
            "dwt": specs.get("dwt", ""),
            "length_m": specs.get("length_m", ""),
            "beam_m": specs.get("beam_m", ""),
            "speed": 14.0,
            "course": 0.0,
            "heading": 0,
            "lat": est_lat,
            "lon": est_lon,
            "status": "Approaching",
            "dest_port": dest_port_name,
            "dest_state": row.get("state", ""),
            "cargo": row.get("cargo_type", ""),
            "origin": from_location,
            "origin_country": origin_country,
            "eta_hours": hours_to_go,
            "eta_display": format_eta(hours_to_go),
            "marker_size": _gt_to_marker_size(gt),
            "timestamp": "",
            "source": "estimated",
        })
        ais_names.add(name)


# ── Sidebar filters ────────────────────────────────────────────

st.sidebar.markdown("### Filters")

# Last updated
cached_pos, fetched_utc = load_cached_positions()
if fetched_utc:
    st.sidebar.caption(f"Last updated: {fetched_utc[:19]}Z")

# Status filter
all_statuses = sorted(set(v["status"] for v in vessels)) if vessels else []
status_filter = st.sidebar.multiselect("Status", all_statuses, default=all_statuses)

# Cargo filter
all_cargos = sorted(set(v["cargo"] for v in vessels if v["cargo"])) if vessels else []
if all_cargos:
    cargo_filter = st.sidebar.multiselect("Cargo Type", all_cargos, default=all_cargos)
else:
    cargo_filter = []

show_routes = st.sidebar.checkbox("Show route lines", value=True)

# Apply filters
filtered = [v for v in vessels
            if v["status"] in status_filter
            and (not all_cargos or not v["cargo"] or v["cargo"] in cargo_filter)]


# ── Metrics row ────────────────────────────────────────────────

ais_vessel_count = sum(1 for v in vessels if v.get("source") == "ais")
if not ais_vessel_count:
    st.info(
        "No live AIS positions received — tankers may be outside coverage or "
        "AISStream temporarily unavailable. Showing port-schedule and estimated positions."
    )

m1, m2, m3, m4, m5 = st.columns(5)
with m1:
    st.metric("Vessels Tracked", len(filtered))
with m2:
    live_ais = sum(1 for v in filtered if v.get("source") == "ais")
    st.metric("Live AIS", live_ais)
with m3:
    en_route = sum(1 for v in filtered if v["status"] == "Approaching")
    st.metric("En Route", en_route)
with m4:
    in_port = sum(1 for v in filtered if v["status"] == "In Port")
    st.metric("In Port", in_port)
with m5:
    etas = [v["eta_hours"] for v in filtered if v["eta_hours"] is not None]
    avg_eta = sum(etas) / len(etas) if etas else None
    st.metric("Avg ETA", format_eta(avg_eta) if avg_eta else "N/A")


# ── Build map ──────────────────────────────────────────────────

fig = go.Figure()

# Layer 1: Australian fuel ports
port_names_major = [p for p in AU_PORT_COORDS if p in MAJOR_FUEL_PORTS]
port_names_minor = [p for p in AU_PORT_COORDS if p not in MAJOR_FUEL_PORTS]

fig.add_trace(go.Scattergeo(
    lat=[AU_PORT_COORDS[p][0] for p in port_names_major],
    lon=[AU_PORT_COORDS[p][1] for p in port_names_major],
    text=[p.title() for p in port_names_major],
    mode="markers+text",
    textposition="top center",
    marker=dict(size=7, color="red", symbol="diamond"),
    textfont=dict(size=9, color="darkred"),
    name="Major Fuel Ports",
    hoverinfo="text",
    showlegend=False,
))

fig.add_trace(go.Scattergeo(
    lat=[AU_PORT_COORDS[p][0] for p in port_names_minor],
    lon=[AU_PORT_COORDS[p][1] for p in port_names_minor],
    text=[p.title() for p in port_names_minor],
    mode="markers",
    marker=dict(size=4, color="darkred", symbol="diamond", opacity=0.5),
    name="Other Ports",
    hoverinfo="text",
    showlegend=False,
))

# Layer 2: Route lines (before vessel markers so vessels draw on top)
if show_routes and filtered:
    for v in filtered:
        if v["status"] == "In Port":
            continue  # no route line for vessels already in port
        dest = v["dest_port"]
        dest_coords = AU_PORT_COORDS.get(dest)
        if not dest_coords:
            continue

        source = v.get("source", "ais")
        waypoints = get_route_waypoints(
            v["lat"], v["lon"], dest, dest_coords[0], dest_coords[1]
        )
        if source == "estimated":
            route_color = "#cccccc"
            line_width = 1.0
            dash = "dot"
        else:
            route_color = get_port_color(dest)
            line_width = 1.5
            dash = "dash"

        fig.add_trace(go.Scattergeo(
            lat=[wp[0] for wp in waypoints],
            lon=[wp[1] for wp in waypoints],
            mode="lines",
            line=dict(width=line_width, color=route_color, dash=dash),
            hoverinfo="skip",
            showlegend=False,
        ))

# Layer 3: Vessel markers
if filtered:
    for v in filtered:
        source = v.get("source", "ais")

        # Marker style by source
        if source == "ais":
            color = STATUS_COLORS.get(v["status"], "#636363")
            symbol = "triangle-up"
            opacity = 0.9
            line_color = "white"
            line_width = 1
        elif source == "port_schedule":
            color = "#888888"
            symbol = "circle"
            opacity = 0.8
            line_color = "#555555"
            line_width = 1
        else:  # estimated
            color = "#bbbbbb"
            symbol = "circle-open"
            opacity = 0.6
            line_color = "#bbbbbb"
            line_width = 2

        # Build rich hover block
        hover_lines = [f"<b>🚢 {v['name'] or 'Unknown'}</b>"]

        id_parts = []
        if v["imo"]:
            id_parts.append(f"IMO: {v['imo']}")
        if v["mmsi"]:
            id_parts.append(f"MMSI: {v['mmsi']}")
        if v["call_sign"]:
            id_parts.append(f"Call: {v['call_sign']}")
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
        if v["beam_m"]:
            try:
                size_parts.append(f"B: {float(v['beam_m']):.0f}m")
            except (ValueError, TypeError):
                size_parts.append(f"B: {v['beam_m']}m")
        if size_parts:
            hover_lines.append(" | ".join(size_parts))

        if source == "ais":
            nav_parts = [f"Speed: {v['speed']:.1f} kn", f"Course: {v['course']:.0f}°"]
            if v["heading"] and v["heading"] != 511:
                nav_parts.append(f"Heading: {v['heading']}°")
            hover_lines.append(" | ".join(nav_parts))

        dest_label = v["dest_port"].title()
        if v["dest_state"]:
            dest_label += f" ({v['dest_state']})"
        hover_lines.append(f"Destination: {dest_label}")
        hover_lines.append(f"ETA: {v['eta_display']}")

        if v["cargo"]:
            hover_lines.append(f"Cargo: {v['cargo']}")
        if v["origin"]:
            origin_str = v["origin"]
            if v["origin_country"]:
                origin_str += f" ({v['origin_country']})"
            hover_lines.append(f"Origin: {origin_str}")

        # Position source label
        hover_lines.append("─────────────────────")
        if source == "ais":
            pos_label = "📍 AIS position (live)"
            if v["timestamp"]:
                pos_label += f"  ·  {v['timestamp'][:16]}Z"
        elif source == "port_schedule":
            pos_label = "📍 In Port — port schedule confirmed"
            if v["timestamp"]:
                pos_label += f"  ·  {v['timestamp']}"
        else:
            pos_label = f"📍 Estimated position (dead reckoning)"
        hover_lines.append(pos_label)

        hover_text = "<br>".join(hover_lines)

        fig.add_trace(go.Scattergeo(
            lat=[v["lat"]],
            lon=[v["lon"]],
            mode="markers",
            marker=dict(
                size=v["marker_size"],
                color=color,
                symbol=symbol,
                line=dict(width=line_width, color=line_color),
                opacity=opacity,
            ),
            hovertemplate=hover_text + "<extra></extra>",
            showlegend=False,
        ))

# Map layout
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
fig.update_layout(
    height=700,
    margin=dict(l=0, r=0, t=0, b=0),
    showlegend=False,
)
st.plotly_chart(fig, use_container_width=True)

# Legend
legend_cols = st.columns(7)
with legend_cols[0]:
    st.markdown(":green[▲] In Port (AIS)")
with legend_cols[1]:
    st.markdown(":orange[▲] Approaching (AIS)")
with legend_cols[2]:
    st.markdown(":blue[▲] Stationary (AIS)")
with legend_cols[3]:
    st.markdown("<span style='color:#888888'>●</span> In Port (schedule)", unsafe_allow_html=True)
with legend_cols[4]:
    st.markdown("<span style='color:#bbbbbb'>○</span> Estimated position", unsafe_allow_html=True)
with legend_cols[5]:
    st.markdown(":red[◆] Major Fuel Port")
with legend_cols[6]:
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

        source_labels = {"ais": "AIS (live)", "port_schedule": "Port schedule", "estimated": "Estimated"}
        table_data.append({
            "Vessel": v["name"],
            "Flag": v["flag"],
            "Type": v["ship_type"],
            "GT": gt_str,
            "DWT": dwt_str,
            "Speed (kn)": f"{v['speed']:.1f}",
            "Destination": dest_label,
            "Cargo": v["cargo"],
            "ETA": v["eta_display"],
            "Origin": v["origin_country"] or v["origin"],
            "Status": v["status"],
            "Position": source_labels.get(v.get("source", "ais"), v.get("source", "")),
        })

    table_df = pd.DataFrame(table_data)

    # Sort by ETA: In Port first, then by eta_hours ascending
    eta_sort = []
    for v in filtered:
        if v["status"] == "In Port":
            eta_sort.append(-1)
        elif v["eta_hours"] is not None:
            eta_sort.append(v["eta_hours"])
        else:
            eta_sort.append(999999)
    table_df["_sort"] = eta_sort
    table_df = table_df.sort_values("_sort").drop(columns=["_sort"])

    def _color_status(val):
        if val == "In Port":
            return "background-color: #d4edda"
        elif val == "Approaching":
            return "background-color: #fff3cd"
        return ""

    st.dataframe(
        table_df.style.applymap(_color_status, subset=["Status"]),
        use_container_width=True,
        hide_index=True,
        height=min(400, 35 * len(table_df) + 38),
    )
else:
    st.info("No vessels to display with current filters.")

# ── Port route color legend ────────────────────────────────────

st.divider()
with st.expander("Route line color legend"):
    legend_items = []
    seen = set()
    for port, color in PORT_COLORS.items():
        if color not in seen:
            seen.add(color)
            legend_items.append(f"- <span style='color:{color}'>━━━</span> {port.title()}")
    st.markdown("\n".join(legend_items), unsafe_allow_html=True)
