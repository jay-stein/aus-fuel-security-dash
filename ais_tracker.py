"""AIS vessel position tracker via AISStream.io WebSocket API.

Connects to AISStream, subscribes for known tanker MMSIs in the
Australia region, collects position reports for ~20 seconds, then
disconnects.  Results are cached to data/ais_positions.json and
served to the Streamlit page via @st.cache_data.
"""

import asyncio
import concurrent.futures
import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import websockets

# ── Paths ──────────────────────────────────────────────────────
POSITIONS_CACHE = Path("data/ais_positions.json")
VESSEL_CACHE = Path("data/vessel_cache.json")

# ── AISStream config ──────────────────────────────────────────
WS_URL = "wss://stream.aisstream.io/v0/stream"
SNAPSHOT_DURATION = 45  # seconds to keep the socket open

# Bounding box: southern ocean → equator+10N, eastern Indian Ocean → date line
AUSTRALIA_BBOX = [[-50, 70], [10, 180]]

# ── Port colours for route lines (dest port key → hex) ────────
PORT_COLORS = {
    "kwinana": "#1f77b4",      # blue
    "fremantle": "#1f77b4",
    "geelong": "#2ca02c",      # green
    "botany": "#ff7f0e",       # orange
    "port botany": "#ff7f0e",
    "sydney": "#ff7f0e",
    "brisbane": "#d62728",     # red
    "darwin": "#9467bd",       # purple
    "adelaide": "#17becf",     # teal
    "port adelaide": "#17becf",
    "gladstone": "#bcbd22",    # gold
    "hobart": "#e377c2",       # pink
    "townsville": "#8c564b",   # brown
    "newcastle": "#7f7f7f",    # grey
    "port kembla": "#aec7e8",  # light blue
    "kembla": "#aec7e8",
    "whyalla": "#ffbb78",      # light orange
    "devonport": "#98df8a",    # light green
    "bell bay": "#c5b0d5",     # light purple
    "mackay": "#f7b6d2",       # light pink
    "cairns": "#c49c94",       # light brown
}
DEFAULT_ROUTE_COLOR = "#636363"

# ── Generalised shipping route waypoints ──────────────────────
# (lat, lon) tuples tracing major sea lanes to Australian ports.
# All waypoints are in open water — routes hug coastlines only at
# chokepoints (Lombok, Torres Strait, Cape Leeuwin, Bass Strait).
ROUTE_WAYPOINTS = {
    # ── Singapore / SE Asia approaches ────────────────────────
    "singapore_west_au": [
        (1.2, 103.8),    # Singapore
        (-4.0, 109.0),   # Java Sea
        (-8.5, 115.5),   # Lombok Strait
        (-20.0, 114.0),  # NW Shelf (off Dampier)
        (-28.0, 114.5),  # Off Geraldton
        (-32.0, 115.7),  # Off Fremantle / Kwinana
    ],
    "singapore_east_au_north": [
        (1.2, 103.8),    # Singapore
        (-2.0, 117.0),   # Makassar Strait
        (-5.0, 132.0),   # Banda Sea
        (-10.5, 142.0),  # Torres Strait
        (-16.9, 146.0),  # Off Cairns (outside GBR)
        (-19.3, 147.5),  # Off Townsville
        (-23.8, 152.0),  # Off Gladstone
        (-27.4, 154.0),  # Off Brisbane (Moreton Bay)
    ],
    "singapore_east_au_south": [
        (1.2, 103.8),    # Singapore
        (-2.0, 117.0),   # Makassar Strait
        (-5.0, 132.0),   # Banda Sea
        (-10.5, 142.0),  # Torres Strait
        (-16.9, 146.0),  # Off Cairns (outside GBR)
        (-23.8, 152.0),  # Off Gladstone
        (-33.8, 152.0),  # Off Sydney
        (-37.5, 150.0),  # Off Eden
        (-39.5, 146.5),  # Bass Strait
        (-38.5, 144.5),  # Port Phillip approach / off Geelong
    ],
    "singapore_south_au": [
        (1.2, 103.8),    # Singapore
        (-4.0, 109.0),   # Java Sea
        (-8.5, 115.5),   # Lombok Strait
        (-32.0, 115.7),  # Off Fremantle
        (-34.4, 115.1),  # Cape Leeuwin
        (-37.0, 121.0),  # Off Albany
        (-37.0, 130.0),  # Great Australian Bight mid
        (-37.5, 138.0),  # Off SE South Australia
        (-35.5, 137.5),  # Investigator Strait / off Adelaide
    ],
    # ── Middle East approaches ─────────────────────────────────
    "mideast_west_au": [
        (26.0, 56.5),    # Strait of Hormuz
        (12.5, 50.0),    # Gulf of Aden
        (-5.0, 70.0),    # Central Indian Ocean
        (-20.0, 114.0),  # NW Shelf (off Dampier)
        (-28.0, 114.5),  # Off Geraldton
        (-32.0, 115.7),  # Off Fremantle / Kwinana
    ],
    "mideast_south_au": [
        (26.0, 56.5),    # Strait of Hormuz
        (12.5, 50.0),    # Gulf of Aden
        (-5.0, 70.0),    # Central Indian Ocean
        (-8.5, 115.5),   # Lombok Strait
        (-32.0, 115.7),  # Off Fremantle
        (-34.4, 115.1),  # Cape Leeuwin
        (-37.0, 121.0),  # Off Albany
        (-37.0, 130.0),  # Great Australian Bight
        (-37.5, 138.0),  # Off SE South Australia
        (-35.5, 137.5),  # Investigator Strait / off Adelaide
    ],
    "mideast_east_au": [
        (26.0, 56.5),    # Strait of Hormuz
        (12.5, 50.0),    # Gulf of Aden
        (-5.0, 70.0),    # Central Indian Ocean
        (-8.5, 115.5),   # Lombok Strait
        (-10.5, 125.0),  # Timor Sea
        (-11.0, 132.0),  # North of Darwin
        (-10.5, 136.0),  # Gulf of Carpentaria approach
        (-10.5, 142.0),  # Torres Strait
        (-16.9, 146.0),  # Off Cairns (outside GBR)
        (-23.8, 152.0),  # Off Gladstone
        (-27.4, 154.0),  # Off Brisbane
        (-33.8, 152.0),  # Off Sydney
        (-37.5, 150.0),  # Off Eden
        (-39.5, 146.5),  # Bass Strait
        (-38.5, 144.5),  # Off Geelong
    ],
    # ── NE Asia approaches ────────────────────────────────────
    "korea_japan_north_au": [
        (35.0, 130.0),   # Korea Strait
        (20.0, 125.0),   # Philippine Sea
        (5.0, 125.0),    # South Philippines
        (-5.0, 132.0),   # Banda Sea
        (-10.5, 142.0),  # Torres Strait
        (-16.9, 146.0),  # Off Cairns
        (-19.3, 147.5),  # Off Townsville
        (-23.8, 152.0),  # Off Gladstone
        (-27.4, 154.0),  # Off Brisbane
    ],
    "korea_japan_west_au": [
        (35.0, 130.0),   # Korea Strait
        (20.0, 125.0),   # Philippine Sea
        (5.0, 118.0),    # South China Sea
        (-8.5, 115.5),   # Lombok Strait
        (-20.0, 114.0),  # NW Shelf
        (-28.0, 114.5),  # Off Geraldton
        (-32.0, 115.7),  # Off Fremantle
    ],
    "korea_japan_east_au_south": [
        (35.0, 130.0),   # Korea Strait
        (20.0, 125.0),   # Philippine Sea
        (5.0, 125.0),    # South Philippines
        (-5.0, 132.0),   # Banda Sea
        (-10.5, 142.0),  # Torres Strait
        (-16.9, 146.0),  # Off Cairns (outside GBR)
        (-23.8, 152.0),  # Off Gladstone
        (-33.8, 152.0),  # Off Sydney
        (-37.5, 150.0),  # Off Eden
        (-39.5, 146.5),  # Bass Strait
        (-38.5, 144.5),  # Off Geelong
    ],
    # ── India / Bay of Bengal approaches ─────────────────────
    "india_west_au": [
        (10.0, 80.0),    # South India
        (-5.0, 90.0),    # Central Indian Ocean
        (-8.5, 115.5),   # Lombok Strait
        (-20.0, 114.0),  # NW Shelf
        (-28.0, 114.5),  # Off Geraldton
        (-32.0, 115.7),  # Off Fremantle
    ],
    "india_east_au": [
        (10.0, 80.0),    # South India
        (-5.0, 90.0),    # Central Indian Ocean
        (-8.5, 115.5),   # Lombok Strait
        (-10.5, 125.0),  # Timor Sea
        (-11.0, 132.0),  # North of Darwin
        (-10.5, 136.0),  # Gulf of Carpentaria approach
        (-10.5, 142.0),  # Torres Strait
        (-23.8, 152.0),  # Off Gladstone
        (-27.4, 154.0),  # Off Brisbane
    ],
    # ── Australian coastal corridors ──────────────────────────
    # Used when vessel is already in Australian waters.
    "au_west_to_south_adelaide": [  # WA → SA / Adelaide via Cape Leeuwin + Bight
        (-20.0, 114.0),  # NW Shelf anchor
        (-28.0, 114.5),  # Off Geraldton
        (-32.0, 115.7),  # Off Fremantle
        (-34.4, 115.1),  # Cape Leeuwin
        (-37.0, 121.0),  # Off Albany
        (-37.0, 130.0),  # Great Australian Bight mid
        (-37.5, 138.0),  # Off SE South Australia
        (-35.5, 137.5),  # Investigator Strait / off Adelaide
    ],
    "au_west_to_geelong": [  # WA → Geelong / Melbourne via Cape Leeuwin + Bass Strait
        (-20.0, 114.0),  # NW Shelf
        (-28.0, 114.5),  # Off Geraldton
        (-32.0, 115.7),  # Off Fremantle
        (-34.4, 115.1),  # Cape Leeuwin
        (-37.0, 121.0),  # Off Albany
        (-37.0, 130.0),  # Great Australian Bight
        (-39.5, 140.0),  # South of SA
        (-39.5, 143.5),  # West Bass Strait (north of King Island)
        (-38.5, 144.5),  # Port Phillip Bay approach
    ],
    "au_west_to_east": [  # WA → east coast via Top End + Torres Strait
        (-20.0, 114.0),  # NW Shelf
        (-14.0, 122.0),  # Off Broome (heading north into Timor Sea)
        (-11.0, 132.0),  # Timor Sea / north of Darwin
        (-10.5, 136.0),  # North of Gulf of Carpentaria
        (-10.5, 142.0),  # Torres Strait
        (-16.9, 146.0),  # Off Cairns
        (-23.8, 152.0),  # Off Gladstone
        (-27.4, 154.0),  # Off Brisbane
    ],
    "au_north_to_east": [  # NT coast → east coast via Torres Strait
        (-11.0, 132.0),  # Timor Sea / off Darwin
        (-10.5, 136.0),  # North of Gulf of Carpentaria
        (-10.5, 142.0),  # Torres Strait
        (-16.9, 146.0),  # Off Cairns
        (-19.3, 147.5),  # Off Townsville
        (-23.8, 152.0),  # Off Gladstone
        (-27.4, 154.0),  # Off Brisbane
    ],
    "au_north_to_west": [  # NT / Timor Sea → WA coast (head west)
        (-11.0, 132.0),  # Off Darwin
        (-14.0, 122.0),  # Off Broome
        (-20.0, 114.0),  # NW Shelf / off Dampier
        (-28.0, 114.5),  # Off Geraldton
        (-32.0, 115.7),  # Off Fremantle
    ],
    "au_east_to_geelong": [  # East coast → Geelong / Melbourne via Bass Strait
        (-27.4, 154.0),  # Off Brisbane
        (-33.8, 152.0),  # Off Sydney
        (-37.5, 150.0),  # Off Eden
        (-39.5, 146.5),  # Bass Strait (east of Wilson's Prom)
        (-38.5, 144.5),  # Port Phillip Bay approach / off Geelong
    ],
    "au_east_to_adelaide": [  # East coast → Adelaide via Bass Strait + Bight
        (-27.4, 154.0),  # Off Brisbane
        (-33.8, 152.0),  # Off Sydney
        (-37.5, 150.0),  # Off Eden
        (-39.5, 143.5),  # West Bass Strait
        (-39.5, 140.0),  # South of SA
        (-37.5, 138.0),  # Off SE SA
        (-35.5, 137.5),  # Investigator Strait / off Adelaide
    ],
    "au_south_to_east": [  # SA / Melbourne → east coast via Bass Strait
        (-35.5, 137.5),  # Investigator Strait / off Adelaide
        (-37.5, 138.0),  # Off SE South Australia
        (-39.5, 140.0),  # South of SA
        (-39.5, 143.5),  # West Bass Strait
        (-39.5, 146.5),  # Bass Strait
        (-37.5, 150.0),  # Off Eden
        (-33.8, 152.0),  # Off Sydney
        (-27.4, 154.0),  # Off Brisbane
    ],
    "au_south_to_west": [  # SA / Melbourne → WA via Cape Leeuwin
        (-38.5, 144.5),  # Off Geelong
        (-39.5, 143.5),  # West Bass Strait
        (-39.5, 140.0),  # South of SA
        (-37.0, 130.0),  # Great Australian Bight
        (-37.0, 121.0),  # Off Albany
        (-34.4, 115.1),  # Cape Leeuwin
        (-32.0, 115.7),  # Off Fremantle
    ],
}


def _densify_route(
    waypoints: list[tuple[float, float]],
    max_step_deg: float = 1.0,
) -> list[tuple[float, float]]:
    """Insert linearly-interpolated points so no segment exceeds max_step_deg.
    Prevents Plotly's chord renderer from visually crossing land on long segments.
    """
    if len(waypoints) < 2:
        return list(waypoints)
    result: list[tuple[float, float]] = [waypoints[0]]
    for (lat1, lon1), (lat2, lon2) in zip(waypoints[:-1], waypoints[1:]):
        steps = max(int(abs(lat2 - lat1) / max_step_deg),
                    int(abs(lon2 - lon1) / max_step_deg), 1)
        for k in range(1, steps + 1):
            frac = k / steps
            result.append((lat1 + frac * (lat2 - lat1),
                            lon1 + frac * (lon2 - lon1)))
    return result


# Destination port groupings for route selection
_WEST_AU_PORTS = {"kwinana", "fremantle", "bunbury", "geraldton", "dampier",
                  "port hedland", "broome", "albany", "esperance"}
_NORTH_AU_PORTS = {"darwin", "gove", "weipa", "cairns", "townsville",
                   "mackay", "gladstone", "brisbane", "hay point", "abbot point"}
_SOUTH_AU_PORTS = {"geelong", "melbourne", "portland", "hastings",
                   "adelaide", "port adelaide", "whyalla", "port lincoln",
                   "hobart", "devonport", "bell bay", "launceston", "burnie"}
_EAST_AU_PORTS = {"sydney", "botany", "port botany", "port kembla",
                  "kembla", "newcastle", "eden"}


# ── API key ────────────────────────────────────────────────────

def get_api_key() -> str | None:
    """Return AISStream API key from env var, Streamlit secrets, or secrets.toml."""
    # 1. Environment variable
    key = os.environ.get("AISSTREAM_API")
    if key:
        return key
    # 2. Streamlit secrets (when running inside Streamlit)
    try:
        import streamlit as st
        k = st.secrets.get("aisstream", {}).get("api_key")
        if k:
            return k
    except Exception:
        pass
    # 3. Read .streamlit/secrets.toml directly (for standalone scripts)
    try:
        import tomllib
        toml_path = Path(".streamlit/secrets.toml")
        if toml_path.exists():
            with open(toml_path, "rb") as f:
                data = tomllib.load(f)
            k = data.get("aisstream", {}).get("api_key")
            if k:
                return k
    except Exception:
        pass
    return None


# ── MMSI list from vessel cache ────────────────────────────────

def load_target_mmsis() -> dict[str, str]:
    """Return {mmsi: vessel_name} for all cached vessels with MMSI."""
    if not VESSEL_CACHE.exists():
        return {}
    try:
        data = json.loads(VESSEL_CACHE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    result = {}
    for name, specs in data.items():
        mmsi = specs.get("mmsi", "").strip()
        if mmsi and len(mmsi) >= 9:
            result[mmsi] = name
    return result


# ── Haversine & ETA ────────────────────────────────────────────

def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    R_NM = 3440.065  # Earth radius in nautical miles
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return R_NM * 2 * math.asin(math.sqrt(a))


def estimate_eta_hours(vessel_lat: float, vessel_lon: float,
                       port_lat: float, port_lon: float,
                       speed_knots: float) -> float | None:
    """Estimated hours to arrive. Returns None if speed is ~0."""
    if not speed_knots or speed_knots < 0.5:
        return None
    dist = haversine_nm(vessel_lat, vessel_lon, port_lat, port_lon)
    return dist / speed_knots


def format_eta(hours: float | None) -> str:
    """Format ETA hours as 'Xd Yh' string."""
    if hours is None:
        return "N/A (stationary)"
    if hours < 1:
        return f"{int(hours * 60)}m"
    days = int(hours // 24)
    remaining_h = int(hours % 24)
    if days > 0:
        return f"{days}d {remaining_h}h"
    return f"{remaining_h}h"


# ── Shipping route selection ───────────────────────────────────

def _vessel_region(lat: float, lon: float) -> str:
    """Classify vessel position into a rough origin region."""
    # Already in Australian coastal waters (south of 5°S, between Indian and Pacific)
    if lat < -5 and 105 < lon < 165:
        return "au_waters"
    # Middle East / Arabian Gulf
    if lat > 15 and 40 < lon < 70:
        return "mideast"
    # NE Asia (Korea, Japan, eastern China coast)
    if lat > 20 and lon > 115:
        return "korea_japan"
    # India / Sri Lanka / Bay of Bengal
    if -5 < lat < 30 and 70 < lon < 98:
        return "india"
    # Singapore / SE Asia (covers Indonesia at slightly south of equator too)
    if -5 < lat <= 25 and 95 < lon < 130:
        return "singapore"
    # Southern Indian Ocean / East Africa / southern Africa — route via Lombok like India
    if lon < 105:
        return "india"
    return "unknown"


def _au_sector(lat: float, lon: float) -> str:
    """Classify a position within Australian waters into a coastal sector.

    Used for routing vessels that are already in Australian waters.
    """
    if lon < 122 and lat > -36:
        return "west"    # WA coast
    if lat > -17 and lon < 140:
        return "north"   # NT / Kimberley / Timor Sea
    if lat > -15 and lon >= 138:
        return "ne"      # Torres Strait / Cape York Peninsula
    if lon >= 145 and lat > -40:
        return "east"    # QLD / NSW / VIC east coast
    return "south"       # Southern Ocean / Bight / Bass Strait


def _dest_region(port_name: str) -> str:
    """Classify destination port into west/north/south/east."""
    p = port_name.lower().strip()
    if p in _WEST_AU_PORTS:
        return "west"
    if p in _NORTH_AU_PORTS:
        return "north"
    if p in _SOUTH_AU_PORTS:
        return "south"
    if p in _EAST_AU_PORTS:
        return "east"
    return "south"  # default


def get_route_waypoints(vessel_lat: float, vessel_lon: float,
                        dest_port: str,
                        port_lat: float, port_lon: float) -> list[tuple[float, float]]:
    """Return waypoint list from vessel position to destination port.

    Picks the best generalised route template based on where the vessel
    currently is and which Australian port it's heading to.  Returns a
    list of (lat, lon) tuples: [vessel_pos, ...waypoints..., port_pos].
    """
    origin = _vessel_region(vessel_lat, vessel_lon)
    dest = _dest_region(dest_port)

    # Route selection matrix
    route_key = None
    if origin == "singapore":
        if dest == "west":
            route_key = "singapore_west_au"
        elif dest == "north":
            route_key = "singapore_east_au_north"
        elif dest == "south":
            # Adelaide/SA → go via Lombok then Cape Leeuwin + Bight
            if dest_port in {"adelaide", "port adelaide", "whyalla", "port lincoln"}:
                route_key = "singapore_south_au"
            else:
                route_key = "singapore_east_au_south"  # Geelong via Torres + Bass Strait
        else:  # east
            route_key = "singapore_east_au_south"
    elif origin == "mideast":
        if dest == "west":
            route_key = "mideast_west_au"
        elif dest == "north":
            route_key = "mideast_west_au"   # north WA ports — Indian Ocean direct
        elif dest == "south":
            if dest_port in {"adelaide", "port adelaide", "whyalla", "port lincoln"}:
                route_key = "mideast_south_au"
            else:
                route_key = "mideast_east_au"  # Geelong via Torres + Bass Strait
        else:  # east
            route_key = "mideast_east_au"
    elif origin == "korea_japan":
        if dest in ("north", "east"):
            route_key = "korea_japan_north_au"
        elif dest == "south":
            if dest_port in {"adelaide", "port adelaide", "whyalla", "port lincoln"}:
                route_key = "korea_japan_east_au_south"  # Torres + Bass Strait + Bight
            else:
                route_key = "korea_japan_east_au_south"  # Torres + Bass Strait → Geelong
        else:  # west
            route_key = "korea_japan_west_au"
    elif origin == "india":
        if dest in ("east", "north"):
            route_key = "india_east_au"
        else:
            route_key = "india_west_au"
    elif origin == "au_waters":
        # Vessel already in Australian waters — route along the coast
        sector = _au_sector(vessel_lat, vessel_lon)
        if sector == "west":
            if dest == "west":
                route_key = None  # staying on WA coast — direct is fine
            elif dest == "north":
                route_key = None  # heading north up WA coast — direct
            elif dest == "south":
                if dest_port in {"adelaide", "port adelaide", "whyalla", "port lincoln"}:
                    route_key = "au_west_to_south_adelaide"
                else:
                    route_key = "au_west_to_geelong"
            else:  # east
                route_key = "au_west_to_east"
        elif sector == "north":
            if dest in ("east", "north"):
                route_key = "au_north_to_east"
            elif dest == "west":
                route_key = "au_north_to_west"
            elif dest == "south":
                route_key = "au_north_to_east"  # go east then south via Bass Strait
        elif sector == "ne":
            if dest in ("east", "north"):
                route_key = "au_north_to_east"  # down east coast from Torres
            elif dest == "south":
                route_key = "au_east_to_geelong"
            elif dest == "west":
                route_key = "au_north_to_west"
        elif sector == "east":
            if dest == "east":
                route_key = None  # staying on east coast — direct is fine
            elif dest == "south":
                if dest_port in {"adelaide", "port adelaide", "whyalla", "port lincoln"}:
                    route_key = "au_east_to_adelaide"
                else:
                    route_key = "au_east_to_geelong"
            elif dest == "west":
                route_key = "au_east_to_adelaide"  # go south then west
            elif dest == "north":
                route_key = None  # heading north up east coast — direct
        elif sector == "south":
            if dest in ("east",):
                route_key = "au_south_to_east"
            elif dest == "west":
                route_key = "au_south_to_west"
            elif dest == "south":
                route_key = None  # staying in southern waters — direct

    if route_key and route_key in ROUTE_WAYPOINTS:
        template = ROUTE_WAYPOINTS[route_key]
        # Find the nearest waypoint on the template to the vessel
        # and use the route from that point onward
        best_idx = 0
        best_dist = float("inf")
        for i, (wlat, wlon) in enumerate(template):
            d = haversine_nm(vessel_lat, vessel_lon, wlat, wlon)
            if d < best_dist:
                best_dist = d
                best_idx = i
        # Use waypoints from the nearest onward (vessel may have passed some)
        waypoints = template[best_idx:]
    else:
        waypoints = []

    # Build full path: vessel → waypoints → port
    path = [(vessel_lat, vessel_lon)]
    for wp in waypoints:
        path.append(wp)
    path.append((port_lat, port_lon))
    return _densify_route(path)


def get_port_color(port_name: str) -> str:
    """Return a hex color for a destination port's route line."""
    return PORT_COLORS.get(port_name.lower().strip(), DEFAULT_ROUTE_COLOR)


def estimate_position_on_route(
    origin_lat: float, origin_lon: float,
    dest_port: str, dest_lat: float, dest_lon: float,
    hours_to_go: float,
    avg_speed_knots: float = 14.0,
) -> tuple[float, float]:
    """Dead-reckon a vessel's current position from its ETA.

    Works backwards from the destination along the route template by
    ``hours_to_go * avg_speed_knots`` nautical miles.  If the calculated
    distance exceeds the route length, the approximate origin is returned.

    Returns (lat, lon) of estimated current position.
    """
    dist_remaining = hours_to_go * avg_speed_knots
    waypoints = get_route_waypoints(origin_lat, origin_lon, dest_port, dest_lat, dest_lon)

    if len(waypoints) < 2:
        return origin_lat, origin_lon

    # Walk backwards from the destination end of the route
    remaining = dist_remaining
    for i in range(len(waypoints) - 1, 0, -1):
        seg_lat1, seg_lon1 = waypoints[i - 1]
        seg_lat2, seg_lon2 = waypoints[i]
        seg_dist = haversine_nm(seg_lat1, seg_lon1, seg_lat2, seg_lon2)
        if seg_dist <= 0:
            continue
        if remaining <= seg_dist:
            frac = remaining / seg_dist
            est_lat = seg_lat2 + frac * (seg_lat1 - seg_lat2)
            est_lon = seg_lon2 + frac * (seg_lon1 - seg_lon2)
            return est_lat, est_lon
        remaining -= seg_dist

    # Distance exceeds route — vessel is near or beyond origin point
    return waypoints[0][0], waypoints[0][1]


# ── AIS WebSocket snapshot ─────────────────────────────────────

def _parse_ais_timestamp_age(ts_str: str) -> float | None:
    """Return seconds since the AIS timestamp, or None if unparseable.

    AISStream sends timestamps like '2026-03-30 05:30:58.672292057 +0000 UTC'
    """
    # Strip nanoseconds and timezone label — keep up to microseconds
    clean = re.sub(r"(\.\d{6})\d+", r"\1", ts_str)   # trim nanoseconds
    clean = re.sub(r"\s+UTC$", "", clean).strip()      # remove trailing " UTC"
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f %z",
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            dt = datetime.strptime(clean, fmt)
            age = (datetime.now(timezone.utc) - dt).total_seconds()
            return age
        except ValueError:
            continue
    return None


async def _collect_positions(api_key: str, mmsi_map: dict[str, str],
                             duration: int = SNAPSHOT_DURATION) -> dict:
    """Connect to AISStream and collect PositionReport messages.

    Returns dict keyed by MMSI with position data.
    """
    positions: dict[str, dict] = {}

    # Subscribe to all PositionReport messages in the Australia bbox.
    # We filter by ShipType client-side (80-89 = all tanker variants).
    sub_msg = json.dumps({
        "APIKey": api_key,
        "BoundingBoxes": [AUSTRALIA_BBOX],
        "FilterMessageTypes": ["PositionReport"],
    })

    deadline = time.monotonic() + duration

    async with websockets.connect(WS_URL) as ws:
        await ws.send(sub_msg)

        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("MessageType")
            if msg_type != "PositionReport":
                continue

            meta = msg.get("MetaData", {})
            pos = msg.get("Message", {}).get("PositionReport", {})
            mmsi = str(meta.get("MMSI", ""))

            if not mmsi or not pos:
                continue

            # AIS ship types 80-89 = all tanker variants (oil, chemical, gas, LPG, LNG, etc.)
            ship_type_ais = int(meta.get("ShipType", 0))
            if not (80 <= ship_type_ais <= 89):
                continue

            lat = pos.get("Latitude", 0)
            lon = pos.get("Longitude", 0)
            if lat == 0 and lon == 0:
                continue  # invalid position

            # Reject stale positions older than 2 hours
            ts_str = meta.get("time_utc", "")
            if ts_str:
                ts_age = _parse_ais_timestamp_age(ts_str)
                if ts_age is not None and ts_age > 7200:
                    continue  # skip — stale cached position

            positions[mmsi] = {
                "mmsi": mmsi,
                "vessel_name": meta.get("ShipName", "").strip(),
                "lat": lat,
                "lon": lon,
                "speed_knots": pos.get("Sog", 0),
                "course": pos.get("Cog", 0),
                "heading": pos.get("TrueHeading", 0),
                "nav_status": pos.get("NavigationalStatus", -1),
                "ship_type_ais": ship_type_ais,
                "timestamp_utc": meta.get("time_utc", ""),
            }

    return positions


AIS_CACHE_MAX_AGE = timedelta(hours=12)


def fetch_ais_snapshot() -> dict[str, dict]:
    """Synchronous entry point: fetch AIS positions, cache to disk.

    Returns the disk cache unchanged if it is younger than AIS_CACHE_MAX_AGE
    (12 h).  Uses ThreadPoolExecutor to avoid event-loop conflicts in
    Streamlit.  Falls back to stale disk cache on any fetch failure.
    """
    # Return disk cache if fresh enough (avoids a 45-second fetch)
    if POSITIONS_CACHE.exists():
        try:
            raw = json.loads(POSITIONS_CACHE.read_text(encoding="utf-8"))
            fetched_utc = raw.get("_meta", {}).get("fetched_utc")
            if fetched_utc:
                age = datetime.now(timezone.utc) - datetime.fromisoformat(fetched_utc)
                if age < AIS_CACHE_MAX_AGE:
                    raw.pop("_meta", None)
                    return raw
        except Exception:
            pass

    api_key = get_api_key()
    if not api_key:
        return _load_disk_cache()

    mmsi_map = load_target_mmsis()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, _collect_positions(api_key, mmsi_map))
            positions = future.result(timeout=SNAPSHOT_DURATION + 15)
    except Exception as e:
        print(f"AIS fetch failed: {e}")
        return _load_disk_cache()

    # Merge vessel names from cache for any MMSI we know
    for mmsi, data in positions.items():
        if not data.get("vessel_name") and mmsi in mmsi_map:
            data["vessel_name"] = mmsi_map[mmsi]

    # Save to disk
    _save_disk_cache(positions)
    return positions


def _load_disk_cache() -> dict[str, dict]:
    """Load cached positions from disk."""
    if not POSITIONS_CACHE.exists():
        return {}
    try:
        data = json.loads(POSITIONS_CACHE.read_text(encoding="utf-8"))
        meta = data.pop("_meta", {})
        return data
    except (json.JSONDecodeError, OSError):
        return {}


def load_cached_positions() -> tuple[dict[str, dict], str | None]:
    """Load cached positions + metadata. Returns (positions, fetched_utc)."""
    if not POSITIONS_CACHE.exists():
        return {}, None
    try:
        data = json.loads(POSITIONS_CACHE.read_text(encoding="utf-8"))
        meta = data.pop("_meta", {})
        return data, meta.get("fetched_utc")
    except (json.JSONDecodeError, OSError):
        return {}, None


def _save_disk_cache(positions: dict[str, dict]):
    """Write positions + metadata to disk cache."""
    POSITIONS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    data = dict(positions)
    data["_meta"] = {
        "fetched_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(positions),
        "duration_seconds": SNAPSHOT_DURATION,
    }
    tmp = POSITIONS_CACHE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(POSITIONS_CACHE)


# ── CLI for standalone testing ─────────────────────────────────

if __name__ == "__main__":
    api_key = get_api_key()
    if not api_key:
        print("Error: Set AISSTREAM_API environment variable")
        raise SystemExit(1)

    mmsi_map = load_target_mmsis()
    print(f"Tracking {len(mmsi_map)} vessels with MMSI (max 50 sent to API)")
    print(f"Connecting to AISStream for {SNAPSHOT_DURATION}s...")

    positions = fetch_ais_snapshot()
    print(f"\nReceived {len(positions)} position reports:\n")

    for mmsi, data in sorted(positions.items(), key=lambda x: x[1].get("vessel_name", "")):
        name = data.get("vessel_name", "Unknown")
        lat = data.get("lat", 0)
        lon = data.get("lon", 0)
        speed = data.get("speed_knots", 0)
        course = data.get("course", 0)
        ts = data.get("timestamp_utc", "")
        print(f"  {name:30s}  MMSI={mmsi}  "
              f"Pos=({lat:.3f}, {lon:.3f})  "
              f"Speed={speed:.1f}kn  Course={course:.0f}°  "
              f"Time={ts}")

    print(f"\nCached to {POSITIONS_CACHE}")
