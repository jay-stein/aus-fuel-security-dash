"""Shared constants, helpers, and reference data for the fuel security dashboard."""

# ─── Threshold Constants ───────────────────────────────────

IEA_OBLIGATION_DAYS = 90   # IEA 90-day net import cover obligation
COVER_GREEN = 28           # IEA domestic consumption cover guideline (days)
COVER_AMBER = 20           # Critical threshold
COVER_RED = 14             # Emergency threshold
BRENT_AMBER = 100.0        # USD/bbl — elevated price
BRENT_RED = 120.0          # USD/bbl — crisis price
CONCENTRATION_ALERT = 0.40 # Flag when single country > 40% of imports

# MSO surplus thresholds (higher is better — % above minimum stockholding obligation)
MSO_SURPLUS_GREEN = 0.50   # ≥50% surplus above MSO requirement
MSO_SURPLUS_AMBER = 0.10   # ≥10% surplus

# ─── RAG (Red/Amber/Green) Helpers ─────────────────────────

RAG_COLORS = {
    "green": "#28a745",
    "amber": "#ffc107",
    "red": "#dc3545",
}


def rag_status(value: float, green_above: float, amber_above: float) -> str:
    """Return 'green', 'amber', or 'red' based on thresholds.

    green_above > amber_above: higher is better (e.g. days of cover).
    green_above < amber_above: lower is better (e.g. Brent price).
    """
    if value is None:
        return "red"
    if green_above > amber_above:
        # Higher is better (consumption cover, IEA days)
        if value >= green_above:
            return "green"
        elif value >= amber_above:
            return "amber"
        return "red"
    else:
        # Lower is better (Brent price)
        if value <= green_above:
            return "green"
        elif value <= amber_above:
            return "amber"
        return "red"


def rag_color(value: float, green_above: float, amber_above: float) -> str:
    """Return hex color for RAG status."""
    return RAG_COLORS[rag_status(value, green_above, amber_above)]


def rag_label(value: float, green_above: float, amber_above: float) -> str:
    """Return human label for RAG status."""
    labels = {"green": "OK", "amber": "Warning", "red": "Critical"}
    return labels[rag_status(value, green_above, amber_above)]


def rag_icon(value: float, green_above: float, amber_above: float) -> str:
    """Return status icon for RAG."""
    icons = {"green": "✅", "amber": "⚠️", "red": "🔴"}
    return icons[rag_status(value, green_above, amber_above)]


# ─── Product Column Mappings ───────────────────────────────

# Import volume columns → display names
FUEL_COLS = {
    "Crude oil & other refinery feedstocks (ML)": "Crude Oil",
    "Diesel oil (ML)": "Diesel",
    "Automotive gasoline (ML)": "Petrol",
    "Aviation turbine fuel (ML)": "Jet Fuel",
    "LPG (ML)": "LPG",
    "Fuel oil (ML)": "Fuel Oil",
}

# Consumption cover columns → display names
COVER_COLS = {
    "Crude oil and refinery feedstocks (days)": "Crude Oil",
    "Diesel oil (days)": "Diesel",
    "Automotive gasoline (days)": "Petrol",
    "Aviation turbine fuel (days)": "Jet Fuel",
}

# Stock volume columns → display names
STOCK_COLS = {
    "Crude oil and refinery feedstocks (ML)": "Crude Oil",
    "Diesel oil (ML)": "Diesel",
    "Automotive gasoline (ML)": "Petrol",
    "Aviation turbine fuel (ML)": "Jet Fuel",
}

# Sales by state product columns → display names
STATE_SALES_COLS = {
    "Automotive gasoline: total (ML)": "Petrol",
    "Diesel oil: total (ML)": "Diesel",
    "Aviation turbine fuel: total (ML)": "Jet Fuel",
    "Other products (ML)": "Other",
}

# ─── Refinery Reference Data ──────────────────────────────

STATE_REFINERIES = {
    "QLD": [{"name": "Lytton (Ampol)", "capacity_bpd": 109_000,
             "products": ["Petrol", "Diesel", "Jet Fuel"]}],
    "VIC": [{"name": "Geelong (Viva Energy)", "capacity_bpd": 120_000,
             "products": ["Petrol", "Diesel", "Jet Fuel"]}],
    # All other states have no operating refinery
}

# ─── Chokepoint → Source Country Mapping ───────────────────

CHOKEPOINT_ROUTES = {
    "Strait of Malacca": [
        "Singapore", "Malaysia", "Thailand", "India", "Saudi Arabia",
        "UAE", "Kuwait", "Iraq", "Qatar", "Bahrain", "Oman",
    ],
    "Strait of Hormuz": [
        "Saudi Arabia", "UAE", "Kuwait", "Iraq", "Iran",
        "Qatar", "Bahrain", "Oman",
    ],
    "South China Sea": [
        "China", "Taiwan", "Vietnam", "Philippines",
    ],
    "Lombok / Sunda Strait": [
        "South Korea", "Japan", "Korea, Republic of",
    ],
}

# ─── HHI (Herfindahl-Hirschman Index) ─────────────────────

HHI_GREEN = 1500   # Well-diversified
HHI_AMBER = 2500   # Moderately concentrated
# Above 2500 = highly concentrated (red)


def _cache_age_label(path, ttl_hours: float | None = None) -> str:
    """Return a short freshness label for a cache file, e.g. '✅ 2h ago' or '⚠️ 8h ago'."""
    import json
    from datetime import datetime, timedelta, timezone
    from pathlib import Path

    p = Path(path)
    now = datetime.now(tz=timezone.utc)

    ts = None
    if p.suffix == ".json" and p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            raw = data.get("fetched_at") or data.get("_meta", {}).get("fetched_utc")
            if raw:
                ts = datetime.fromisoformat(raw)
        except Exception:
            pass
        if ts is None:
            try:
                ts = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            except Exception:
                pass
    elif p.exists():
        try:
            ts = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        except Exception:
            pass

    if ts is None:
        seed_p = Path(str(path).replace("data/", "seed/"))
        if seed_p.exists():
            return "🌱 seed"
        return "❌ missing"

    age = now - ts
    if age < timedelta(hours=1):
        age_str = f"{int(age.total_seconds() / 60)}m ago"
    elif age < timedelta(hours=48):
        age_str = f"{int(age.total_seconds() / 3600)}h ago"
    else:
        age_str = ts.strftime("%d %b")

    if ttl_hours is None or age < timedelta(hours=ttl_hours):
        return f"✅ {age_str}"
    return f"⚠️ {age_str}"


def render_page_data_freshness(sources: list[tuple[str, str, float | None]]) -> None:
    """Render a compact data freshness row below a page title.

    sources: list of (label, cache_path, ttl_hours)
    Example: [("MSO", "data/mso_weekly.json", 6), ("APS", "data/aus...", None)]
    """
    import streamlit as st
    parts = [f"**{label}** {_cache_age_label(path, ttl)}" for label, path, ttl in sources]
    st.caption("Data freshness — " + " · ".join(parts))


def render_data_freshness_sidebar() -> None:
    """Show data freshness info in the sidebar. Call from every page."""
    import streamlit as st
    from config import is_offline, seed_refreshed_at
    if is_offline():
        ts = seed_refreshed_at()
        date_str = ts.strftime("%d %b %Y") if ts else "unknown"
        st.sidebar.warning(f"Offline mode — data as at {date_str}")
    else:
        st.sidebar.caption("Live data — refreshes on each page load")


def compute_hhi(shares: list[float]) -> float:
    """Compute Herfindahl-Hirschman Index from market shares (0-100 scale).

    shares: list of percentage shares (e.g. [45.0, 30.0, 25.0])
    Returns HHI on 0-10000 scale.
    """
    return sum(s ** 2 for s in shares)
