"""Fetch live vessel positions from ShipNext API (no Playwright needed)."""

import json
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx


# Rotate user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

# Cache file for ShipNext positions
SHIPNEXT_CACHE = Path("data/shipnext_positions.json")


def _load_cache() -> dict:
    """Load cached ShipNext positions."""
    if SHIPNEXT_CACHE.exists():
        try:
            return json.loads(SHIPNEXT_CACHE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    """Save cached ShipNext positions."""
    SHIPNEXT_CACHE.parent.mkdir(parents=True, exist_ok=True)
    try:
        SHIPNEXT_CACHE.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    except Exception:
        pass


def fetch_shipnext_position(imo: str, vessel_name: str = "", retries: int = 2) -> Optional[tuple[float, float]]:
    """Fetch live vessel position from ShipNext API with smart retries.

    Uses the public JSON API: https://shipnext.com/api/v2/fleet/public/{imo}
    No Playwright, no HTML scraping, instant responses.
    Retries on network failures with exponential backoff.

    Args:
        imo: IMO number (e.g., "9538440")
        vessel_name: Vessel name (unused, for compatibility)
        retries: Number of retry attempts on failure (default 2 = 3 total attempts)

    Returns:
        (lat, lon) tuple or None if unavailable after retries
    """
    if not imo:
        return None

    cache_key = f"imo_{imo}"
    cache = _load_cache()

    # Check if we have a recent cached result (3 hour TTL)
    if cache_key in cache:
        entry = cache[cache_key]
        try:
            cached_at = datetime.fromisoformat(entry.get("cached_at", ""))
            if datetime.now(timezone.utc) - cached_at < timedelta(hours=3):
                pos = entry.get("position")
                if pos:
                    return tuple(pos)
        except Exception:
            pass

    # Fetch from ShipNext API with retries
    url = f"https://shipnext.com/api/v2/fleet/public/{imo}"

    for attempt in range(retries + 1):
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://shipnext.com/",
                "DNT": "1",
            }

            response = httpx.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data.get("errors"):
                    return None  # Vessel not found (not a retry-able error)

                vessel_data = data.get("data", {})
                location = vessel_data.get("location", {})
                coords = location.get("coordinates")

                if coords and len(coords) == 2:
                    # ShipNext API returns [lon, lat], we need (lat, lon)
                    lon, lat = coords
                    pos = (lat, lon)

                    # Capture AIS observation timestamp.
                    # ShipNext API returns location.lastPosUpdatedAt = when the vessel's
                    # AIS transponder was last received (matches the website's
                    # "Last Position update" field).
                    ais_updated_at = (
                        location.get("lastPosUpdatedAt")
                        or location.get("timestamp")
                        or vessel_data.get("hasKnownLocationSince")
                    )

                    # Update cache
                    cache[cache_key] = {
                        "position": list(pos),
                        "cached_at": datetime.now(timezone.utc).isoformat(),
                        "ais_updated_at": ais_updated_at,
                    }
                    _save_cache(cache)

                    return pos

                return None  # No coordinates found (not retry-able)

            elif response.status_code in (429, 503, 520):
                # Rate limit or server error — retry with backoff + jitter
                if attempt < retries:
                    backoff = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s...
                    jitter = random.uniform(backoff * 0.5, backoff * 1.5)  # ±50% randomness
                    time.sleep(jitter)
                    continue
                return None

            else:
                # Other HTTP errors — don't retry
                return None

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            # Network errors — retry with backoff + jitter
            if attempt < retries:
                backoff = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s...
                jitter = random.uniform(backoff * 0.5, backoff * 1.5)  # ±50% randomness
                time.sleep(jitter)
                continue
            return None

        except Exception:
            # Other exceptions — don't retry
            return None

    return None


def fetch_all_vessel_positions(vessels: list[dict], progress_callback=None, delay_sec: float = 1.0) -> tuple[dict[str, tuple[float, float]], list[tuple]]:
    """Fetch ShipNext positions for all vessels with IMOs using the API.

    Much faster than Playwright — no rate limiting issues, instant responses.

    Args:
        vessels: List of vessel dicts (each with 'v_imo' and 'vessel' keys)
        progress_callback: Callable(i, total, vessel_name) for progress updates
        delay_sec: Delay between requests in seconds (1.0 is fine for API)

    Returns:
        Tuple of (results dict, errors list)
        - results: Dict mapping IMO → (lat, lon) of successfully fetched positions
        - errors: List of (imo, vessel_name, reason) tuples
    """
    results = {}
    errors = []
    # Deduplicate by IMO — keep first occurrence of each vessel
    seen_imos: set[str] = set()
    deduped: list[dict] = []
    for v in vessels:
        imo = (v.get("v_imo") or "").strip()
        if imo and imo not in seen_imos:
            seen_imos.add(imo)
            deduped.append(v)
    with_imo = deduped

    for i, vessel in enumerate(with_imo):
        imo = vessel.get("v_imo", "").strip()
        name = vessel.get("vessel", "Unknown").strip()

        if progress_callback:
            progress_callback(i, len(with_imo), name)

        try:
            pos = fetch_shipnext_position(imo, name)
            if pos:
                results[imo] = pos
            else:
                errors.append((imo, name, "No position data found on ShipNext"))
        except Exception as e:
            errors.append((imo, name, f"Exception: {str(e)}"))

        # Small delay to be respectful
        if i < len(with_imo) - 1:
            jitter = random.uniform(delay_sec - 0.2, delay_sec + 0.3)
            time.sleep(jitter)

    return results, errors
