"""Vessel enrichment: look up tanker specs by name, cache locally."""

import json
import re
import time
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
import polars as pl


CACHE_PATH = Path("data/vessel_cache.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

# Fields we store per vessel
SPEC_FIELDS = [
    "imo", "mmsi", "call_sign", "ship_type", "flag", "year_built",
    "gt", "dwt", "length_m", "beam_m", "draft_m",
    "speed_knots", "builder",
]


class VesselCache:
    """JSON-backed cache of vessel specifications."""

    def __init__(self):
        self._cache: dict[str, dict] = self._load()

    def _load(self) -> dict:
        if CACHE_PATH.exists():
            try:
                return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return {}
        return {}

    def _save(self):
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = CACHE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._cache, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(CACHE_PATH)

    @staticmethod
    def _key(name: str) -> str:
        return name.strip().upper()

    def get(self, vessel_name: str) -> dict | None:
        return self._cache.get(self._key(vessel_name))

    def put(self, vessel_name: str, specs: dict):
        self._cache[self._key(vessel_name)] = specs
        self._save()

    def __len__(self):
        return len(self._cache)

    def enrich_dataframe(self, df: pl.DataFrame, progress_callback=None) -> pl.DataFrame:
        """Enrich a Polars DataFrame with cached + freshly looked-up vessel specs.

        Only looks up vessels where is_tanker=True and specs aren't cached.
        progress_callback(current, total, vessel_name) is called during lookups.
        """
        tanker_names = (
            df.filter(pl.col("is_tanker"))["vessel"]
            .unique()
            .to_list()
        )

        # Find uncached vessels
        uncached = [n for n in tanker_names if n and self.get(n) is None]

        if uncached:
            total = len(uncached)
            for i, name in enumerate(uncached):
                if progress_callback:
                    progress_callback(i, total, name)
                specs = lookup_vessel(name)
                if specs:
                    self.put(name, specs)
                else:
                    # Store empty dict so we don't retry
                    self.put(name, {})

        # Build spec columns from cache
        spec_data = {field: [] for field in SPEC_FIELDS}
        for row_name in df["vessel"].to_list():
            cached = self.get(row_name) if row_name else None
            for field in SPEC_FIELDS:
                spec_data[field].append(cached.get(field) if cached else None)

        # Add columns to DataFrame
        for field in SPEC_FIELDS:
            if field in ("gt", "dwt", "length_m", "beam_m", "draft_m", "speed_knots"):
                df = df.with_columns(
                    pl.Series(name=f"v_{field}", values=spec_data[field], dtype=pl.Float64)
                )
            else:
                df = df.with_columns(
                    pl.Series(name=f"v_{field}", values=spec_data[field], dtype=pl.String)
                )

        return df


def lookup_vessel(name: str) -> dict | None:
    """Look up vessel specs by name. Returns dict of specs or None."""
    try:
        # Step 1: search MyShipTracking for IMO/MMSI
        imo, mmsi = _search_myshiptracking(name)
        if not imo:
            return None

        time.sleep(2)

        # Step 2: fetch full specs from VesselFinder
        specs = _fetch_vesselfinder_specs(imo)
        if specs:
            # Ensure IMO/MMSI are set even if VF page didn't have them
            specs.setdefault("imo", imo)
            specs.setdefault("mmsi", mmsi or "")
            return specs

        # Fallback: return just IMO/MMSI from search
        return {"imo": imo, "mmsi": mmsi or ""}

    except Exception as e:
        print(f"  Warning: lookup failed for {name}: {e}")
        return None


def _search_myshiptracking(name: str) -> tuple[str | None, str | None]:
    """Search MyShipTracking for a vessel by name. Returns (imo, mmsi) or (None, None)."""
    try:
        url = f"https://www.myshiptracking.com/vessels?name={name.replace(' ', '+')}"
        client = httpx.Client(headers=HEADERS, timeout=15, follow_redirects=True)
        resp = client.get(url)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")

        # Look for vessel links that contain imo- in the href
        for a in soup.find_all("a", href=True):
            href = a["href"]
            imo_match = re.search(r"imo-(\d{7})", href)
            mmsi_match = re.search(r"mmsi-(\d{9})", href)
            if imo_match:
                return imo_match.group(1), mmsi_match.group(1) if mmsi_match else None

        return None, None
    except Exception as e:
        print(f"  Warning: MyShipTracking search failed for {name}: {e}")
        return None, None


def _fetch_vesselfinder_specs(imo: str) -> dict | None:
    """Fetch vessel specs from VesselFinder detail page by IMO."""
    try:
        url = f"https://www.vesselfinder.com/vessels/details/{imo}"
        client = httpx.Client(headers=HEADERS, timeout=15, follow_redirects=True)
        resp = client.get(url)
        resp.raise_for_status()
        return _parse_vesselfinder_html(resp.text)
    except Exception as e:
        print(f"  Warning: VesselFinder fetch failed for IMO {imo}: {e}")
        return None


def _parse_vesselfinder_html(html: str) -> dict:
    """Parse VesselFinder vessel detail page HTML into specs dict."""
    soup = BeautifulSoup(html, "lxml")
    specs = {}

    # Map VesselFinder labels to our field names
    label_map = {
        "imo number": "imo",
        "mmsi": "mmsi",
        "callsign": "call_sign",
        "call sign": "call_sign",
        "ship type": "ship_type",
        "flag": "flag",
        "year of build": "year_built",
        "length overall": "length_m",
        "length overall (m)": "length_m",
        "beam": "beam_m",
        "beam (m)": "beam_m",
        "gross tonnage": "gt",
        "deadweight": "dwt",
        "deadweight (t)": "dwt",
        "service speed": "speed_knots",
        "service speed (kn)": "speed_knots",
        "builder": "builder",
    }

    # Parse table rows with tpc1/tpc2 or tpx1/tpx2 class pairs
    for td in soup.find_all("td", class_=["tpc1", "tpx1"]):
        label = td.get_text(strip=True).lower()
        value_td = td.find_next_sibling("td", class_=["tpc2", "tpx2"])
        if not value_td:
            continue
        value = value_td.get_text(strip=True)
        if not value or value == "-":
            continue

        field = label_map.get(label)
        if not field:
            # Try partial match
            for lbl, fld in label_map.items():
                if lbl in label:
                    field = fld
                    break

        if field:
            # Clean numeric fields
            if field in ("gt", "dwt", "length_m", "beam_m", "draft_m", "speed_knots"):
                # Remove units and commas: "100,341" -> 100341, "288.18 m" -> 288.18
                num_str = re.sub(r"[^\d.]", "", value.replace(",", ""))
                try:
                    specs[field] = float(num_str)
                except ValueError:
                    specs[field] = value
            else:
                specs[field] = value

    return specs


# ─── CLI for pre-populating cache ────────────────────────────

if __name__ == "__main__":
    from port_scraper import scrape_all_ports

    print("Scraping vessel movements...")
    df = scrape_all_ports(tankers_only=True)
    tanker_names = df["vessel"].unique().sort().to_list()
    tanker_names = [n for n in tanker_names if n]

    cache = VesselCache()
    cached = sum(1 for n in tanker_names if cache.get(n) is not None)
    uncached = [n for n in tanker_names if cache.get(n) is None]

    print(f"Found {len(tanker_names)} unique tankers, {cached} already cached, {len(uncached)} to look up")

    for i, name in enumerate(uncached):
        print(f"  [{i+1}/{len(uncached)}] Looking up: {name}")
        specs = lookup_vessel(name)
        if specs:
            cache.put(name, specs)
            imo = specs.get("imo", "?")
            gt = specs.get("gt", "?")
            flag = specs.get("flag", "?")
            print(f"    -> IMO={imo}, GT={gt}, Flag={flag}")
        else:
            cache.put(name, {})
            print(f"    -> not found")
        time.sleep(3)  # polite delay

    print(f"\nDone. Cache has {len(cache)} vessels saved to {CACHE_PATH}")
