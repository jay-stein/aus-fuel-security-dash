"""Vessel enrichment: look up tanker specs by name, cache locally."""

import json
import re
import time
from datetime import datetime, timezone
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

# Vessel type prefixes to strip before searching (MV SOMETHING → SOMETHING)
_PREFIX_RE = re.compile(
    r"^(?:M/?V\.?\s+|M/?T\.?\s+|S/?S\.?\s+|T/?S\.?\s+|F/?V\.?\s+|L/?B\.?\s+)",
    re.IGNORECASE,
)

# How long to honour a recorded failure before retrying (days)
_FAILURE_TTL_DAYS = 7


def _valid_imo(s: str | None) -> bool:
    """Return True if s is a plausible 7-digit IMO number."""
    return bool(s and re.fullmatch(r"\d{7}", s.strip()))


class VesselCache:
    """JSON-backed cache of vessel specifications.

    Failure entries are stored as {"failed_at": "<iso-timestamp>"} and
    expire after _FAILURE_TTL_DAYS so transient network failures are retried.
    Old-style empty {} entries are treated as cache misses (retried immediately).
    """

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
        """Return cached entry, or None if uncached / expired failure / old-style empty."""
        entry = self._cache.get(self._key(vessel_name))
        if entry is None:
            return None
        # Old-style permanent failure — treat as cache miss so it gets retried
        if not entry:
            return None
        # New-style failure with TTL
        if "failed_at" in entry and len(entry) == 1:
            try:
                failed_at = datetime.fromisoformat(entry["failed_at"])
                age_days = (datetime.now(timezone.utc) - failed_at).days
                return entry if age_days < _FAILURE_TTL_DAYS else None
            except Exception:
                return None
        return entry

    def is_known_failure(self, vessel_name: str) -> bool:
        """Return True if this vessel has a recent recorded failure (skip lookup)."""
        entry = self._cache.get(self._key(vessel_name))
        if not entry:
            return False
        if "failed_at" in entry and len(entry) == 1:
            try:
                failed_at = datetime.fromisoformat(entry["failed_at"])
                return (datetime.now(timezone.utc) - failed_at).days < _FAILURE_TTL_DAYS
            except Exception:
                return False
        return False

    def put(self, vessel_name: str, specs: dict):
        self._cache[self._key(vessel_name)] = specs
        self._save()

    def put_failure(self, vessel_name: str):
        """Record a timed failure. Will be retried after _FAILURE_TTL_DAYS days."""
        self._cache[self._key(vessel_name)] = {
            "failed_at": datetime.now(timezone.utc).isoformat()
        }
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

        # Find uncached vessels (excluding recent known failures)
        uncached = [n for n in tanker_names if n and self.get(n) is None and not self.is_known_failure(n)]

        if uncached:
            total = len(uncached)
            for i, name in enumerate(uncached):
                if progress_callback:
                    progress_callback(i, total, name)
                specs = lookup_vessel(name)
                if specs:
                    self.put(name, specs)
                else:
                    self.put_failure(name)

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


def _clean_vessel_name(name: str) -> list[str]:
    """Return a list of progressively cleaned search queries for a vessel name.

    Handles:
    - Vessel type prefixes (MV, MT, SS, M/V etc.)
    - Apostrophes, curly apostrophes: AL FAT'H → AL FATH
    - Dots: K. ACACIA → K ACACIA
    - Hyphens: NORD-AM → NORD AM
    - "NO." abbreviation: TAITAR NO. 4 → TAITAR 4
    - Trailing pure numbers: GRAND WINNER 6 → GRAND WINNER
    - Last-word fallback (only for words >= 5 chars, not purely numeric)
    """
    name = name.strip()
    candidates = [name]

    # Strip vessel type prefix
    stripped = _PREFIX_RE.sub("", name).strip()
    if stripped != name:
        candidates.append(stripped)
    base = stripped if stripped != name else name

    # Apostrophe and curly-apostrophe variants
    if re.search(r"['\u2019]", base):
        # Replace with space
        apos_space = re.sub(r"['\u2019]", " ", base)
        apos_space = re.sub(r"\s{2,}", " ", apos_space).strip()
        candidates.append(apos_space)
        # Remove entirely
        apos_none = re.sub(r"['\u2019]", "", base).strip()
        candidates.append(apos_none)

    # Dot removal (K. ACACIA → K ACACIA; also covers NO.)
    if "." in base:
        no_dots = re.sub(r"\.", "", base)
        no_dots = re.sub(r"\s{2,}", " ", no_dots).strip()
        candidates.append(no_dots)

    # Hyphen variants (NORD-AM → NORD AM, NORDAM)
    if "-" in base:
        hyph_space = base.replace("-", " ").strip()
        candidates.append(re.sub(r"\s{2,}", " ", hyph_space))
        candidates.append(base.replace("-", "").strip())

    # "NO." abbreviation: "TAITAR NO. 4" → "TAITAR 4"
    no_abbrev = re.sub(r"\bNO\.\s*", "", base, flags=re.IGNORECASE).strip()
    no_abbrev = re.sub(r"\s{2,}", " ", no_abbrev)
    if no_abbrev != base:
        candidates.append(no_abbrev)

    # Trailing pure number: "GRAND WINNER 6" → "GRAND WINNER"
    base_clean = re.sub(r"['\u2019.`]", "", base)
    no_trailing_num = re.sub(r"\s+\d+$", "", base_clean.strip())
    if no_trailing_num != base_clean.strip() and len(no_trailing_num) >= 4:
        candidates.append(no_trailing_num.strip())

    # Last-word fallback — only for words >= 5 chars and not purely numeric
    words = re.sub(r"['\u2019.\-]", " ", base).split()
    if len(words) > 1:
        last = words[-1]
        if len(last) >= 5 and not last.isdigit():
            candidates.append(last)

    return list(dict.fromkeys(c for c in candidates if c))


def lookup_vessel(name: str) -> dict | None:
    """Look up vessel specs by name. Returns dict of specs or None.

    Search strategy (in order):
    1. MyShipTracking — tries original name + cleaned variants
    2. VesselFinder search — fallback when MST doesn't index the vessel
    Once an IMO is found (validated as 7-digit), fetches full specs from
    VesselFinder detail page.
    """
    try:
        imo, mmsi = None, None

        # Step 1: MyShipTracking with progressive name cleaning
        for query in _clean_vessel_name(name):
            imo, mmsi = _search_myshiptracking(query)
            if imo:
                break
            time.sleep(0.5)

        # Step 2: VesselFinder search fallback
        if not imo:
            for query in _clean_vessel_name(name):
                imo = _search_vesselfinder(query)
                if imo:
                    break
                time.sleep(0.5)

        if not imo:
            return None

        time.sleep(2)

        # Step 3: fetch full specs from VesselFinder detail page
        specs = _fetch_vesselfinder_specs(imo)
        if specs:
            specs.setdefault("imo", imo)
            specs.setdefault("mmsi", mmsi or "")
            return specs

        # Fallback: return just IMO/MMSI if spec page failed
        return {"imo": imo, "mmsi": mmsi or ""}

    except Exception as e:
        print(f"  Warning: lookup failed for {name}: {e}")
        return None


def _search_myshiptracking(name: str) -> tuple[str | None, str | None]:
    """Search MyShipTracking for a vessel by name. Returns (imo, mmsi) or (None, None)."""
    from urllib.parse import quote_plus
    try:
        url = f"https://www.myshiptracking.com/vessels?name={quote_plus(name)}"
        client = httpx.Client(headers=HEADERS, timeout=15, follow_redirects=True)
        resp = client.get(url)
        resp.raise_for_status()

        for a in BeautifulSoup(resp.text, "lxml").find_all("a", href=True):
            href = a["href"]
            imo_match = re.search(r"imo-(\d{7})", href)
            mmsi_match = re.search(r"mmsi-(\d{9})", href)
            if imo_match:
                imo = imo_match.group(1)
                if _valid_imo(imo):
                    return imo, mmsi_match.group(1) if mmsi_match else None

        return None, None
    except Exception as e:
        print(f"  Warning: MyShipTracking search failed for {name!r}: {e}")
        return None, None


def _search_vesselfinder(name: str) -> str | None:
    """Search VesselFinder for a vessel by name. Returns IMO or None.

    Fallback for vessels not indexed by MyShipTracking.
    """
    from urllib.parse import quote_plus
    try:
        url = f"https://www.vesselfinder.com/vessels?name={quote_plus(name)}"
        resp = httpx.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
        resp.raise_for_status()
        # VesselFinder search results link to /vessels/details/{IMO} (7-digit)
        for a in BeautifulSoup(resp.text, "lxml").find_all("a", href=True):
            m = re.search(r"/vessels/details/(\d{7})$", a["href"])
            if m:
                imo = m.group(1)
                if _valid_imo(imo):
                    return imo
        return None
    except Exception as e:
        print(f"  Warning: VesselFinder search failed for {name!r}: {e}")
        return None


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
    uncached = [n for n in tanker_names if cache.get(n) is None and not cache.is_known_failure(n)]

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
            cache.put_failure(name)
            print(f"    -> not found (will retry in {_FAILURE_TTL_DAYS} days)")
        time.sleep(3)  # polite delay

    print(f"\nDone. Cache has {len(cache)} vessels saved to {CACHE_PATH}")
