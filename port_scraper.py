"""Scrape vessel movements from Australian port authority websites."""

import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

import json
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
import polars as pl


TANKER_KEYWORDS = [
    "tanker",
    "crude",
    "products tanker",
    "lng",
    "lpg",
    "gas carrier",
    "chemical",
]

FUEL_CARGO_KEYWORDS = [
    "petroleum",
    "crude",
    "diesel",
    "gasoline",
    "petrol",
    "jet fuel",
    "liquid bulk",
    "fuel oil",
    "bitumen",
    "lng",
    "lpg",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Approximate cargo capacity in litres by vessel length range (very rough)
# Based on typical product tanker deadweight-to-volume ratios
# ~1.15 m³ per tonne for refined products, ~1.165 m³ per tonne for crude
VESSEL_CAPACITY_ESTIMATES = {
    # (min_length, max_length): (litres_low, litres_high, confidence)
    (0, 99): (1_000_000, 5_000_000, "Very Rough"),       # small coastal tanker
    (100, 149): (5_000_000, 25_000_000, "Rough"),         # Handysize
    (150, 189): (25_000_000, 55_000_000, "Rough"),        # MR tanker
    (190, 229): (55_000_000, 80_000_000, "Rough"),        # LR1
    (230, 274): (80_000_000, 160_000_000, "Rough"),       # LR2 / Aframax
    (275, 350): (160_000_000, 320_000_000, "Rough"),      # Suezmax / VLCC
}

# Australian port cities/regions — used to detect domestic movements
AU_LOCATIONS = {
    # WA
    "fremantle", "kwinana", "perth", "bunbury", "geraldton", "dampier",
    "port hedland", "port walcott", "barrow island", "varanus island",
    "broome", "albany", "esperance", "exmouth", "onslow", "carnarvon",
    "busselton", "christmas island", "wa ports",
    # NT
    "darwin", "gove",
    # SA
    "adelaide", "port adelaide", "whyalla", "port lincoln",
    "sa ports",
    # VIC
    "geelong", "melbourne", "portland", "hastings",
    # NSW
    "sydney", "botany", "port botany", "kembla", "port kembla", "newcastle",
    "eden",
    # QLD
    "brisbane", "gladstone", "townsville", "cairns", "mackay", "hay point",
    "bundaberg", "weipa", "abbot point", "port alma", "airlie beach",
    "amrun", "aurukun", "green island", "horn island", "boigu island",
    "kubin island", "murray island", "seisia", "skardon river",
    "warraber island", "willis island", "yorke island", "yorkeys knob",
    # TAS
    "devonport", "hobart", "bell bay", "launceston",
    # Generic
    "outside port limits",
}

AU_STATE_MAP = {
    # WA
    "fremantle": "WA", "kwinana": "WA", "perth": "WA", "bunbury": "WA",
    "geraldton": "WA", "dampier": "WA", "port hedland": "WA",
    "port walcott": "WA", "barrow island": "WA", "varanus island": "WA",
    "broome": "WA", "albany": "WA", "esperance": "WA", "exmouth": "WA",
    "onslow": "WA", "carnarvon": "WA", "busselton": "WA",
    "christmas island": "WA", "wa ports": "WA",
    # NT
    "darwin": "NT", "gove": "NT",
    # SA
    "adelaide": "SA", "port adelaide": "SA", "whyalla": "SA",
    "port lincoln": "SA", "sa ports": "SA",
    # VIC
    "geelong": "VIC", "melbourne": "VIC", "portland": "VIC", "hastings": "VIC",
    # NSW
    "sydney": "NSW", "botany": "NSW", "port botany": "NSW",
    "kembla": "NSW", "port kembla": "NSW", "newcastle": "NSW", "eden": "NSW",
    # QLD
    "brisbane": "QLD", "gladstone": "QLD", "townsville": "QLD",
    "cairns": "QLD", "mackay": "QLD", "hay point": "QLD",
    "bundaberg": "QLD", "weipa": "QLD", "abbot point": "QLD",
    "port alma": "QLD", "airlie beach": "QLD", "amrun": "QLD",
    "aurukun": "QLD", "green island": "QLD", "horn island": "QLD",
    "boigu island": "QLD", "kubin island": "QLD", "murray island": "QLD",
    "seisia": "QLD", "skardon river": "QLD", "warraber island": "QLD",
    "willis island": "QLD", "yorke island": "QLD", "yorkeys knob": "QLD",
    # TAS
    "devonport": "TAS", "hobart": "TAS", "bell bay": "TAS", "launceston": "TAS",
}

# Berth / wharf names within NSW ports — these are internal locations, not origins
_BERTH_KEYWORDS = [
    "dock", "berth", "wharf", "terminal", "kooragang", "mayfield",
    "dyke", "basin", "gore cove", "glebe island", "kurnell",
    "anchorage", "cruise terminal", "passenger terminal", "eglo",
]


# Comprehensive map of international port/city names to countries.
# Includes all ports observed in Australian vessel movement data.
PORT_COUNTRY_MAP = {
    # ── South Korea ─────────────────────────────────────────────
    "busan": "South Korea", "ulsan": "South Korea", "yeosu": "South Korea",
    "incheon": "South Korea", "daesan": "South Korea", "onsan": "South Korea",
    "gwangyang": "South Korea", "kwangyang": "South Korea",
    "pyeongtaek": "South Korea", "boryeong": "South Korea",
    "tongyeong": "South Korea", "tongyoung": "South Korea",
    "dangjin": "South Korea", "pohang": "South Korea",
    "gunsan": "South Korea", "kunsan": "South Korea",
    "samcheonpo": "South Korea", "taean": "South Korea",
    "korea": "South Korea",
    # ── Japan ───────────────────────────────────────────────────
    "tokyo": "Japan", "yokohama": "Japan", "kobe": "Japan", "chiba": "Japan",
    "nagoya": "Japan", "osaka": "Japan", "kawasaki": "Japan", "mizushima": "Japan",
    "sakai": "Japan", "kiire": "Japan", "niigata": "Japan", "niiigata": "Japan",
    "fukuyama": "Japan", "hiroshima": "Japan", "higashiharima": "Japan",
    "higashihirima": "Japan", "hitachinaka": "Japan", "kanda": "Japan",
    "kudamatsu": "Japan", "matsuura": "Japan", "kagoshima": "Japan",
    "mishima": "Japan", "kawanoe": "Japan", "nadahama": "Japan",
    "nanao": "Japan", "onahama": "Japan", "reihoku": "Japan",
    "sendai": "Japan", "shiogama": "Japan", "sendaishiogama": "Japan",
    "susaki": "Japan", "takehara": "Japan", "taketoyo": "Japan",
    "tobata": "Japan", "kitakyushu": "Japan", "tokuyama": "Japan",
    "tokuyamakudamatsu": "Japan", "tomakomai": "Japan", "tonda": "Japan",
    "tsukumi": "Japan", "ube": "Japan",
    # ── China ───────────────────────────────────────────────────
    "shanghai": "China", "ningbo": "China", "qingdao": "China", "dalian": "China",
    "tianjin": "China", "zhanjiang": "China", "zhoushan": "China", "maoming": "China",
    "quanzhou": "China", "huizhou": "China", "rizhao": "China",
    "zhuhai": "China", "lianyungang": "China", "caofeidan": "China",
    "changzhou": "China", "dongjiakou": "China", "dongjiangkou": "China",
    "fangcheng": "China", "fuzhou": "China", "heshan": "China",
    "jiangmen": "China", "jiangyin": "China", "jingtang": "China",
    "tangshan": "China", "jinzhou": "China", "lanshan": "China",
    "longkou": "China", "machong": "China", "nantong": "China",
    "penglai": "China", "qinzhou": "China", "rugao": "China",
    "shekou": "China", "taicang": "China", "taizhou": "China",
    "yangjiang": "China", "yantai": "China", "yantian": "China",
    "zhenjiang": "China", "china": "China",
    # ── Taiwan ──────────────────────────────────────────────────
    "kaohsiung": "Taiwan", "mailiao": "Taiwan", "mai-liao": "Taiwan",
    "taichung": "Taiwan",
    # ── Singapore ───────────────────────────────────────────────
    "singapore": "Singapore", "jurong": "Singapore", "pulau bukom": "Singapore",
    # ── Malaysia ────────────────────────────────────────────────
    "tanjung pelepas": "Malaysia", "port klang": "Malaysia", "pengerang": "Malaysia",
    "tanjung langsat": "Malaysia", "bintulu": "Malaysia", "labuan": "Malaysia",
    "kemaman": "Malaysia", "melaka": "Malaysia", "pasir gudang": "Malaysia",
    "sungai": "Malaysia", "linggi": "Malaysia", "malaysia": "Malaysia",
    # ── Thailand ────────────────────────────────────────────────
    "bangkok": "Thailand", "si racha": "Thailand", "sriracha": "Thailand",
    "map ta phut": "Thailand", "laem chabang": "Thailand",
    "koh sichang": "Thailand",
    # ── Vietnam ─────────────────────────────────────────────────
    "ho chi minh": "Vietnam", "vung tau": "Vietnam", "dung quat": "Vietnam",
    "phu my": "Vietnam", "phu-my": "Vietnam", "hongai": "Vietnam",
    # ── Indonesia ───────────────────────────────────────────────
    "batam": "Indonesia", "balikpapan": "Indonesia", "cilacap": "Indonesia",
    "dumai": "Indonesia", "merak": "Indonesia", "tuban": "Indonesia",
    "bahudopi": "Indonesia", "jakarta": "Indonesia", "surabaya": "Indonesia",
    "sekupang": "Indonesia", "bontang": "Indonesia", "obi island": "Indonesia",
    "amamapare": "Indonesia", "adang bay": "Indonesia",
    "indonesia": "Indonesia",
    # ── Philippines ─────────────────────────────────────────────
    "manila": "Philippines", "batangas": "Philippines", "bauan": "Philippines",
    "subic": "Philippines", "general santos": "Philippines",
    "philippines": "Philippines",
    # ── South Asia ──────────────────────────────────────────────
    "mumbai": "India", "jamnagar": "India", "kochi": "India", "cochin": "India",
    "chennai": "India", "mangalore": "India", "paradip": "India",
    "haldia": "India", "vizag": "India", "visakhapatnam": "India",
    "kandla": "India", "sikka": "India", "chittagong": "Bangladesh",
    "colombo": "Sri Lanka", "india": "India",
    # ── Middle East / Gulf ──────────────────────────────────────
    "jebel ali": "UAE", "fujairah": "UAE", "ruwais": "UAE", "das island": "UAE",
    "abu dhabi": "UAE", "dubai": "UAE", "khor fakkan": "UAE",
    "ras tanura": "Saudi Arabia", "yanbu": "Saudi Arabia", "jubail": "Saudi Arabia",
    "jeddah": "Saudi Arabia", "rabigh": "Saudi Arabia",
    "al ahmadi": "Kuwait", "mina al ahmadi": "Kuwait", "shuaiba": "Kuwait",
    "mina saud": "Kuwait",
    "basra": "Iraq", "al basrah": "Iraq", "khor al amaya": "Iraq",
    "kharg island": "Iran", "bandar abbas": "Iran", "assaluyeh": "Iran",
    "sohar": "Oman", "mina al fahal": "Oman", "muscat": "Oman",
    "sitra": "Bahrain", "ras laffan": "Qatar", "mesaieed": "Qatar",
    # ── Africa ──────────────────────────────────────────────────
    "durban": "South Africa", "richards bay": "South Africa",
    "cape town": "South Africa", "port elizabeth": "South Africa",
    "mombasa": "Kenya", "dar es salaam": "Tanzania",
    "luanda": "Angola", "bonny": "Nigeria", "lagos": "Nigeria",
    "port louis": "Mauritius",
    # ── Europe ──────────────────────────────────────────────────
    "rotterdam": "Netherlands", "antwerp": "Belgium", "hamburg": "Germany",
    "le havre": "France", "marseille": "France",
    "milford haven": "UK", "southampton": "UK", "fawley": "UK",
    # ── Americas ────────────────────────────────────────────────
    "houston": "USA", "corpus christi": "USA", "beaumont": "USA",
    "long beach": "USA", "los angeles": "USA",
    "punto fijo": "Venezuela", "jose": "Venezuela",
    "rodman": "Panama",
    # ── Pacific ─────────────────────────────────────────────────
    "noumea": "New Caledonia", "suva": "Fiji", "lautoka": "Fiji",
    "apia": "Samoa", "nuku'alofa": "Tonga",
    "port moresby": "Papua New Guinea", "lae": "Papua New Guinea",
    "motukea": "Papua New Guinea", "lihir island": "Papua New Guinea",
    "papua new guinea": "Papua New Guinea",
    "honiara": "Solomon Islands", "guadalcanal": "Solomon Islands",
    "mystery island": "Vanuatu",
    # ── New Zealand ─────────────────────────────────────────────
    "auckland": "New Zealand", "wellington": "New Zealand",
    "tauranga": "New Zealand", "marsden point": "New Zealand",
    "dunedin": "New Zealand",
    # ── East Timor ──────────────────────────────────────────────
    "dili": "East Timor",
    # ── Hong Kong ───────────────────────────────────────────────
    "hong kong": "Hong Kong",
    # ── Antarctica ──────────────────────────────────────────────
    "antarctica": "Antarctica",
    # ── Bali (Indonesia) ────────────────────────────────────────
    "bali": "Indonesia", "benoa": "Indonesia",
}

# Country centroid coordinates for map plotting (lat, lon)
COUNTRY_COORDS: dict[str, tuple[float, float]] = {
    "South Korea": (36.5, 127.8),
    "Japan": (36.2, 138.3),
    "China": (35.9, 104.2),
    "Taiwan": (23.7, 121.0),
    "Singapore": (1.35, 103.82),
    "Malaysia": (4.2, 101.98),
    "Thailand": (15.87, 100.99),
    "Vietnam": (14.06, 108.28),
    "Indonesia": (-0.79, 113.92),
    "Philippines": (12.88, 121.77),
    "India": (20.59, 78.96),
    "Bangladesh": (23.68, 90.36),
    "Sri Lanka": (7.87, 80.77),
    "UAE": (23.42, 53.85),
    "Saudi Arabia": (23.89, 45.08),
    "Kuwait": (29.31, 47.48),
    "Iraq": (33.22, 43.68),
    "Iran": (32.43, 53.69),
    "Oman": (21.47, 55.98),
    "Bahrain": (26.07, 50.55),
    "Qatar": (25.35, 51.18),
    "South Africa": (-30.56, 22.94),
    "Kenya": (-0.02, 37.91),
    "Tanzania": (-6.37, 34.89),
    "Angola": (-11.20, 17.87),
    "Nigeria": (9.08, 8.68),
    "Mauritius": (-20.35, 57.55),
    "Netherlands": (52.13, 5.29),
    "Belgium": (50.50, 4.47),
    "Germany": (51.17, 10.45),
    "France": (46.23, 2.21),
    "UK": (55.38, -3.44),
    "USA": (37.09, -95.71),
    "Venezuela": (6.42, -66.59),
    "Panama": (8.54, -80.78),
    "New Caledonia": (-20.90, 165.62),
    "Fiji": (-17.71, 178.07),
    "Samoa": (-13.76, -172.10),
    "Tonga": (-21.18, -175.20),
    "Papua New Guinea": (-6.31, 147.18),
    "Solomon Islands": (-9.43, 160.03),
    "Vanuatu": (-15.38, 166.96),
    "New Zealand": (-40.90, 174.89),
    "East Timor": (-8.87, 125.73),
    "Hong Kong": (22.40, 114.11),
    "Antarctica": (-75.25, 0.07),
    "Australia": (-25.27, 133.78),
}

# Australian port/city coordinates for domestic bubble map
AU_PORT_COORDS: dict[str, tuple[float, float]] = {
    # WA
    "fremantle": (-32.06, 115.75), "kwinana": (-32.23, 115.77),
    "perth": (-31.95, 115.86), "bunbury": (-33.33, 115.63),
    "geraldton": (-28.77, 114.62), "dampier": (-20.66, 116.71),
    "port hedland": (-20.31, 118.58), "port walcott": (-20.59, 117.19),
    "barrow island": (-20.79, 115.40), "varanus island": (-20.66, 115.57),
    "broome": (-17.96, 122.24), "albany": (-35.02, 117.88),
    "esperance": (-33.86, 121.89), "exmouth": (-21.93, 114.13),
    "onslow": (-21.64, 115.11), "carnarvon": (-24.88, 113.66),
    "busselton": (-33.65, 115.35),
    # NT
    "darwin": (-12.46, 130.84), "gove": (-12.27, 136.82),
    # SA
    "adelaide": (-34.93, 138.60), "port adelaide": (-34.84, 138.51),
    "whyalla": (-33.03, 137.52), "port lincoln": (-34.73, 135.86),
    "port pirie": (-33.19, 138.02), "port bonython": (-32.98, 137.77),
    "wallaroo": (-33.93, 137.63), "ardrossan": (-34.43, 137.92),
    "klein point": (-34.93, 137.78), "thevenard": (-32.15, 133.64),
    "port giles": (-34.77, 137.77),
    # VIC
    "geelong": (-38.15, 144.36), "melbourne": (-37.81, 144.96),
    "portland": (-38.34, 141.60), "hastings": (-38.30, 145.20),
    # NSW
    "sydney": (-33.87, 151.21), "botany": (-33.95, 151.20),
    "port botany": (-33.97, 151.23), "kembla": (-34.47, 150.90),
    "port kembla": (-34.47, 150.90), "newcastle": (-32.93, 151.78),
    "eden": (-37.07, 149.90),
    # QLD
    "brisbane": (-27.47, 153.03), "gladstone": (-23.85, 151.27),
    "townsville": (-19.25, 146.77), "cairns": (-16.92, 145.77),
    "mackay": (-21.14, 149.19), "hay point": (-21.28, 149.30),
    "bundaberg": (-24.87, 152.35), "weipa": (-12.63, 141.87),
    "abbot point": (-19.87, 148.08), "port alma": (-23.58, 150.86),
    "airlie beach": (-20.27, 148.72),
    # TAS
    "devonport": (-41.18, 146.36), "hobart": (-42.88, 147.33),
    "bell bay": (-41.12, 146.85), "launceston": (-41.43, 147.14),
    "burnie": (-41.05, 145.87), "stanley": (-40.76, 145.29),
    "grassy": (-39.95, 144.07),
}


def _parse_wcf_date(val: str) -> str:
    """Parse WCF JSON date like /Date(1775310300000+1000)/ to readable string."""
    m = re.search(r"/Date\((\d+)([+-]\d{4})?\)/", val)
    if m:
        epoch_ms = int(m.group(1))
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        if m.group(2):
            sign = 1 if m.group(2)[0] == "+" else -1
            offset_h = int(m.group(2)[1:3])
            offset_m = int(m.group(2)[3:5])
            dt = dt.astimezone(timezone(timedelta(hours=sign * offset_h, minutes=sign * offset_m)))
        return dt.strftime("%Y-%m-%d %H:%M")
    return val


_DATE_FORMATS = [
    "%Y-%m-%d %H:%M",       # already correct
    "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y %H:%M",       # AU format dd/mm/yyyy
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %I:%M %p",    # AU with AM/PM
    "%d/%m/%Y %I:%M:%S %p",
    "%m/%d/%Y %I:%M:%S %p", # US format (Geelong PowerApps) 3/25/2026 10:00:00 PM
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%d %b %Y %H:%M",       # 28 Mar 2026 10:00
    "%d %B %Y %H:%M",       # 28 March 2026 10:00
    "%d-%b-%Y %H:%M",
    "%d %b %H:%M",           # NSW: "30 Mar 06:00" (after stripping dow + inserting space)
    "%d %b%H:%M",            # fallback without space
    "%Y-%m-%dT%H:%M:%S",    # ISO
    "%Y-%m-%dT%H:%M",
    "%d/%m/%Y",              # date-only (TasPorts)
]

# Day-of-week prefixes NSW uses, e.g. "Mon 30 Mar06:00"
_DOW_RE = re.compile(
    r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+", re.IGNORECASE
)


def _normalise_date(val: str) -> str:
    """Parse any date string and return yyyy-mm-dd HH:MM format."""
    if not val or val.strip() == "":
        return ""
    val = _parse_wcf_date(val)  # handle WCF /Date(...)/ first
    val = val.strip()

    # Strip day-of-week prefix: "Mon 30 Mar06:00" -> "30 Mar06:00"
    val = _DOW_RE.sub("", val).strip()

    # NSW dates sometimes lack space before time: "30 Mar06:00"
    # Insert space before HH:MM if missing: look for letter immediately followed by digit
    val = re.sub(r"([A-Za-z])(\d{1,2}:\d{2})", r"\1 \2", val)

    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(val, fmt)
            # If year is 1900 (no year in format), assume current year
            if dt.year == 1900:
                now = datetime.now()
                dt = dt.replace(year=now.year)
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            continue
    # If nothing matched, return as-is
    return val


def _lookup_country(port_name: str) -> str:
    """Look up country for a port name. Returns country or the original name."""
    if not port_name:
        return ""
    lower = port_name.lower().strip()
    for key, country in PORT_COUNTRY_MAP.items():
        if key in lower:
            return country
    return port_name


def _load_vessel_cache() -> dict:
    """Load vessel cache JSON for tanker detection (lazy, loaded once)."""
    cache_path = Path("data/vessel_cache.json")
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


_VESSEL_CACHE: dict | None = None


def _is_tanker(vessel_type: str, cargo_type: str = "", vessel_name: str = "") -> bool:
    combined = (vessel_type + " " + cargo_type).lower()
    if any(kw in combined for kw in TANKER_KEYWORDS + FUEL_CARGO_KEYWORDS):
        return True
    # For ports that don't provide vessel type (Darwin, SA), check vessel name patterns
    if not vessel_type.strip() and vessel_name:
        name_lower = vessel_name.lower()
        name_hints = ["gas ", "gaslog", "lng ", " lng", "gaschem", " maru",
                       "taitar", "tortuga tide"]
        if any(hint in name_lower for hint in name_hints):
            return True
    # Check vessel cache for ship_type (catches tankers like KRISTINITA at ports
    # that don't expose vessel classification in their API)
    if vessel_name:
        global _VESSEL_CACHE
        if _VESSEL_CACHE is None:
            _VESSEL_CACHE = _load_vessel_cache()
        cached = _VESSEL_CACHE.get(vessel_name.strip().upper(), {})
        cached_type = cached.get("ship_type", "").lower()
        if any(kw in cached_type for kw in TANKER_KEYWORDS):
            return True
    return False


def estimate_fuel_volume(length_m: float | None, tonnage: float | None) -> tuple[str, str]:
    """Estimate fuel volume in litres. Returns (estimate_str, confidence)."""
    # If we have actual tonnage (from Geelong), use it directly
    if tonnage and tonnage > 0:
        # ~1.15 m³/tonne for refined products = 1150 L/tonne
        litres = tonnage * 1150
        return f"{litres / 1_000_000:.1f} ML", "High (from tonnage)"

    # Otherwise estimate from vessel length
    if length_m and length_m > 0:
        for (lo, hi), (l_low, l_high, conf) in VESSEL_CAPACITY_ESTIMATES.items():
            if lo <= length_m <= hi:
                mid = (l_low + l_high) / 2
                return f"~{mid / 1_000_000:.0f} ML", conf
        return "Unknown", "Unknown"

    return "Unknown", "Unknown"


# Generic country labels that appear in port data (e.g. "China, People's Republic Of - Unknown")
_COUNTRY_ALIASES = {
    "china": "China", "people's republic": "China",
    "japan": "Japan", "korea, south": "South Korea", "korea south": "South Korea",
    "malaysia": "Malaysia", "indonesia": "Indonesia", "philippines": "Philippines",
    "india": "India", "singapore": "Singapore", "taiwan": "Taiwan",
    "vietnam": "Vietnam", "thailand": "Thailand", "bangladesh": "Bangladesh",
    "papua new guinea": "Papua New Guinea", "new zealand": "New Zealand",
}


def classify_origin(origin: str) -> tuple[str, str]:
    """Classify origin as domestic or international.

    Returns (origin_type, origin_detail) where:
      origin_type is "Domestic" or "International"
      origin_detail is the AU state or country name
    """
    if not origin:
        return "Unknown", ""

    origin_lower = origin.lower().strip()

    # Skip berth/wharf names — these are internal port locations, not origins
    if any(kw in origin_lower for kw in _BERTH_KEYWORDS):
        return "Unknown", ""

    # Skip placeholders
    if origin_lower in {"unknown", "unknown port", "to be advised", "overseas", ""}:
        return "Unknown", ""

    # Check Australian locations
    for loc in AU_LOCATIONS:
        if loc in origin_lower:
            state = AU_STATE_MAP.get(loc, "AU")
            return "Domestic", state

    # If it contains "australia" or "re-import" or "sa ports"
    if "australia" in origin_lower or "re-import" in origin_lower:
        return "Domestic", "AU"

    # Check for generic country labels like "China, People's Republic Of - Unknown"
    for alias, country in _COUNTRY_ALIASES.items():
        if alias in origin_lower:
            return "International", country

    return "International", _lookup_country(origin)


# ─── NSW Port Authority ───────────────────────────────────────


@dataclass
class PortConfig:
    name: str
    url: str
    state: str


NSW_PORTS = [
    PortConfig("Sydney Harbour", "https://www.portauthoritynsw.com.au/port-operations/sydney-harbour/sydney-harbour-daily-vessel-movements", "NSW"),
    PortConfig("Port Botany", "https://www.portauthoritynsw.com.au/port-operations/port-botany/port-botany-daily-vessel-movements", "NSW"),
    PortConfig("Port Kembla", "https://www.portauthoritynsw.com.au/port-operations/port-kembla/port-kembla-daily-vessel-movements", "NSW"),
    PortConfig("Newcastle Harbour", "https://www.portauthoritynsw.com.au/port-operations/newcastle-harbour/newcastle-harbour-daily-vessel-movements", "NSW"),
]


def _parse_nsw_table(html: str, port: PortConfig) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    table = soup.find("table")
    if not table:
        view = soup.find("div", class_=re.compile(r"view-vessel-movement"))
        if view:
            table = view.find("table")
    if not table:
        return rows

    headers = []
    thead = table.find("thead")
    if thead:
        for th in thead.find_all("th"):
            headers.append(th.get_text(strip=True))

    tbody = table.find("tbody")
    if not tbody:
        return rows

    for tr in tbody.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 4:
            continue
        row = {}
        for i, val in enumerate(cells):
            if i < len(headers):
                row[headers[i]] = val
            else:
                row[f"col_{i}"] = val
        row["port"] = port.name
        row["state"] = port.state
        rows.append(row)
    return rows


def scrape_nsw_ports() -> list[dict]:
    all_rows = []
    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        for port in NSW_PORTS:
            try:
                resp = client.get(port.url)
                resp.raise_for_status()
                raw = _parse_nsw_table(resp.text, port)
                all_rows.extend(raw)
            except Exception as e:
                print(f"Warning: Failed to scrape {port.name}: {e}")
    return all_rows


# ─── Geelong (VIC) ────────────────────────────────────────────


def _parse_geelong_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="shipping")
    if not table:
        return []

    rows = []
    current_berth = ""
    for tr in table.find_all("tr"):
        if "berthGroup" in tr.get("class", []):
            td = tr.find("td")
            if td:
                current_berth = td.get_text(strip=True)
            continue

        cells = tr.find_all("td")
        if len(cells) < 8:
            continue

        vessel = cells[0].get_text(strip=True)
        if not vessel:
            continue

        eta = tr.get("data-eta", cells[1].get_text(strip=True))
        cargo_type = cells[7].get_text(strip=True) if len(cells) > 7 else ""
        tonnage_str = cells[8].get_text(strip=True) if len(cells) > 8 else ""
        length_str = cells[9].get_text(strip=True) if len(cells) > 9 else ""
        agent = cells[11].get_text(strip=True) if len(cells) > 11 else ""
        customer = cells[12].get_text(strip=True) if len(cells) > 12 else ""

        tonnage = float(tonnage_str) if tonnage_str.replace(".", "").isdigit() else None
        length_m = float(length_str) if length_str.replace(".", "").isdigit() else None

        rows.append({
            "port": "Geelong",
            "state": "VIC",
            "Vessel": vessel,
            "Date & Time": eta,
            "ARR / DEP": "Arrival",
            "Vessel type": cargo_type,
            "Agent": agent,
            "From": "",
            "To": current_berth,
            "In port": "",
            "cargo_type": cargo_type,
            "tonnage": tonnage,
            "length_m": length_m,
            "customer": customer,
        })
    return rows


def scrape_geelong() -> list[dict]:
    url = "https://geelongport.powerappsportals.com/Shipping/"
    try:
        with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return _parse_geelong_table(resp.text)
    except Exception as e:
        print(f"Warning: Failed to scrape Geelong: {e}")
        return []


# ─── Ports Victoria (Melbourne + Geelong) ─────────────────────

# Vessel type codes used in Ports Victoria ship name suffix, e.g. "STI Winnie (T)"
_PV_TYPE_CODES = {
    "T": "Tanker",
    "LP": "LPG Tanker",
    "LG": "LNG Tanker",
    "CT": "Chemical Tanker",
    "PP": "Car Carrier",
    "P": "Passenger",
    "BM": "Bulk/Multi",
    "SC": "Container",
}

# Berths that confirm a vessel is a tanker/chemical carrier, even without a (T) suffix.
# Only include dedicated petroleum/chemical berths — NOT general bulk berths like Lascelles.
_PV_FUEL_BERTHS = {
    "gellibrand",       # ExxonMobil/Mobil fuel import terminal (Melbourne)
    "holden dock",      # Oil terminal (Melbourne)
    "maribyrnong",      # Chemical/product tanker berth (Melbourne)
    "victoria dock",    # LPG tankers (Melbourne)
    "geelong refinery", # Viva Energy crude/products (Geelong)
    "refinery pier",    # Viva Energy (Geelong PowerApps uses this name)
    "esso",
    "bp berth",
}


def _pv_port_from_berth(berth: str) -> str:
    """Map a Ports Victoria berth name to a port label."""
    b = berth.lower()
    if any(k in b for k in ("geelong refinery", "corio", "lascelles")):
        return "Geelong"
    return "Melbourne"


def _parse_ports_victoria(html: str) -> list[dict]:
    """Parse the 5 ship-movement tables from ports.vic.gov.au."""
    soup = BeautifulSoup(html, "lxml")
    rows = []

    _HEADING_MOVEMENT = {
        "expected arrivals": "Arrival",
        "actual arrivals": "Arrival",
        "expected departures": "Departure",
        "actual departures": "Departure",
        "in port": "In Port",
    }

    current_movement = "Arrival"

    for element in soup.find_all(["h2", "h3", "h4", "table"]):
        if element.name in ("h2", "h3", "h4"):
            heading = element.get_text(strip=True).lower()
            for key, mv in _HEADING_MOVEMENT.items():
                if key in heading:
                    current_movement = mv
                    break
            continue

        # --- table ---
        thead = element.find("thead")
        tbody = element.find("tbody")
        if not thead or not tbody:
            continue
        headers = [th.get_text(strip=True) for th in thead.find_all("th")]
        if not headers:
            continue

        # Detect In Port table by its unique column structure (Berth + Arrived + ETD)
        # It has no preceding heading on the Ports Victoria page
        header_set = {h.lower() for h in headers}
        if "berth" in header_set and "arrived" in header_set:
            current_movement = "In Port"

        is_inport = current_movement == "In Port"

        for tr in tbody.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not cells or not cells[0]:
                continue

            cell_map = {headers[i]: cells[i] for i in range(min(len(headers), len(cells)))}

            # Clean vessel name and extract type code, e.g. "STI Winnie (T)" → name="STI Winnie", code="T"
            raw_name = cells[0]
            vessel_name = re.sub(r"\s*\([A-Z]+\)\s*$", "", raw_name).strip()
            code_match = re.search(r"\(([A-Z]+)\)\s*$", raw_name)
            vessel_type = _PV_TYPE_CODES.get(code_match.group(1), "") if code_match else ""

            if is_inport:
                # Columns: Ship Name | Berth | Arrived | ETD | To | Agent
                berth = cell_map.get("Berth", "")
                port = _pv_port_from_berth(berth)
                date_time = cell_map.get("ETD", cell_map.get("Arrived", ""))
                from_loc = ""
                to_loc = cell_map.get("To", "")
                agent = cell_map.get("Agent", "")
                in_port = "Yes"
            else:
                # Columns: Ship Name | Date & Time | From | To | Agent
                date_time = cell_map.get("Date & Time", "")
                from_loc = cell_map.get("From", "")
                to_loc = cell_map.get("To", "")
                agent = cell_map.get("Agent", "")
                berth = to_loc if current_movement == "Arrival" else from_loc
                port = _pv_port_from_berth(berth)
                in_port = ""

            # Supplement type detection with berth name for arrivals/in-port
            # (not all tankers carry a (T) suffix on the Ports Victoria page)
            relevant_berth = to_loc if not is_inport else cell_map.get("Berth", "")
            if not vessel_type and any(k in relevant_berth.lower() for k in _PV_FUEL_BERTHS):
                vessel_type = "Tanker"

            rows.append({
                "port": port,
                "state": "VIC",
                "Vessel": vessel_name,
                "Date & Time": date_time,
                "Movement": current_movement,
                "Vessel type": vessel_type,
                "Agent": agent,
                "From": from_loc,
                "To": to_loc,
                "In port": in_port,
                "cargo_type": "",
                "tonnage": None,
                "length_m": None,
                "customer": "",
            })

    return rows


def scrape_ports_victoria() -> list[dict]:
    """Scrape vessel movements from Ports Victoria (Melbourne + Geelong).

    Source: https://ports.vic.gov.au/marine-operations/ship-movements/
    5 tables: Expected Arrivals, Actual Arrivals, Expected Departures,
    Actual Departures, In Port. Updated hourly by Ports Victoria.
    Covers Gellibrand Pier and Holden Dock (Melbourne fuel import berths)
    as well as Geelong Refinery berths.
    """
    url = "https://ports.vic.gov.au/marine-operations/ship-movements/"
    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return _parse_ports_victoria(resp.text)


# ─── Generic PortControl API helpers ──────────────────────────


def _portcontrol_get_session(
    base_url: str, caller_path: str
) -> tuple[httpx.Client, str]:
    """Get a PortControl/WebX session with stamp."""
    client = httpx.Client(headers=HEADERS, timeout=15, follow_redirects=True)
    resp = client.get(f"{base_url}/")
    m = re.search(r"__stamp = '([^']+)'", resp.text)
    if not m:
        raise RuntimeError(f"Could not extract stamp from {base_url}")
    stamp = m.group(1) + "\x0b" + caller_path
    return client, stamp


def _portcontrol_get_data(
    client: httpx.Client, stamp: str, base_url: str, svc_path: str,
    report_code: str
) -> dict:
    """Fetch report data from a PortControl/WebX API."""
    info_payload = {
        "request": {
            "__type": "TGetReportInfoREQ:#WebX.Services",
            "stamp": stamp,
            "requestID": None,
            "reportCode": report_code,
            "dashbCode": None,
            "execContext": None,
            "token": None,
        }
    }
    r = client.post(
        f"{base_url}/{svc_path}/GetReportInfo",
        json=info_payload,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    info = r.json()["d"]
    ds_name = info["ReportInfo"]["DataSources"][0]["Name"]

    data_payload = {
        "request": {
            "__type": "TGetDataXREQ:#WebX.Services",
            "stamp": stamp,
            "requestID": None,
            "reportCode": report_code,
            "dataSource": ds_name,
            "filterName": None,
            "parameters": [],
            "metaVersion": 0,
            "token": None,
        }
    }
    r2 = client.post(
        f"{base_url}/{svc_path}/GetDataX",
        json=data_payload,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    return r2.json()["d"]


# ─── Fremantle (WA) via PortControl API ───────────────────────


FREMANTLE_BASE = "https://www3.fremantleports.com.au/vtmis"


def scrape_fremantle() -> list[dict]:
    """Scrape vessel movements from Fremantle Ports VTMIS API."""
    try:
        client, stamp = _portcontrol_get_session(FREMANTLE_BASE, "fmp.public/main-view")
    except Exception as e:
        print(f"Warning: Failed to get Fremantle session: {e}")
        return []

    rows = []
    for report_code in ["FMP-WEB-0001"]:
        try:
            data = _portcontrol_get_data(client, stamp, FREMANTLE_BASE,
                                         "services/wxdata.svc", report_code)
            table = data["Tables"][0]
            cols = [c["Name"] for c in table["MetaData"]["Columns"]]

            for row_data in table["Data"]:
                mapped = dict(zip(cols, row_data))
                vessel_class = str(mapped.get("VESSEL_CLASS", ""))
                job_type = str(mapped.get("JOB_TYPE_NAME", ""))

                rows.append({
                    "port": "Fremantle",
                    "state": "WA",
                    "Vessel": mapped.get("VESSEL_NAME", ""),
                    "Date & Time": str(mapped.get("START_TIME", "")),
                    "ARR / DEP": job_type,
                    "Vessel type": vessel_class,
                    "Agent": mapped.get("AGENCY_NAME", ""),
                    "From": mapped.get("LASTPORT_NAME", ""),
                    "To": mapped.get("NEXTPORT_NAME", ""),
                    "In port": "",
                    "cargo_type": "",
                    "tonnage": None,
                    "length_m": None,
                    "customer": "",
                })
        except Exception as e:
            print(f"Warning: Failed to fetch Fremantle {report_code}: {e}")

    return rows


# ─── QShips (QLD) via PortControl API ──────────────────────────


QSHIPS_BASE = "https://qships.tmr.qld.gov.au/webx"

# Map QLD port IDs to names
QLD_PORTS = {
    "67": "Brisbane",
    "40": "Gladstone",
    "296004": "Townsville",
    "295993": "Mackay",
    "295994": "Cairns",
}

JOB_TYPE_MAP = {
    "ARR": "Arrival",
    "DEP": "Departure",
    "EXT": "Arrival",      # Expected arrival
    "SHF": "Shift",
    "REM": "Removal",
}


def scrape_qships() -> list[dict]:
    """Scrape vessel movements from QShips (all QLD ports)."""
    try:
        client = httpx.Client(headers=HEADERS, timeout=15, follow_redirects=True)
        client.get(f"{QSHIPS_BASE}/")

        payload = {
            "token": None,
            "reportCode": "MSQ-WEB-0001",
            "dataSource": "DATA",
            "filterName": "Next 7 days",
            "parameters": [],
            "metaVersion": 0,
        }
        r = client.post(
            f"{QSHIPS_BASE}/services/wxdata.svc/GetDataX",
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        data = r.json()["d"]
        table = data["Tables"][0]
        cols = [c["Name"] for c in table["MetaData"]["Columns"]]

        rows = []
        for row_data in table["Data"]:
            mapped = dict(zip(cols, row_data))
            ship_type = str(mapped.get("MSQ_SHIP_TYPE", ""))
            job_code = str(mapped.get("JOB_TYPE_CODE", ""))
            loa = mapped.get("LOA")
            loa_float = float(loa) if loa else None

            # Determine port from TO_LOCATION_NAME or use generic "QLD"
            to_loc = str(mapped.get("TO_LOCATION_NAME", ""))
            from_loc = str(mapped.get("FROM_LOCATION_NAME", ""))

            # Try to identify port from berth location names
            port_name = "QLD Port"
            for loc in [to_loc, from_loc]:
                loc_lower = loc.lower()
                for pid, pname in QLD_PORTS.items():
                    if pname.lower() in loc_lower:
                        port_name = pname
                        break

            rows.append({
                "port": port_name,
                "state": "QLD",
                "Vessel": mapped.get("VESSEL_NAME", ""),
                "Date & Time": str(mapped.get("START_TIME", "")),
                "ARR / DEP": JOB_TYPE_MAP.get(job_code, job_code),
                "Vessel type": ship_type,
                "Agent": mapped.get("AGENCY_NAME", ""),
                "From": mapped.get("LASTPORT_NAME", ""),
                "To": mapped.get("NEXTPORT_NAME", ""),
                "In port": "",
                "cargo_type": "",
                "tonnage": None,
                "length_m": loa_float,
                "customer": "",
            })

        return rows
    except Exception as e:
        print(f"Warning: Failed to scrape QShips: {e}")
        return []


# ─── Flinders Ports (SA) via PortControl API ─────────────────


FLINDERS_BASE = "https://portmis.flindersports.com.au"


def scrape_flinders() -> list[dict]:
    """Scrape SA port movements from Flinders Ports PortControl API."""
    try:
        client, stamp = _portcontrol_get_session(FLINDERS_BASE, "fnt.public/main-view")
    except Exception as e:
        print(f"Warning: Failed to get Flinders Ports session: {e}")
        return []

    rows = []
    # FNT-WEB-0001 = Expected Ship Movements (all SA ports)
    try:
        data = _portcontrol_get_data(client, stamp, FLINDERS_BASE,
                                     "services/wxdata.svc", "FNT-WEB-0001")
        table = data["Tables"][0]
        cols = [c["Name"] for c in table["MetaData"]["Columns"]]

        for row_data in table["Data"]:
            mapped = dict(zip(cols, row_data))
            rows.append({
                "port": mapped.get("PORT", "SA Port"),
                "state": "SA",
                "Vessel": mapped.get("SHIP", ""),
                "Date & Time": str(mapped.get("DATE", "")),
                "ARR / DEP": mapped.get("JOB_TYPE", ""),
                "Vessel type": "",
                "Agent": mapped.get("AGENT", ""),
                "From": mapped.get("LAST_PORT", ""),
                "To": mapped.get("NEXT_PORT", ""),
                "In port": "",
                "cargo_type": "",
                "tonnage": None,
                "length_m": None,
                "customer": "",
            })
    except Exception as e:
        print(f"Warning: Failed to fetch Flinders Ports expected data: {e}")

    # FNT-PUBLIC-IN-PORT = Vessels currently in port
    try:
        data = _portcontrol_get_data(client, stamp, FLINDERS_BASE,
                                     "services/wxdata.svc", "FNT-PUBLIC-IN-PORT")
        table = data["Tables"][0]
        cols = [c["Name"] for c in table["MetaData"]["Columns"]]

        for row_data in table["Data"]:
            mapped = dict(zip(cols, row_data))
            rows.append({
                "port": mapped.get("PORT", "SA Port"),
                "state": "SA",
                "Vessel": mapped.get("SHIP", ""),
                "Date & Time": str(mapped.get("ARR_DATE", "")),
                "ARR / DEP": "In Port",
                "Vessel type": "",
                "Agent": mapped.get("ADENT", ""),
                "From": mapped.get("TO_LOCATION", ""),
                "To": "",
                "In port": mapped.get("BERTH", ""),
                "cargo_type": "",
                "tonnage": None,
                "length_m": None,
                "customer": "",
            })
    except Exception as e:
        print(f"Warning: Failed to fetch Flinders Ports in-port data: {e}")

    return rows


# ─── Darwin Port (NT) via PortControl API ────────────────────


DARWIN_BASE = "https://portinfo.darwinport.com.au/webx"

_DARWIN_JOB_MAP = {"ARR": "Arrival", "DEP": "Departure", "EXT": "External"}


def scrape_darwin() -> list[dict]:
    """Scrape NT port movements from Darwin Port PortControl API."""
    try:
        client, stamp = _portcontrol_get_session(DARWIN_BASE, "audrw.public/main-view")
    except Exception as e:
        print(f"Warning: Failed to get Darwin Port session: {e}")
        return []

    rows = []
    # AUDRW-WEB-0001 = Current Schedule
    try:
        data = _portcontrol_get_data(client, stamp, DARWIN_BASE,
                                     "services/wxdata.svc", "AUDRW-WEB-0001")
        table = data["Tables"][0]
        cols = [c["Name"] for c in table["MetaData"]["Columns"]]

        for row_data in table["Data"]:
            mapped = dict(zip(cols, row_data))
            job_code = str(mapped.get("JOB_TYPE", ""))
            job_type = _DARWIN_JOB_MAP.get(job_code, job_code)

            rows.append({
                "port": mapped.get("PORT", "Darwin"),
                "state": "NT",
                "Vessel": mapped.get("SHIP", ""),
                "Date & Time": str(mapped.get("DATE", "")),
                "ARR / DEP": job_type,
                "Vessel type": "",
                "Agent": mapped.get("AGENT", ""),
                "From": mapped.get("LAST_PORT", ""),
                "To": mapped.get("NEXT_PORT", ""),
                "In port": "",
                "cargo_type": "",
                "tonnage": None,
                "length_m": None,
                "customer": "",
            })
    except Exception as e:
        print(f"Warning: Failed to fetch Darwin Port schedule data: {e}")

    # AUDRW-WEB-0003 = Vessels currently in port
    try:
        data = _portcontrol_get_data(client, stamp, DARWIN_BASE,
                                     "services/wxdata.svc", "AUDRW-WEB-0003")
        table = data["Tables"][0]
        cols = [c["Name"] for c in table["MetaData"]["Columns"]]

        for row_data in table["Data"]:
            mapped = dict(zip(cols, row_data))
            rows.append({
                "port": "Darwin",
                "state": "NT",
                "Vessel": mapped.get("SHIP", ""),
                "Date & Time": str(mapped.get("ARR_DATE", "")),
                "ARR / DEP": "In Port",
                "Vessel type": "",
                "Agent": mapped.get("AGENT", ""),
                "From": mapped.get("TO_LOCATION", ""),
                "To": "",
                "In port": mapped.get("BERTH", ""),
                "cargo_type": "",
                "tonnage": None,
                "length_m": None,
                "customer": "",
            })
    except Exception as e:
        print(f"Warning: Failed to fetch Darwin Port in-port data: {e}")

    return rows


# ─── TasPorts (TAS) via JSON API ────────────────────────────


TASPORTS_BASE = "https://tasports.com.au/actions/site-module/shipping/get-json-data"


def scrape_tasports() -> list[dict]:
    """Scrape TAS port movements from TasPorts JSON API."""
    rows = []
    try:
        client = httpx.Client(headers=HEADERS, timeout=15, follow_redirects=True)

        # Expected Shipping (arrivals, departures, shifts)
        url = f"{TASPORTS_BASE}?type=External+Schedule+-+Expected+Shipping&filterPort="
        resp = client.get(url)
        resp.raise_for_status()
        for item in resp.json():
            # Parse date: "29/03/2026 - Sunday" → "29/03/2026"
            date_str = str(item.get("DATE", ""))
            if " - " in date_str:
                date_str = date_str.split(" - ")[0]
            # Append time from ETA/D if available
            eta = item.get("ETA_X002F_D", "")
            if eta and date_str:
                date_str = f"{date_str} {eta}"

            length = item.get("LOA", "")
            try:
                length_m = float(length) if length else None
            except (ValueError, TypeError):
                length_m = None

            rows.append({
                "port": item.get("PORT", "TAS Port"),
                "state": "TAS",
                "Vessel": item.get("VESSEL", ""),
                "Date & Time": date_str,
                "ARR / DEP": item.get("MOVEMENT", ""),
                "Vessel type": item.get("VESSEL_TYPE", ""),
                "Agent": item.get("AGENT", ""),
                "From": item.get("PORT_FROM", ""),
                "To": item.get("PORT_TO", ""),
                "In port": "",
                "cargo_type": "",
                "tonnage": None,
                "length_m": length_m,
                "customer": "",
            })

        # Vessels in Port
        url2 = f"{TASPORTS_BASE}?type=External+Schedule+-+Vessels+in+Port&filterPort="
        resp2 = client.get(url2)
        resp2.raise_for_status()
        for item in resp2.json():
            date_str = str(item.get("ALONGSIDE_BERTH", ""))
            time_str = item.get("FIRST_LINE_TIME", "")
            if time_str and date_str:
                date_str = f"{date_str} {time_str}"

            length = item.get("LOA", "")
            try:
                length_m = float(length) if length else None
            except (ValueError, TypeError):
                length_m = None

            rows.append({
                "port": item.get("PORT", "TAS Port"),
                "state": "TAS",
                "Vessel": item.get("VESSEL", ""),
                "Date & Time": date_str,
                "ARR / DEP": "In Port",
                "Vessel type": item.get("VESSEL_TYPE", ""),
                "Agent": item.get("AGENT", ""),
                "From": item.get("PORT_FROM", ""),
                "To": item.get("PORT_TO", ""),
                "In port": item.get("BERTH", ""),
                "cargo_type": "",
                "tonnage": None,
                "length_m": length_m,
                "customer": "",
            })

    except Exception as e:
        print(f"Warning: Failed to fetch TasPorts data: {e}")

    return rows


# ─── Normalisation ─────────────────────────────────────────────


EMPTY_SCHEMA = {
    "port": pl.String,
    "state": pl.String,
    "date_time": pl.String,
    "movement": pl.String,
    "vessel": pl.String,
    "vessel_type": pl.String,
    "cargo_type": pl.String,
    "tonnage": pl.Float64,
    "length_m": pl.Float64,
    "est_volume": pl.String,
    "est_confidence": pl.String,
    "origin_type": pl.String,
    "origin_detail": pl.String,
    "origin_country": pl.String,
    "is_tanker": pl.Boolean,
    "agent": pl.String,
    "from_location": pl.String,
    "to_location": pl.String,
    "customer": pl.String,
    "in_port": pl.String,
}


def _empty_df() -> pl.DataFrame:
    return pl.DataFrame(schema=EMPTY_SCHEMA)


def _normalise_rows(raw_rows: list[dict]) -> list[dict]:
    normalised = []
    for row in raw_rows:
        n = {
            "port": row.get("port", ""),
            "state": row.get("state", ""),
        }

        for key in ["Date & Time", "Date", "DateTime", "date_time"]:
            if key in row:
                n["date_time"] = _normalise_date(str(row[key]))
                break
        else:
            n["date_time"] = ""

        for key in ["ARR / DEP", "ARR/DEP", "Movement"]:
            if key in row:
                n["movement"] = row[key]
                break
        else:
            n["movement"] = ""

        for key in ["Vessel", "Ship", "Name"]:
            if key in row:
                n["vessel"] = row[key]
                break
        else:
            n["vessel"] = ""

        for key in ["Vessel type", "Vessel Type", "Ship Type"]:
            if key in row:
                n["vessel_type"] = row[key]
                break
        else:
            n["vessel_type"] = ""

        n["cargo_type"] = row.get("cargo_type", "")
        n["tonnage"] = row.get("tonnage")
        n["length_m"] = row.get("length_m")
        n["customer"] = row.get("customer", "")
        n["is_tanker"] = _is_tanker(n["vessel_type"], n["cargo_type"], n["vessel"])

        # Fuel volume estimate
        tonnage = n["tonnage"]
        length = n["length_m"]
        if n["is_tanker"]:
            n["est_volume"], n["est_confidence"] = estimate_fuel_volume(length, tonnage)
        else:
            n["est_volume"] = ""
            n["est_confidence"] = ""

        # Origin classification
        from_loc = row.get("From", "")
        n["from_location"] = from_loc
        n["to_location"] = row.get("To", "")
        n["origin_type"], n["origin_detail"] = classify_origin(from_loc)
        # For international origins, origin_detail is already the country (via _lookup_country).
        # For domestic, it's the AU state. Store country separately for filtering.
        if n["origin_type"] == "International":
            n["origin_country"] = n["origin_detail"]
        elif n["origin_type"] == "Domestic":
            n["origin_country"] = "Australia"
        else:
            n["origin_country"] = ""

        n["agent"] = row.get("Agent", "")
        n["in_port"] = row.get("In port", "")

        normalised.append(n)
    return normalised


_PORT_CACHE = Path("data/port_schedule.json")
_PORT_SEED = Path("seed/port_schedule.json")
_PORT_CACHE_MAX_AGE = timedelta(hours=3)


def scrape_all_ports(tankers_only: bool = False) -> pl.DataFrame:
    """Scrape vessel movements from all available Australian ports.

    Currently covers:
    - NSW: Sydney Harbour, Port Botany, Port Kembla, Newcastle
    - VIC: Geelong (Viva Energy refinery)
    - WA: Fremantle (PortControl API)
    - QLD: All ports via QShips (PortControl API)
    - SA: All ports via Flinders Ports (PortControl API)
    - NT: Darwin (PortControl API)
    - TAS: All ports via TasPorts (JSON API)

    Caches to data/port_schedule.json (3 h TTL); falls back to disk on failure.
    """
    def _df_from_cache(records: list[dict], tankers_only: bool) -> pl.DataFrame:
        if not records:
            return _empty_df()
        df = pl.DataFrame(records, infer_schema_length=None)
        if tankers_only and "is_tanker" in df.columns:
            df = df.filter(pl.col("is_tanker"))
        return df

    # Check disk cache (full data, filter after)
    if _PORT_CACHE.exists():
        try:
            cached = json.loads(_PORT_CACHE.read_text(encoding="utf-8"))
            fetched_at = datetime.fromisoformat(cached["fetched_at"])
            if datetime.now(tz=timezone.utc) - fetched_at < _PORT_CACHE_MAX_AGE:
                return _df_from_cache(cached["data"], tankers_only)
        except Exception:
            pass

    # In offline mode, serve seed before attempting any live scrape
    from config import is_offline
    if is_offline() and _PORT_SEED.exists():
        try:
            cached = json.loads(_PORT_SEED.read_text(encoding="utf-8"))
            return _df_from_cache(cached["data"], tankers_only)
        except Exception:
            pass

    # Scrape each port individually so one failure doesn't wipe out the rest
    _SCRAPERS = [
        ("NSW",       scrape_nsw_ports),
        ("VIC",       scrape_geelong),
        ("Melbourne", scrape_ports_victoria),
        ("WA",        scrape_fremantle),
        ("QLD",       scrape_qships),
        ("SA",        scrape_flinders),
        ("NT",        scrape_darwin),
        ("TAS",       scrape_tasports),
    ]

    all_raw = []
    port_status: dict[str, dict] = {}
    any_succeeded = False

    for label, fn in _SCRAPERS:
        try:
            rows = fn()
            all_raw.extend(rows)
            port_status[label] = {"ok": True, "count": len(rows)}
            any_succeeded = True
        except Exception as exc:
            port_status[label] = {"ok": False, "error": str(exc)[:200]}

    if any_succeeded:
        normalised = _normalise_rows(all_raw)
        df = pl.DataFrame(normalised, infer_schema_length=None) if normalised else _empty_df()

        # Save full (unfiltered) data + per-port status to disk
        _PORT_CACHE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _PORT_CACHE.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({
                "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
                "port_status": port_status,
                "data": df.to_dicts(),
            }, default=str),
            encoding="utf-8",
        )
        tmp.replace(_PORT_CACHE)

        if tankers_only:
            df = df.filter(pl.col("is_tanker"))
        return df

    # All scrapers failed — fall back to disk cache or seed
    for src in (_PORT_CACHE, _PORT_SEED):
        if src.exists():
            try:
                cached = json.loads(src.read_text(encoding="utf-8"))
                return _df_from_cache(cached["data"], tankers_only)
            except Exception:
                pass
    return _empty_df()


def get_port_scrape_status() -> dict[str, dict] | None:
    """Return per-port scrape status from the last run, or None if no cache exists.

    Each value is either {"ok": True, "count": N} or {"ok": False, "error": "..."}.
    """
    for src in (_PORT_CACHE, _PORT_SEED):
        if src.exists():
            try:
                cached = json.loads(src.read_text(encoding="utf-8"))
                return cached.get("port_status")
            except Exception:
                pass
    return None


if __name__ == "__main__":
    print("Scraping all port vessel movements...")
    df = scrape_all_ports()
    print(f"Total vessels: {len(df)}")
    print(f"States covered: {df['state'].unique().sort().to_list()}")
    print(f"Ports covered: {df['port'].unique().sort().to_list()}")

    tankers = df.filter(pl.col("is_tanker"))
    print(f"\nTankers/fuel vessels: {len(tankers)}")
    if len(tankers) > 0:
        for row in tankers.iter_rows(named=True):
            print(
                f"  {row['vessel']:25s} | {row['vessel_type']:25s} | "
                f"{row['movement']:12s} | {row['origin_type']:12s} {row['origin_detail']:15s} | "
                f"vol={row['est_volume']} ({row['est_confidence']})"
            )
