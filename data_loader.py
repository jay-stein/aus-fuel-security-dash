"""Load and parse the Australian Petroleum Statistics Excel data."""

import io
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import polars as pl


XLSX_PATH = "data/australian-petroleum-statistics.xlsx"

_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

_CKAN_API = "https://data.gov.au/api/3/action/package_show?id=australian-petroleum-statistics"


def _aps_direct_urls() -> list[str]:
    """Return candidate direct download URLs for the APS workbook (most recent first)."""
    from datetime import date
    urls = []
    today = date.today()
    for months_back in range(0, 8):
        year = today.year
        month = today.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        slug = f"{year}-{month:02d}"
        urls.append(
            f"https://www.energy.gov.au/sites/default/files/documents/"
            f"australian-petroleum-statistics-{slug}.xlsx"
        )
    return urls


def _ensure_aps_downloaded() -> None:
    """Download the APS workbook if not already present.

    Tries the data.gov.au CKAN API first, then falls back to known direct
    URL patterns on energy.gov.au.
    """
    path = Path(XLSX_PATH)
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)

    # Strategy 1: CKAN API
    try:
        meta = httpx.get(_CKAN_API, headers=_HTTP_HEADERS, timeout=20)
        meta.raise_for_status()
        resources = meta.json()["result"]["resources"]
        xlsx_resources = [r for r in resources if r.get("format", "").upper() == "XLSX"]
        xlsx_resources.sort(
            key=lambda r: r.get("last_modified") or r.get("created") or "", reverse=True
        )
        if xlsx_resources:
            resp = httpx.get(
                xlsx_resources[0]["url"], headers=_HTTP_HEADERS, timeout=120, follow_redirects=True
            )
            if resp.status_code == 200 and len(resp.content) > 10_000:
                path.write_bytes(resp.content)
                return
    except Exception:
        pass

    # Strategy 2: direct energy.gov.au URL patterns
    for url in _aps_direct_urls():
        try:
            resp = httpx.get(url, headers=_HTTP_HEADERS, timeout=60, follow_redirects=True)
            if resp.status_code == 200 and len(resp.content) > 10_000:
                path.write_bytes(resp.content)
                return
        except Exception:
            continue

    raise RuntimeError(
        "Could not download the Australian Petroleum Statistics workbook from "
        "data.gov.au or energy.gov.au. Pages that require it will be unavailable."
    )


def _clean_numeric(df: pl.DataFrame, skip_cols: list[str]) -> pl.DataFrame:
    """Convert string columns to float, treating 'n.a.' and blanks as null."""
    casts = []
    for col in df.columns:
        if col in skip_cols:
            continue
        casts.append(
            pl.col(col)
            .cast(pl.String)
            .str.replace_all(",", "")
            .str.strip_chars()
            .replace({"n.a.": None, "": None, "None": None})
            .cast(pl.Float64, strict=False)
            .alias(col)
        )
    return df.with_columns(casts)


def load_imports_volume() -> pl.DataFrame:
    """Monthly import volumes (ML) by product type."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Imports volume")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_imports_by_country() -> pl.DataFrame:
    """Monthly import volumes (ML) by source country and product."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Imports volume by country")
    df = df.rename({df.columns[0]: "month", df.columns[1]: "country"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month", "country"])


def load_exports_volume() -> pl.DataFrame:
    """Monthly export volumes (ML) by product type."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Exports volume")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_sales() -> pl.DataFrame:
    """Monthly national sales volumes (ML) by product type."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Sales of products")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_sales_by_state() -> pl.DataFrame:
    """Monthly sales volumes (ML) by state and product type."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Sales by state and territory")
    df = df.rename({df.columns[0]: "state", df.columns[1]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["state", "month"])


def load_stocks() -> pl.DataFrame:
    """Monthly stock volumes (ML) by product type."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Stock volume by product")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_consumption_cover() -> pl.DataFrame:
    """Monthly consumption cover (days) by product type."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Consumption cover")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_refinery_production() -> pl.DataFrame:
    """Monthly refinery production (ML) by product type."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Refinery production")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_fuel_prices() -> pl.DataFrame:
    """Quarterly Australian fuel prices (cents per litre)."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Australian fuel prices")
    df = df.rename({df.columns[0]: "year", df.columns[1]: "quarter"})
    return _clean_numeric(df, ["year", "quarter"])


def load_iea_net_import_cover() -> pl.DataFrame:
    """Monthly IEA net import coverage (days) and daily net imports (kT/day)."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="IEA days net import cover")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_stocks_incl_on_way() -> pl.DataFrame:
    """Monthly stock volumes including on-the-way (ML) — on-land, at-sea, overseas."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Stock volume incl. on the way")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_iea_days_incl_on_way() -> pl.DataFrame:
    """Monthly IEA days including on-the-way stocks (breakdown by location)."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Stock IEA days incl. on the way")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_petroleum_production() -> pl.DataFrame:
    """Monthly domestic petroleum production (crude oil, condensate, LPG, gas)."""
    _ensure_aps_downloaded()
    df = pl.read_excel(XLSX_PATH, sheet_name="Petroleum production")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


# ─── DCCEEW Weekly MSO Stock Data (Power BI) ────────────────

_PBI_BASE = "https://wabi-australia-east-b-primary-api.analysis.windows.net"
_PBI_RESOURCE_KEY = "372fa8f8-8dc7-44c7-a1a9-1967565a3793"
_PBI_MODEL_ID = 2191920
_MSO_CACHE = Path("data/mso_weekly.json")
_MSO_CACHE_MAX_AGE = timedelta(hours=6)


def _pbi_query(entity_from: list[dict], select: list[dict],
               order_by: list[dict] | None = None, top: int = 500) -> dict:
    """Build a Power BI SemanticQuery request body."""
    query = {"Version": 2, "From": entity_from, "Select": select}
    if order_by:
        query["OrderBy"] = order_by
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": [{"SemanticQueryDataShapeCommand": {
                "Query": query,
                "ExecutionMetricsKind": 1,
                "Binding": {
                    "Primary": {"Groupings": [{"Projections": list(range(len(select)))}]},
                    "DataReduction": {"DataVolume": 4, "Primary": {"Top": {"Count": top}}},
                    "Version": 1,
                },
            }}]},
            "QueryId": "",
            "ApplicationContext": {"DatasetId": "", "Sources": []},
        }],
        "cancelQueries": [],
        "modelId": _PBI_MODEL_ID,
    }


def _pbi_col(source: str, prop: str, name: str) -> dict:
    return {"Column": {"Expression": {"SourceRef": {"Source": source}}, "Property": prop}, "Name": name}


def _pbi_order_desc(source: str, prop: str) -> dict:
    return {"Direction": 2, "Expression": {"Column": {"Expression": {"SourceRef": {"Source": source}}, "Property": prop}}}


def _decode_dsr_rows(resp_json: dict, num_cols: int) -> tuple[list[list], dict]:
    """Decode Power BI DSR format rows and value dictionaries."""
    data = resp_json["results"][0]["result"]["data"]
    dsr = data.get("dsr", {})
    value_dicts = dsr.get("ValueDicts", {})
    rows = []
    prev = [None] * num_cols
    for dataset in dsr.get("DS", []):
        for group in dataset.get("PH", []):
            for row in group.get("DM0", group.get("DM1", [])):
                c_vals = row.get("C", [])
                r_mask = row.get("R", 0)
                full = []
                c_idx = 0
                for i in range(num_cols):
                    if r_mask & (1 << i):
                        full.append(prev[i])
                    elif c_idx < len(c_vals):
                        full.append(c_vals[c_idx])
                        c_idx += 1
                    else:
                        full.append(None)
                prev = full[:]
                rows.append(full)
    return rows, value_dicts


def _fetch_mso_days() -> list[dict]:
    """Query weekly MSO days-of-supply from Power BI."""
    body = _pbi_query(
        entity_from=[{"Name": "d", "Entity": "Days", "Type": 0}],
        select=[
            _pbi_col("d", "Obligation day", "d.day"),
            _pbi_col("d", "MSO_Days_Diesel", "d.diesel"),
            _pbi_col("d", "MSO_Days_JetFuel", "d.jet"),
            _pbi_col("d", "MSO_Days_Petrol", "d.petrol"),
        ],
        order_by=[_pbi_order_desc("d", "Obligation day")],
    )
    resp = httpx.post(
        f"{_PBI_BASE}/public/reports/querydata",
        json=body,
        headers={"X-PowerBI-ResourceKey": _PBI_RESOURCE_KEY, **_HTTP_HEADERS},
        timeout=20,
    )
    resp.raise_for_status()
    rows, _ = _decode_dsr_rows(resp.json(), 4)
    return [
        {
            "week_ending": datetime.fromtimestamp(r[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
            "diesel_days": r[1],
            "jet_fuel_days": r[2],
            "petrol_days": r[3],
        }
        for r in rows if r[0] is not None
    ]


def _fetch_mso_surplus() -> list[dict]:
    """Query weekly MSO surplus percentages from Power BI."""
    body = _pbi_query(
        entity_from=[{"Name": "s", "Entity": "Surplus", "Type": 0}],
        select=[
            _pbi_col("s", "Obligation day", "s.day"),
            _pbi_col("s", "Automotive gasoline", "s.petrol"),
            _pbi_col("s", "Aviation kerosene", "s.jet"),
            _pbi_col("s", "Automotive diesel", "s.diesel"),
        ],
        order_by=[_pbi_order_desc("s", "Obligation day")],
    )
    resp = httpx.post(
        f"{_PBI_BASE}/public/reports/querydata",
        json=body,
        headers={"X-PowerBI-ResourceKey": _PBI_RESOURCE_KEY, **_HTTP_HEADERS},
        timeout=20,
    )
    resp.raise_for_status()
    rows, _ = _decode_dsr_rows(resp.json(), 4)
    def _to_float(v) -> float | None:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    return [
        {
            "week_ending": datetime.fromtimestamp(r[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
            "petrol_surplus_pct": _to_float(r[1]),
            "jet_fuel_surplus_pct": _to_float(r[2]),
            "diesel_surplus_pct": _to_float(r[3]),
        }
        for r in rows if r[0] is not None
    ]


def _fetch_mso_volumes() -> list[dict]:
    """Query weekly reported stock volumes from Power BI."""
    body = _pbi_query(
        entity_from=[{"Name": "r", "Entity": "Chart - Reported", "Type": 0}],
        select=[
            _pbi_col("r", "Rpt MSO_Main_Form[ObligationDate]", "r.date"),
            _pbi_col("r", "Rpt MSO_Main_Form[MSOProduct]", "r.product"),
            _pbi_col("r", "[SumReportedVolume]", "r.volume"),
        ],
        order_by=[_pbi_order_desc("r", "Rpt MSO_Main_Form[ObligationDate]")],
    )
    resp = httpx.post(
        f"{_PBI_BASE}/public/reports/querydata",
        json=body,
        headers={"X-PowerBI-ResourceKey": _PBI_RESOURCE_KEY, **_HTTP_HEADERS},
        timeout=20,
    )
    resp.raise_for_status()
    rows, value_dicts = _decode_dsr_rows(resp.json(), 3)
    # Product dict: 0=Automotive gasoline, 1=Aviation kerosene, 2=Automotive diesel
    product_map = value_dicts.get("D0", ["Automotive gasoline", "Aviation kerosene", "Automotive diesel"])
    results: dict[str, dict] = {}
    for r in rows:
        if r[0] is None:
            continue
        week = datetime.fromtimestamp(r[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        product_name = product_map[r[1]] if isinstance(r[1], int) and r[1] < len(product_map) else str(r[1])
        if week not in results:
            results[week] = {"week_ending": week}
        col = {
            "Automotive gasoline": "petrol_volume_ml",
            "Aviation kerosene": "jet_fuel_volume_ml",
            "Automotive diesel": "diesel_volume_ml",
        }.get(product_name)
        if col:
            results[week][col] = r[2]
    return list(results.values())


def _cast_mso_types(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure MSO DataFrame has correct numeric types."""
    casts = [pl.col("week_ending").cast(pl.String).str.to_date("%Y-%m-%d")]
    for col in df.columns:
        if col.endswith("_pct"):
            casts.append(pl.col(col).cast(pl.String).cast(pl.Float64, strict=False))
        elif col.endswith("_days") or col.endswith("_ml"):
            casts.append(pl.col(col).cast(pl.Float64, strict=False))
    return df.with_columns(casts)


def load_mso_weekly() -> pl.DataFrame:
    """Load weekly MSO stock data (days, surplus %, volumes).

    Fetches from DCCEEW Power BI and caches to data/mso_weekly.json.
    Returns DataFrame sorted by week_ending ascending.
    """
    # Check cache
    if _MSO_CACHE.exists():
        cache = json.loads(_MSO_CACHE.read_text())
        cached_at = datetime.fromisoformat(cache["fetched_at"])
        if datetime.now(tz=timezone.utc) - cached_at < _MSO_CACHE_MAX_AGE:
            return _cast_mso_types(pl.DataFrame(cache["data"])).sort("week_ending")

    try:
        days_data = _fetch_mso_days()
        surplus_data = _fetch_mso_surplus()
        volumes_data = _fetch_mso_volumes()

        # Join on week_ending
        df_days = pl.DataFrame(days_data)
        df_surplus = pl.DataFrame(surplus_data)
        df_volumes = pl.DataFrame(volumes_data)

        df = df_days.join(df_surplus, on="week_ending", how="left")
        df = df.join(df_volumes, on="week_ending", how="left")

        df = _cast_mso_types(df)

        _MSO_CACHE.parent.mkdir(parents=True, exist_ok=True)
        cache = {
            "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
            "data": df.to_dicts(),
        }
        _MSO_CACHE.write_text(json.dumps(cache, default=str))

        return df.sort("week_ending")

    except Exception:
        # Fall back to stale runtime cache, then committed seed snapshot
        for src in (_MSO_CACHE, _MSO_SEED):
            if src.exists():
                cache = json.loads(src.read_text())
                return _cast_mso_types(pl.DataFrame(cache["data"])).sort("week_ending")
        raise


# ─── Wholesale / benchmark price loaders ────────────────────

_BRENT_CACHE = Path("data/brent_prices.json")
_FUTURES_CACHE = Path("data/futures.json")
_TGP_CACHE = Path("data/aip_tgp.json")
_PRICE_CACHE_MAX_AGE = timedelta(hours=6)
_TGP_CACHE_MAX_AGE = timedelta(hours=24)

# Seed snapshots committed to the repo — used as last-resort fallback when
# live fetch fails and no runtime cache exists yet (e.g. fresh cloud deploy).
_MSO_SEED = Path("seed/mso_weekly.json")
_BRENT_SEED = Path("seed/brent_prices.json")
_FUTURES_SEED = Path("seed/futures.json")
_TGP_SEED = Path("seed/aip_tgp.json")


def _load_seed(path: Path) -> dict:
    """Read a seed JSON file; raise FileNotFoundError if absent."""
    if not path.exists():
        raise FileNotFoundError(f"Seed file not found: {path}")
    return json.loads(path.read_text())


def _disk_cache_fresh(path: Path, max_age: timedelta) -> bool:
    """Return True if a JSON disk cache exists and is younger than max_age."""
    if not path.exists():
        return False
    try:
        fetched_at = datetime.fromisoformat(
            json.loads(path.read_text())["fetched_at"]
        )
        return datetime.now(tz=timezone.utc) - fetched_at < max_age
    except Exception:
        return False


def _write_disk_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, default=str))
    tmp.replace(path)


def load_brent_crude(days: int = 180) -> pl.DataFrame:
    """Fetch daily Brent crude prices from FRED (no API key needed).

    Returns DataFrame with columns: date (Date), brent_usd (Float64).
    Caches to data/brent_prices.json (6 h TTL); falls back to disk on failure.
    """
    if _disk_cache_fresh(_BRENT_CACHE, _PRICE_CACHE_MAX_AGE):
        cached = json.loads(_BRENT_CACHE.read_text())
        df = pl.DataFrame(cached["data"])
        return df.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

    try:
        end = date.today()
        start = end - timedelta(days=days)
        url = (
            "https://fred.stlouisfed.org/graph/fredgraph.csv"
            f"?id=DCOILBRENTEU&cosd={start}&coed={end}&fq=Daily&fam=avg"
        )
        resp = httpx.get(url, headers=_HTTP_HEADERS, timeout=15)
        resp.raise_for_status()
        df = pl.read_csv(io.StringIO(resp.text))
        df = df.rename({"observation_date": "date", "DCOILBRENTEU": "brent_usd"})
        df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
        df = df.with_columns(
            pl.col("brent_usd").cast(pl.String).replace(".", None).cast(pl.Float64)
        )
        df = df.drop_nulls("brent_usd")
        _write_disk_cache(_BRENT_CACHE, {
            "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
            "data": df.with_columns(pl.col("date").cast(pl.String)).to_dicts(),
        })
        return df
    except Exception:
        for src in (_BRENT_CACHE, _BRENT_SEED):
            if src.exists():
                cached = json.loads(src.read_text())
                df = pl.DataFrame(cached["data"])
                return df.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
        raise


def load_fuel_futures(days: int = 180) -> pl.DataFrame:
    """Fetch RBOB Gasoline + Heating Oil daily closes from Yahoo Finance.

    Returns DataFrame with columns: date (Date), rbob_usd (Float64), heating_oil_usd (Float64).
    Caches to data/futures.json (6 h TTL); falls back to disk on failure.
    """
    if _disk_cache_fresh(_FUTURES_CACHE, _PRICE_CACHE_MAX_AGE):
        cached = json.loads(_FUTURES_CACHE.read_text())
        df = pl.DataFrame(cached["data"])
        return df.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

    try:
        end_ts = int(datetime.now().timestamp())
        start_ts = int((datetime.now() - timedelta(days=days)).timestamp())

        frames = {}
        for ticker, col_name in [("RB=F", "rbob_usd"), ("HO=F", "heating_oil_usd")]:
            url = (
                f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                f"?period1={start_ts}&period2={end_ts}&interval=1d"
            )
            resp = httpx.get(url, headers=_HTTP_HEADERS, timeout=15)
            resp.raise_for_status()
            result = resp.json()["chart"]["result"][0]
            timestamps = result["timestamp"]
            closes = result["indicators"]["quote"][0]["close"]
            dates = [date.fromtimestamp(ts) for ts in timestamps]
            frames[col_name] = pl.DataFrame({
                "date": dates,
                col_name: closes,
            }).with_columns(pl.col("date").cast(pl.Date))

        df = frames["rbob_usd"].join(frames["heating_oil_usd"], on="date", how="full", coalesce=True)
        df = df.sort("date").drop_nulls(subset=["rbob_usd", "heating_oil_usd"])
        _write_disk_cache(_FUTURES_CACHE, {
            "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
            "data": df.with_columns(pl.col("date").cast(pl.String)).to_dicts(),
        })
        return df
    except Exception:
        for src in (_FUTURES_CACHE, _FUTURES_SEED):
            if src.exists():
                cached = json.loads(src.read_text())
                df = pl.DataFrame(cached["data"])
                return df.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
        raise


def load_tgp_data(days: int = 180) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Download AIP Terminal Gate Price Excel, return (petrol_df, diesel_df).

    Each DataFrame has columns: date, Sydney, Melbourne, Brisbane, Adelaide,
    Perth, Darwin, Hobart, National Average — all in cents/litre inc GST.
    Caches to data/aip_tgp.json (24 h TTL); falls back to disk on failure.
    """
    def _df_from_cache(records: list[dict]) -> pl.DataFrame:
        df = pl.DataFrame(records)
        return df.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))

    if _disk_cache_fresh(_TGP_CACHE, _TGP_CACHE_MAX_AGE):
        cached = json.loads(_TGP_CACHE.read_text())
        return _df_from_cache(cached["petrol"]), _df_from_cache(cached["diesel"])

    try:
        today = date.today()
        content = None
        for offset in range(10):
            d = today - timedelta(days=offset)
            fname = f"AIP_TGP_Data_{d.strftime('%d-%b-%Y')}.xlsx"
            url = (
                f"https://www.aip.com.au/sites/default/files/download-files/"
                f"{d.strftime('%Y-%m')}/{fname}"
            )
            try:
                resp = httpx.get(url, headers=_HTTP_HEADERS, timeout=15, follow_redirects=True)
                if resp.status_code == 200 and len(resp.content) > 1000:
                    content = resp.content
                    break
            except httpx.HTTPError:
                continue

        if content is None:
            raise RuntimeError("Could not download AIP TGP data (tried last 10 days)")

        cutoff = today - timedelta(days=days)
        results = []
        for sheet in ["Petrol TGP", "Diesel TGP"]:
            df = pl.read_excel(io.BytesIO(content), sheet_name=sheet)
            df = df.rename({df.columns[0]: "date"})
            col_names = ["date", "Sydney", "Melbourne", "Brisbane", "Adelaide",
                         "Perth", "Darwin", "Hobart", "National Average"]
            if len(df.columns) >= len(col_names):
                renames = {old: new for old, new in zip(df.columns, col_names)}
                df = df.rename(renames)
            df = df.with_columns(pl.col("date").cast(pl.Date))
            df = df.filter(pl.col("date") >= cutoff)
            for c in df.columns[1:]:
                df = df.with_columns(pl.col(c).cast(pl.Float64, strict=False))
            results.append(df)

        serialise = lambda df: df.with_columns(pl.col("date").cast(pl.String)).to_dicts()
        _write_disk_cache(_TGP_CACHE, {
            "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
            "petrol": serialise(results[0]),
            "diesel": serialise(results[1]),
        })
        return results[0], results[1]
    except Exception:
        for src in (_TGP_CACHE, _TGP_SEED):
            if src.exists():
                cached = json.loads(src.read_text())
                return _df_from_cache(cached["petrol"]), _df_from_cache(cached["diesel"])
        raise
