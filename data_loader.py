"""Load and parse the Australian Petroleum Statistics Excel data."""

import io
from datetime import date, datetime, timedelta

import httpx
import polars as pl


XLSX_PATH = "data/australian-petroleum-statistics.xlsx"

_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


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
    df = pl.read_excel(XLSX_PATH, sheet_name="Imports volume")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_imports_by_country() -> pl.DataFrame:
    """Monthly import volumes (ML) by source country and product."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Imports volume by country")
    df = df.rename({df.columns[0]: "month", df.columns[1]: "country"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month", "country"])


def load_exports_volume() -> pl.DataFrame:
    """Monthly export volumes (ML) by product type."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Exports volume")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_sales() -> pl.DataFrame:
    """Monthly national sales volumes (ML) by product type."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Sales of products")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_sales_by_state() -> pl.DataFrame:
    """Monthly sales volumes (ML) by state and product type."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Sales by state and territory")
    df = df.rename({df.columns[0]: "state", df.columns[1]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["state", "month"])


def load_stocks() -> pl.DataFrame:
    """Monthly stock volumes (ML) by product type."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Stock volume by product")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_consumption_cover() -> pl.DataFrame:
    """Monthly consumption cover (days) by product type."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Consumption cover")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_refinery_production() -> pl.DataFrame:
    """Monthly refinery production (ML) by product type."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Refinery production")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_fuel_prices() -> pl.DataFrame:
    """Quarterly Australian fuel prices (cents per litre)."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Australian fuel prices")
    df = df.rename({df.columns[0]: "year", df.columns[1]: "quarter"})
    return _clean_numeric(df, ["year", "quarter"])


def load_iea_net_import_cover() -> pl.DataFrame:
    """Monthly IEA net import coverage (days) and daily net imports (kT/day)."""
    df = pl.read_excel(XLSX_PATH, sheet_name="IEA days net import cover")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_stocks_incl_on_way() -> pl.DataFrame:
    """Monthly stock volumes including on-the-way (ML) — on-land, at-sea, overseas."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Stock volume incl. on the way")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_iea_days_incl_on_way() -> pl.DataFrame:
    """Monthly IEA days including on-the-way stocks (breakdown by location)."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Stock IEA days incl. on the way")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


def load_petroleum_production() -> pl.DataFrame:
    """Monthly domestic petroleum production (crude oil, condensate, LPG, gas)."""
    df = pl.read_excel(XLSX_PATH, sheet_name="Petroleum production")
    df = df.rename({df.columns[0]: "month"})
    df = df.with_columns(pl.col("month").cast(pl.Date))
    return _clean_numeric(df, ["month"])


# ─── Wholesale / benchmark price loaders ────────────────────


def load_brent_crude(days: int = 180) -> pl.DataFrame:
    """Fetch daily Brent crude prices from FRED (no API key needed).

    Returns DataFrame with columns: date (Date), brent_usd (Float64).
    """
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
    # FRED uses "." for missing values
    df = df.with_columns(
        pl.col("brent_usd").cast(pl.String).replace(".", None).cast(pl.Float64)
    )
    return df.drop_nulls("brent_usd")


def load_fuel_futures(days: int = 180) -> pl.DataFrame:
    """Fetch RBOB Gasoline + Heating Oil daily closes from Yahoo Finance.

    Returns DataFrame with columns: date (Date), rbob_usd (Float64), heating_oil_usd (Float64).
    """
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
    return df.sort("date").drop_nulls(subset=["rbob_usd", "heating_oil_usd"])


def load_tgp_data(days: int = 180) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Download AIP Terminal Gate Price Excel, return (petrol_df, diesel_df).

    Each DataFrame has columns: date, Sydney, Melbourne, Brisbane, Adelaide,
    Perth, Darwin, Hobart, National Average — all in cents/litre inc GST.
    Only returns the last `days` of data.
    """
    # The AIP filename includes the publication date — try recent dates
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
        # Rename city columns to clean names
        col_names = ["date", "Sydney", "Melbourne", "Brisbane", "Adelaide",
                     "Perth", "Darwin", "Hobart", "National Average"]
        if len(df.columns) >= len(col_names):
            renames = {old: new for old, new in zip(df.columns, col_names)}
            df = df.rename(renames)
        df = df.with_columns(pl.col("date").cast(pl.Date))
        # Filter to recent period
        df = df.filter(pl.col("date") >= cutoff)
        # Ensure numeric columns
        for c in df.columns[1:]:
            df = df.with_columns(pl.col(c).cast(pl.Float64, strict=False))
        results.append(df)

    return results[0], results[1]
