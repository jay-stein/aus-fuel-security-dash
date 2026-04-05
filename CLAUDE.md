# Australian Fuel Security Dashboard — Claude Code Instructions

## Tech Stack

- **Python 3.13**, managed with `uv` (use `uv run` for all commands)
- **Streamlit** multi-page app — entry point `app.py`, pages in `pages/`
- **Polars** DataFrames (not pandas) everywhere except Streamlit's `st.dataframe` which needs pandas
- **Plotly** (`go.Figure`, `go.Scattergeo`, `px.*`) for all charts and maps

## Run

```bash
uv run streamlit run app.py
```

## Key Files

| File | Purpose |
|------|---------|
| `app.py` | Streamlit entry point, sidebar nav |
| `data_loader.py` | All data fetching and caching — APS workbook, FRED, AIP, MSO Power BI |
| `dashboard_utils.py` | Shared constants (RAG thresholds, column maps, HHI), helper functions |
| `port_scraper.py` | Scrapes 7 port authority sites; exports `scrape_all_ports()`, `AU_PORT_COORDS`, `COUNTRY_COORDS` |
| `vessel_lookup.py` | IMO/spec lookup (MyShipTracking → VesselFinder fallback); caches to `data/vessel_cache.json` |
| `shipnext_scraper.py` | ShipNext API live positions; caches to `data/shipnext_positions.json` (3h TTL) |
| `ais_tracker.py` | AISStream.io WebSocket snapshot; exports `fetch_ais_snapshot()`, `estimate_position_on_route()` |

## Pages

| Page | Key data source |
|------|----------------|
| `1_Situation_Room.py` | Aggregates all sources for RAG status overview |
| `3_Demand_Signals.py` | DCCEEW MSO Power BI API (weekly stocks) + APS monthly sales |
| `4_Supply_Security.py` | APS stock levels and consumption cover days |
| `5_IEA_Obligation.py` | APS net import cover vs 90-day IEA obligation |
| `6_Import_Risk.py` | APS import source countries, HHI, chokepoint exposure |
| `7_Incoming_Tankers.py` | Port scraper schedules (7 port authorities) |
| `8_Wholesale_Prices.py` | FRED (Brent), AIP terminal gate prices, Yahoo Finance futures |
| `9_Tanker_Tracker.py` | AIS live positions + port-schedule overlay + dead-reckoned estimates |

## Data Files (gitignored, under `data/`)

- `data/australian-petroleum-statistics.xlsx` — download manually from data.gov.au
- `data/mso_weekly.json` — cached MSO Power BI data (6h TTL); delete to force refresh
- `data/ais_positions.json` — cached AIS snapshot (5 min TTL)
- `data/vessel_cache.json` — vessel specs cache (IMO, DWT, GT, flag etc.); failures stored with 7-day TTL for auto-retry
- `data/shipnext_positions.json` — ShipNext live position cache (3h TTL)

## Secrets

Add to `.streamlit/secrets.toml`:

```toml
[aisstream]
api_key = "your-aisstream-key"
```

## Deployment — Streamlit Community Cloud

Deployed from GitHub: `jay-stein/aus-fuel-security-dash` (branch `master`, entry point `app.py`).

**To deploy a new version:**
1. `uv run python refresh_seed.py` — refresh seed data snapshots with current live data
2. Commit seed files + any code changes, push to `master`
3. Streamlit Cloud picks up the push automatically

**Seed data (`seed/`)** — committed JSON snapshots used as a last-resort fallback when the
app cold-starts and live fetches haven't run yet. Covers: MSO weekly, Brent crude, fuel futures,
terminal gate prices. The loader chain is:
`fresh data/ cache → live fetch → stale data/ cache → seed/ snapshot`

**APS workbook** — auto-downloaded from data.gov.au on first load (via CKAN API) if not present.
Takes ~10–15 s on cold start. The `data/` directory is ephemeral on Community Cloud (resets on restart).

**Secrets** — add in the Streamlit Cloud dashboard under App settings → Secrets:
```toml
[aisstream]
api_key = "your-key"

[app]
offline_mode = true   # skips all live HTTP fetches; serves seed data instead
```

**Offline mode** — when `offline_mode = true`, all live fetches (MSO, FRED, Yahoo, AIP, port
scraper) are skipped and seed data is served directly. Every page shows an orange sidebar
warning and the landing page shows a prominent info banner. Locally, omit the secret (or set
env var `OFFLINE_MODE=0`) to use live scraping as normal.

## Conventions

- RAG thresholds live in `dashboard_utils.py` — do not hardcode them in pages
- All data fetching goes through `data_loader.py` or a dedicated module (port_scraper, ais_tracker)
- Use `@st.cache_data(ttl=...)` on every data fetch in pages — never call scrapers directly in page body
- Polars throughout; only convert to pandas immediately before `st.dataframe()` or `px.*`
- Column name mappings (FUEL_COLS, COVER_COLS, STATE_SALES_COLS) are in `dashboard_utils.py`

## DCCEEW MSO Power BI API

Weekly fuel stocks (diesel/jet/petrol) via public Power BI embed. No auth needed.

- Base URL: `https://wabi-australia-east-b-primary-api.analysis.windows.net`
- Resource Key: `372fa8f8-8dc7-44c7-a1a9-1967565a3793`
- Model ID: `2191920`
- Header: `X-PowerBI-ResourceKey`
- Entities: `Days`, `Surplus`, `Chart - Reported`, `MSO Requirements`
- Loader: `data_loader.load_mso_weekly()` — caches to `data/mso_weekly.json`

## AISStream

- Subscribes to all PositionReport messages in Australia bbox (no MMSI filter)
- Filters client-side to AIS ShipType 80–89 (all tanker variants)
- Snapshot duration: 45 seconds
- Tanker Tracker page overlays three data layers:
  - `ais` — live AIS positions (triangle-up markers, full colour)
  - `port_schedule` — port-scraper "In Port" vessels (grey circle)
  - `estimated` — dead-reckoned positions from ETA (light grey open circle, dotted route)

## Vessel Lookup Strategy (`vessel_lookup.py`)

Name → IMO resolution uses a two-source chain with progressive name cleaning:

1. **MyShipTracking** — primary; searches `?name={query}`, extracts `imo-{7digits}` from result hrefs
2. **VesselFinder search** — fallback; searches `?name={query}`, extracts `/vessels/details/{7digits}` hrefs
3. **VesselFinder detail page** — spec fetch once IMO is confirmed

**Name cleaning** (`_clean_vessel_name`) tries variants in order:
- Original name
- Strip vessel-type prefixes: `MV`, `MT`, `SS`, `M/V`, `M/T` etc.
- Apostrophe → space, then apostrophe → removed (handles `AL FAT'H`, `ILE D'YEU`)
- Dot removal (handles `K. ACACIA`, `TAITAR NO. 4` → `TAITAR NO 4`)
- Hyphen variants: `NORD-AM` → `NORD AM`, `NORDAM`
- `NO.` abbreviation removal: `TAITAR NO. 4` → `TAITAR 4`
- Trailing number removal: `GRAND WINNER 6` → `GRAND WINNER`
- Last word (only if ≥ 5 chars and not purely numeric)

**Failure caching**: failed lookups stored as `{"failed_at": "..."}` with 7-day TTL.
After expiry the vessel is retried automatically. Old-style `{}` entries (permanent failures)
are treated as cache misses on next read, triggering immediate retry.

## External Reference Tools

### ShipNext — Vessel Tracking & Voyage Intelligence

- **URL pattern:** `https://shipnext.com/vessel/{IMO}/{VESSEL_NAME}`
- **Example:** https://shipnext.com/vessel/1030703/CSK%20JUBILEE
- **Useful for:** Current vessel position, live voyage progress (origin → destination),
  ETA, vessel specs, AIS track history
- **When to use:** Cross-checking vessel origin/destination from port scrapers, debugging
  misclassified trade directions (Import/Export/Coastal), verifying ETA accuracy.
  IMO number is available in the dashboard's `v_imo` field (from VesselFinder cache).
