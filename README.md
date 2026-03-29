# Australian Fuel Security Dashboard

Decision-support dashboard for monitoring Australia's fuel supply security. Built with Streamlit, Polars, and Plotly.

## Pages

- **Situation Room** — RAG status overview, alerts, key indicators at a glance
- **Supply Security** — Consumption cover days, stock levels, drawdown trends
- **IEA Obligation** — 90-day net import cover compliance tracking
- **Import Risk** — Source country concentration, HHI index, chokepoint analysis
- **Incoming Tankers** — Live tanker movements from 7 state port authorities
- **Wholesale Prices** — Brent crude, terminal gate prices, fuel futures
- **Tanker Tracker** — Live AIS vessel tracking (coming soon)

## Setup

Requires [uv](https://docs.astral.sh/uv/) and Python 3.13+.

```bash
uv sync
```

### Data

Download the **Australian Petroleum Statistics** Excel workbook from [data.gov.au](https://data.gov.au/data/dataset/australian-petroleum-statistics) and place it at:

```
data/australian-petroleum-statistics.xlsx
```

The `data/` directory is gitignored. Other data files (`vessel_cache.json`, `brisbane_schedule.json`) are generated at runtime by the scrapers.

## Run

```bash
uv run streamlit run app.py
```

## Data Sources

- [DCCEEW Australian Petroleum Statistics](https://data.gov.au/data/dataset/australian-petroleum-statistics)
- Port authority feeds (NSW, VIC, WA, QLD, SA, NT, TAS)
- FRED (Brent crude)
- AIP (terminal gate prices)
- Yahoo Finance (fuel futures)
