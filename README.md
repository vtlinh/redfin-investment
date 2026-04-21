# mai-investment

A small ETL + analysis toolkit for evaluating NJ residential real estate as rental investments. Pulls listings from Realtor.com (via the `realty-in-us` RapidAPI wrapper), stores them in SQLite, computes year-1 cash flow and multi-year ROI, and serves the results through a Flask webapp.

Current coverage: 6 NJ counties (Essex, Bergen, Hudson, Passaic, Morris, Union), both for-sale and for-rent.

## Components

- **`fetch.py`** — hits `POST /properties/v3/list` per county, UPSERTs each listing into a persistent `properties` table, calls `/properties/v3/detail` for newly-seen multi-family for-sale listings to resolve unit counts, then upserts the `rent_comps` cache from the for-rent rows. Commits incrementally per county and every 50 detail fetches so progress survives an interrupt. CLI flags: `--limit N` (total cap), `--per-county-limit N`, `--refresh-detail` (re-parse cached detail payload for rows missing HOA/management fees — reuses `extra_info.detail` cache, makes no API calls unless not yet cached).
- **`analyze.py`** — reads `properties` + `rent_comps`, computes year-1 cash flow and annualized total ROI for each active for-sale listing, writes into `cashflow_analysis`. All financing/cost assumptions live in the `DEFAULTS` dict at the top of the file.
- **`webapp.py`** + **`templates/index.html`** — Flask app (port 5000) serving an infinite-scroll, filterable, sortable list of active for-sale properties. Each row expands to a 15-year projection with per-cell breakdown tooltips. A settings panel lets users tweak the calculation assumptions and recompute the DB in place; settings persist across reloads via cookies. Cards display `Low income area` and `Has HOA` tags. Properties in low-income census tracts/ZIPs (median income <$60k or poverty >15%) are flagged and use 2× the configured vacancy and maintenance rates. Two default-on filters exclude low-income areas and listings with <5 photos.
- **`rentcast_fill.py`** — (disabled by default) fills `external_rent_estimates` from Rentcast AVM / HUD FMR for the top-N gap ZIP buckets. Set `RENTCAST_FILL_ENABLE=1` to run.
- **`test_fetch.py`**, **`test_analyze.py`** — pytest unit tests.

## Setup

```bash
uv sync                                  # installs deps (incl. dev) into .venv
echo "RAPIDAPI_KEY=..." > .env           # gitignored; auto-loaded by fetch.py
```

## Run

```bash
uv run fetch.py          # refill properties.db from Realtor.com
uv run analyze.py        # rebuild cashflow_analysis
uv run webapp.py         # http://127.0.0.1:5000
uv run pytest -q         # tests
```

## Configuration

`fetch.py`:
- `COUNTIES` — list of `(county, state_code)` tuples to query.
- `STATUSES` — list of Realtor.com status filters (default: `for_sale`, `ready_to_build`, `for_rent`).
- `PAGE_SIZE`, `MAX_PER_QUERY` — pagination controls.

`analyze.py` `DEFAULTS`:
- Financing: down payment %, interest rate, loan term, closing cost %.
- Holding costs: insurance %, maintenance %, other costs %, vacancy %, management fee %.
- Growth: rent, tax, insurance, HOA, maintenance, other costs (annual %).
- Exit: value appreciation %/yr, holding years, sell cost %.

The webapp lets the user override any of these per-session; the `POST /recompute` route rebuilds `cashflow_analysis` in place with the user's config.

## Storage

SQLite file `properties.db` (gitignored). Schema is persistent across runs — detail-enrichment work (unit counts, source listing status) is not redone for listings already inspected. Every API-derived table (`properties`, `zip_demographics`, `tract_demographics`, `external_rent_estimates`) carries an `extra_info` JSON column that caches the raw API payload, so extraction logic can be re-tuned and re-applied without new API calls. Schema migration is handled by a lightweight `migrate()` that `ALTER`s in missing columns. `rent_comps` is upserted in place — buckets with no current rentals are left untouched rather than dropped.

## API

- Endpoint: `POST https://realty-in-us.p.rapidapi.com/properties/v3/list`
- Auth: `X-RapidAPI-Key` header, subscribed via RapidAPI.
- Free tier: ~500 calls/month. One full run ≈ a few dozen list calls + one-time detail calls for new multi-family listings.
