# CLAUDE.md

Guidance for Claude Code when working in this repo.

## What this is

A small ETL that pulls real estate listings from **Realtor.com** (via the `realty-in-us` RapidAPI wrapper) and stores them in a local SQLite database for ad-hoc analysis. Current sample covers 2 NJ counties (Essex, Bergen) — both for-sale and for-rent, targeting 10-40k unique listings.

History: the project originally targeted the SimplyRETS demo feed (fake Texas data). It now uses Realtor.com because SimplyRETS only serves realistic data for licensed MLS subscribers.

## Layout

- `fetch.py` — POSTs to `/properties/v3/list`, flattens each `SearchHome` result, **UPSERTs** into the persistent `properties` table, calls `/properties/v3/detail` for any new multi-family for-sale listings to resolve unit counts, then **builds the `rent_comps` cache** from the for-rent rows. Single file, raw SQL — no ORM.
- `analyze.py` — reads `properties` + `rent_comps`, computes year-1 cash flow for each for-sale property, writes results to `cashflow_analysis`. All financing/cost assumptions are globals at the top of the file — tune them there.
- `webapp.py` + `templates/index.html` — Flask app serving a paginated list (10/page) of active for-sale properties sorted by CoC. Each row links to the realtor.com listing; a chevron toggles an inline 15-year projection (rent +5%/yr, non-mortgage expenses +3%/yr, value +3%/yr, mortgage fixed). Run with `uv run webapp.py` → http://127.0.0.1:5000.
- `test_fetch.py`, `test_analyze.py` — pytest unit tests.
- `pyproject.toml` / `uv.lock` — dependencies managed by [uv](https://docs.astral.sh/uv/); runtime deps `requests` + `python-dotenv`, dev dep `pytest` (`sqlite3` is stdlib).
- `properties.db` — SQLite file (gitignored).
- `.env` (gitignored) — holds `RAPIDAPI_KEY=...`; loaded automatically by `fetch.py` via `python-dotenv`.

## Config

`fetch.py` is driven by two top-level constants:
- `COUNTIES` — list of `(county, state_code)` tuples. Each county is queried in one call (with pagination) using the API's `county` filter.
- `STATUSES` — flat list of status filters passed together in one `/list` call per county (the API accepts a status array), so sale + rent come back in a single request.

Current setup: 2 NJ counties (Essex, Bergen), statuses `["for_sale", "ready_to_build", "for_rent"]`, capped at `MAX_PER_QUERY=10000` per county. API cost scales with pagination at `PAGE_SIZE=200`, so one run is roughly `ceil(results / 200)` list calls per county plus detail calls for new multi-family listings.

## Schema

Single `properties` table, PK `property_id` (Realtor.com's stable property identifier). Notable columns:

- Status/pricing: `status` (`for_sale`, `for_rent`, `sold`, etc.), `list_price` (monthly rent when `status='for_rent'`), `list_date`, `last_update`
- Property: `property_type`, `sub_type`, `bedrooms`, `baths_full`, `baths_half`, `baths_total`, `area_sqft`, `lot_sqft`, `year_built`, `stories`
- Location: `address_line`, `city`, `state`, `postal_code`, `latitude`, `longitude`, `county_fips`
- Costs: `hoa_fee`
- People/media: `agent_name`, `office_name`, `primary_photo`
- Freeform: `tags_json` (JSON array), `extra_info` (JSON blob of flags/open_houses/virtual_tours/matterport/photo_count)
- Units (populated by detail enrichment): `num_units`, `beds_per_unit_json`, `baths_per_unit_json`, `units_source`
- Liveness: `is_active` (set to 0 at start of every run, flipped to 1 for each UPSERT), `last_seen_at`, `source_listing_status` (MLS status from detail endpoint, e.g. "Active"/"Pending")
- Bookkeeping: `fetched_at`, `detail_fetched_at`

Indexes on `city`, `status`, `list_price`, `is_active`.

The `properties` table is **persistent across runs** so detail-enrichment work (unit counts, source listing status) is not re-done for listings we've already inspected. Schema uses `CREATE IF NOT EXISTS` plus a lightweight `migrate(con)` that `ALTER`s in missing columns. If you add a new column, list it in `_MIGRATION_COLUMNS` *and* in `SCHEMA` so fresh DBs and existing DBs both get it. `rent_comps` is still dropped + rebuilt every run (cheap).

### Detail enrichment & unit detection

After the list-fetch pass, `enrich_pending_details` scans rows where
`is_active=1 AND status='for_sale' AND detail_fetched_at IS NULL` and
resolves `num_units` using a **cost-aware signal chain** (cheapest first):

1. `sub_type` literal (`duplex`/`triplex`/`quadplex`) — no extra API call.
2. `property_type` classification (single_family, condos, etc.) → 1 unit, no call.
3. *Detail call*: `description.units` explicit integer.
4. *Detail call*: MLS "Source Property Type" regex (e.g. "2 Family", "3 Unit").
5. *Detail call*: `description.text` free-text regex for "N-family" / "N unit".
6. Fallback: 2 for multi-family, 1 otherwise.

The resolving signal is recorded in `units_source` for later auditing. Beds are split across units (with remainder distributed to early units); baths are divided evenly. Detail calls also populate `source_listing_status`, which `analyze.py` uses to filter out pending/under-contract/sold listings.

Cost: ~150 extra detail calls on a cold DB; subsequent runs only call detail for newly-listed properties. Free tier is 500 calls/month.

### `rent_comps` (written by `fetch.py`)

Precomputed median-rent cache so the analyzer doesn't recompute comps for every for-sale row. Columns: `(city, bedrooms, baths, median_rent, sample_size)` — PK is the triple. Baths are bucketed via `round(baths_total)`.

Two row types per (beds, baths) bucket:
- **City-specific** rows are written only when the city has ≥ `MIN_COMP_SAMPLES` rentals in that bucket (default 3).
- **`city IS NULL`** fallback row pools every rental in the DB for that bucket — used when a town doesn't have enough local samples.

`analyze.py` looks up `(city, beds, round(baths))`, falls back to the NULL-city row if no local entry, returns `None` if neither exists (listing skipped).

### `cashflow_analysis` (written by `analyze.py`)

One row per for-sale property that has usable rental comps. PK `property_id` (FK to `properties`):

- `annual_income` — comp-derived gross yearly rent; for multi-family, per-unit comp × estimated units
- `mortgage` — annual P&I on a `(1 - DOWN_PAYMENT_PCT) * list_price` loan at `MORTGAGE_RATE` for `LOAN_TERM_YEARS`
- `expenses` — mortgage + property tax + insurance + HOA + maintenance + other + vacancy (year 1)
- `cash_flow` — `annual_income - expenses` (positive = cash-positive in year 1)
- `cash_on_cash_return` — `cash_flow / down_payment`

Multi-family handling: `analyze.py` prefers `num_units` + `beds_per_unit_json` + `baths_per_unit_json` populated by `fetch.py`'s detail pass. If those are missing (old rows, or detail call failed), it falls back to the old `round(baths_total)` heuristic from `estimate_units`. The `MULTI_FAMILY_TYPES` set controls which `property_type` values trigger unit-splitting.

Active-listing filter: `analyze.py` only considers rows where `is_active=1` and `source_listing_status` is NULL or not one of `INACTIVE_SOURCE_STATUSES` (pending/under contract/contingent/sold/etc.). NULL is treated as "keep" because we don't always have detail data.

## Run

```bash
uv sync                                  # install deps (incl. dev) into .venv
# put RAPIDAPI_KEY=... in .env (gitignored) — auto-loaded by fetch.py
uv run fetch.py                          # drops + refills properties.db
uv run analyze.py                        # rebuilds cashflow_analysis table
uv run pytest -q                         # run tests
```

## API notes

- Endpoint: `POST https://realty-in-us.p.rapidapi.com/properties/v3/list`
- Auth: `X-RapidAPI-Key` header, subscribed via RapidAPI
- Max page size tested: 200+. Pagination via `offset`; `data.home_search.total` gives the full count.
- Rentals: pass `status: ["for_rent"]` on the same endpoint. The separate `/list-for-rent` paths 404 or 204.
- Free tier: ~500 calls/month. County-level run scales with pagination: at `PAGE_SIZE=200`, expect `ceil(county_total / 200)` calls per county. Two NJ counties (Essex + Bergen) with heavy listings typically consume several dozen list calls per run plus one-time detail calls for new multi-family listings.
