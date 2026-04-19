# CLAUDE.md

Guidance for Claude Code when working in this repo.

## What this is

A small ETL that pulls real estate listings from the **SimplyRETS** demo API and stores them in a local SQLite database for ad-hoc analysis.

SimplyRETS is a REST wrapper over MLS RETS/RESO Web API feeds. The demo feed (`https://api.simplyrets.com`, basic auth `simplyrets:simplyrets`) returns ~65 fake Texas listings — fine for prototyping. Real data requires an MLS license.

## Layout

- `fetch.py` — fetch `/properties` with cursor pagination (`lastId`), flatten into columns, upsert into SQLite by `mls_id`.
- `requirements.txt` — just `requests`; `sqlite3` is stdlib.
- `properties.db` — SQLite file (gitignored).

## Schema

Single `properties` table, PK `mls_id`. Notable columns:

- Pricing/status: `list_price`, `list_date`, `modification_ts`, `status`, `days_on_market`
- Property: `property_type`, `sub_type`, `bedrooms`, `baths_full`, `baths_half`, `area_sqft`, `year_built`, `lot_size`, `stories`
- Location: `address_full`, `street`, `city`, `state`, `postal_code`, `country`, `latitude`, `longitude`
- Tax: `tax_id`, `tax_annual_amount`, `tax_year`
- Schools: `school_district`, `elementary_school`, `middle_school`, `high_school`
- People: `agent_name`, `office_name`
- Freeform: `remarks`, `photos_json` (JSON array of URLs), `extra_info` (JSON blob of any API fields not promoted to columns)
- Bookkeeping: `fetched_at`

Indexes on `city`, `status`, `list_price`.

When a new field becomes important, promote it from `extra_info` to a real column in `fetch.py` (update `CORE_TOP_LEVEL`/nested core sets, `SCHEMA`, `UPSERT`, and the `flatten()` return dict), then drop & refetch `properties.db`.

## Run

```bash
pip install -r requirements.txt
python fetch.py        # idempotent upsert by mls_id
```

## Notes

- The demo feed is read-only and unchanging; re-running just refreshes `fetched_at`.
- `fetch.py` is deliberately a single file with raw SQL — no ORM, no config layer. Keep it that way until the project clearly outgrows it.
