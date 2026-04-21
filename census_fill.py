"""Fetch Census ACS5 demographic data for NJ zip codes and store in zip_demographics.

Uses the Census Bureau Data API (free, requires a free API key from
https://api.census.gov/data/key_signup.html).

Variables pulled per ZCTA:
  B19013_001E  Median household income (past 12 months)
  B17001_002E  Population below poverty level
  B17001_001E  Total population for poverty calculation

Usage:
  CENSUS_KEY=... uv run census_fill.py
"""

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

DB_PATH    = Path(os.environ.get("DB_PATH", Path(__file__).parent / "properties.db"))
CENSUS_KEY = os.environ.get("CENSUS_KEY")

CENSUS_URL = "https://api.census.gov/data/2023/acs/acs5"

# Thresholds for flagging a zip as low-income.
# A zip is flagged if EITHER condition is true.
LOW_INCOME_THRESHOLD   = 50_000   # median household income below this
HIGH_POVERTY_THRESHOLD = 0.20     # poverty rate above this (20%)


def fetch_acs_nj(api_key=None):
    """Fetch ACS5 median income and poverty data for NJ ZCTAs.
    ZCTAs don't nest under states in the Census API, so we fetch all ZCTAs
    and filter to NJ prefixes (07xxx / 08xxx). API key is optional.

    Returns list of (zcta, income, poverty_rate, raw_row_dict). `raw_row_dict`
    preserves the original Census API row (header → value) so downstream code
    can cache the unprocessed payload in extra_info.
    """
    params = {
        "get": "B19013_001E,B17001_002E,B17001_001E,NAME",
        "for": "zip code tabulation area:*",
    }
    if api_key:
        params["key"] = api_key
    r = requests.get(CENSUS_URL, params=params, timeout=60)
    r.raise_for_status()
    rows = r.json()
    headers = rows[0]
    idx = {h: i for i, h in enumerate(headers)}
    results = []
    for row in rows[1:]:
        try:
            zcta     = row[idx["zip code tabulation area"]]
            income   = int(row[idx["B19013_001E"]])
            pov_num  = int(row[idx["B17001_002E"]])
            pov_den  = int(row[idx["B17001_001E"]])
        except (ValueError, KeyError):
            continue
        if not (zcta.startswith("07") or zcta.startswith("08")):
            continue
        if income < 0 or pov_den <= 0:
            continue
        poverty_rate = pov_num / pov_den
        raw = dict(zip(headers, row))
        results.append((zcta, income, poverty_rate, raw))
    return results


def store(con, rows):
    now = datetime.now(timezone.utc).isoformat()
    con.executemany(
        """
        INSERT OR REPLACE INTO zip_demographics
            (postal_code, median_household_income, poverty_rate, fetched_at, extra_info)
        VALUES (?, ?, ?, ?, ?)
        """,
        [(z, inc, pov, now, json.dumps(raw)) for z, inc, pov, raw in rows],
    )


def main():
    con = sqlite3.connect(DB_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS zip_demographics (
            postal_code              TEXT PRIMARY KEY,
            median_household_income  INTEGER,
            poverty_rate             REAL,
            fetched_at               TEXT NOT NULL,
            extra_info               TEXT
        )
        """
    )
    from fetch import _ensure_extra_info
    _ensure_extra_info(con, "zip_demographics")
    con.commit()

    print("Fetching ACS5 data for NJ ZCTAs...")
    rows = fetch_acs_nj(CENSUS_KEY or None)
    with con:
        store(con, rows)

    flagged = sum(
        1 for _, inc, pov, _ in rows
        if inc < LOW_INCOME_THRESHOLD or pov > HIGH_POVERTY_THRESHOLD
    )
    print(f"Stored {len(rows)} zip codes ({flagged} flagged as low-income/high-poverty).")
    print(f"Thresholds: income < ${LOW_INCOME_THRESHOLD:,} OR poverty rate > {HIGH_POVERTY_THRESHOLD:.0%}")
    con.close()


if __name__ == "__main__":
    main()
