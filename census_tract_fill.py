"""Geocode active for-sale properties to census tracts (via FCC Area API) and
fetch ACS5 income/poverty data at the tract level.

Tract-level data is more granular than ZIP-level: a borderline ZIP like East
Orange 07017 can contain tracts that individually cross the deprivation
thresholds even when the ZIP average does not.

Steps:
  1. Add tract_fips column to properties if missing.
  2. Fetch all NJ census tract ACS5 demographics in one Census API call.
  3. Store in tract_demographics table.
  4. For every active for-sale property with lat/lon and no tract_fips,
     call the FCC Area API (free, no key) to resolve census block FIPS;
     first 11 digits = tract FIPS.
  5. Update properties.tract_fips.

Usage:
  uv run census_tract_fill.py              # geocode all missing
  uv run census_tract_fill.py --limit 200  # geocode up to 200 at a time
"""

import os
import sqlite3
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

DB_PATH    = Path(os.environ.get("DB_PATH", Path(__file__).parent / "properties.db"))
CENSUS_KEY = os.environ.get("CENSUS_KEY")

CENSUS_URL  = "https://api.census.gov/data/2023/acs/acs5"
FCC_URL     = "https://geo.fcc.gov/api/census/block/find"

# Deprivation thresholds (same logic used in webapp.py build_where).
LOW_INCOME_THRESHOLD = 70_000   # flag below this income
HIGH_POVERTY_THRESHOLD = 0.15   # flag above this poverty rate


def fetch_nj_tracts(api_key=None):
    """Return list of (tract_fips_11, income, poverty_rate) for all NJ tracts."""
    params = {
        "get": "B19013_001E,B17001_002E,B17001_001E,NAME",
        "for": "tract:*",
        "in":  "state:34",
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
            state  = row[idx["state"]]
            county = row[idx["county"]]
            tract  = row[idx["tract"]]
            income = int(row[idx["B19013_001E"]])
            pov_n  = int(row[idx["B17001_002E"]])
            pov_d  = int(row[idx["B17001_001E"]])
        except (ValueError, KeyError):
            continue
        if income < 0 or pov_d <= 0:
            continue
        tract_fips = state + county + tract   # 11-digit
        results.append((tract_fips, income, pov_n / pov_d))
    return results


def geocode_fcc(lat, lon, session, retries=2):
    """Return 11-digit census tract FIPS for (lat, lon), or None on failure."""
    for attempt in range(retries + 1):
        try:
            r = session.get(FCC_URL, params={"format": "json", "latitude": lat, "longitude": lon},
                            timeout=15)
            if r.status_code == 200:
                fips = r.json()["Block"]["FIPS"]
                return fips[:11]
        except Exception:
            pass
        if attempt < retries:
            time.sleep(1)
    return None


def ensure_schema(con):
    con.execute(
        """CREATE TABLE IF NOT EXISTS tract_demographics (
            tract_fips               TEXT PRIMARY KEY,
            median_household_income  INTEGER,
            poverty_rate             REAL,
            fetched_at               TEXT NOT NULL)"""
    )
    existing = {r[1] for r in con.execute("PRAGMA table_info(properties)")}
    if "tract_fips" not in existing:
        con.execute("ALTER TABLE properties ADD COLUMN tract_fips TEXT")
    con.commit()


def store_tract_demographics(con, rows):
    now = datetime.now(timezone.utc).isoformat()
    with con:
        con.executemany(
            "INSERT OR REPLACE INTO tract_demographics"
            " (tract_fips, median_household_income, poverty_rate, fetched_at)"
            " VALUES (?, ?, ?, ?)",
            [(f, inc, pov, now) for f, inc, pov in rows],
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0,
                        help="Max properties to geocode (0 = all)")
    args = parser.parse_args()

    con = sqlite3.connect(DB_PATH)
    ensure_schema(con)

    print("Fetching NJ census tract ACS5 data...")
    tracts = fetch_nj_tracts(CENSUS_KEY or None)
    store_tract_demographics(con, tracts)
    flagged = sum(
        1 for _, inc, pov in tracts
        if inc < LOW_INCOME_THRESHOLD or pov > HIGH_POVERTY_THRESHOLD
    )
    print(f"Stored {len(tracts)} NJ census tracts "
          f"({flagged} flagged as deprived).")

    pending = con.execute(
        """SELECT property_id, latitude, longitude
           FROM properties
           WHERE is_active=1 AND status='for_sale'
             AND latitude IS NOT NULL AND longitude IS NOT NULL
             AND tract_fips IS NULL"""
    ).fetchall()
    if args.limit:
        pending = pending[: args.limit]

    print(f"\nGeocoding {len(pending)} properties via FCC Area API...")
    session = requests.Session()
    updated = 0
    failed  = 0
    for i, (pid, lat, lon) in enumerate(pending):
        tract = geocode_fcc(lat, lon, session)
        if tract:
            with con:
                con.execute("UPDATE properties SET tract_fips=? WHERE property_id=?",
                            (tract, pid))
            updated += 1
        else:
            failed += 1
        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{len(pending)} geocoded ({updated} ok, {failed} failed)...")
        time.sleep(0.05)

    print(f"\nDone: {updated} tract FIPS stored, {failed} failed.")

    covered = con.execute(
        "SELECT COUNT(*) FROM properties WHERE is_active=1 AND status='for_sale'"
        " AND tract_fips IS NOT NULL"
    ).fetchone()[0]
    total_sale = con.execute(
        "SELECT COUNT(*) FROM properties WHERE is_active=1 AND status='for_sale'"
    ).fetchone()[0]
    print(f"Tract coverage: {covered}/{total_sale} active for-sale properties.")
    con.close()


if __name__ == "__main__":
    main()
