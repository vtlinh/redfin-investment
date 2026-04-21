"""Fill external_rent_estimates from Rentcast AVM and/or HUD Fair Market Rents.

Steps:
  1. Gap analysis -- group all active for-sale properties by (postal_code, beds, baths),
     expanding multi-family listings to individual units using per-unit JSON.
  2. Filter out groups already covered by city-specific rent_comps or
     already present in external_rent_estimates.
  3. Print the top-20 unserved groups (sorted by property count desc).
  4. If RENTCAST_KEY env var is set, fetch Rentcast AVM estimates for those groups.
  5. If HUD_TOKEN env var is set, fetch HUD Fair Market Rents for NJ and store
     zip-level estimates for each unserved group.

Usage:
  uv run rentcast_fill.py              # gap report only
  RENTCAST_KEY=... uv run rentcast_fill.py
  HUD_TOKEN=... uv run rentcast_fill.py
  RENTCAST_KEY=... HUD_TOKEN=... uv run rentcast_fill.py
"""

import json
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

DB_PATH       = Path(os.environ.get("DB_PATH", Path(__file__).parent / "properties.db"))
RENTCAST_KEY  = os.environ.get("RENTCAST_KEY")
HUD_TOKEN     = os.environ.get("HUD_TOKEN")

RENTCAST_MARKETS_URL = "https://api.rentcast.io/v1/markets"
HUD_FMR_URL          = "https://www.huduser.gov/hudapi/public/fmr/statedata/NJ"

# How many top gaps to fetch (Rentcast counts per unique zip, not per beds/baths).
TOP_N = 20

# Monthly Rentcast API call budget. Each unique zip = 1 call.
RENTCAST_MONTHLY_LIMIT = 10_000


def _round_half(v):
    return round((v or 1.0) * 2) / 2.0


def build_gap_groups(con):
    """Return dict of (postal_code, beds, baths) -> count across all active
    for-sale properties, with multi-family expanded to individual units.
    """
    rows = con.execute(
        """
        SELECT postal_code, bedrooms, baths_total, baths_full,
               num_units, beds_per_unit_json, baths_per_unit_json
        FROM properties
        WHERE status='for_sale' AND is_active=1 AND postal_code IS NOT NULL
        """
    ).fetchall()

    groups = defaultdict(int)
    for r in rows:
        postal_code = r[0]
        beds        = r[1]
        baths       = r[2] if r[2] is not None else r[3]
        num_units   = r[4] or 1

        try:
            bpu = json.loads(r[5] or "[]")
            bau = json.loads(r[6] or "[]")
        except (ValueError, TypeError):
            bpu, bau = [], []

        if num_units > 1 and bpu and bau and len(bpu) == num_units:
            for b, ba in zip(bpu, bau):
                groups[(postal_code, b, _round_half(ba))] += 1
        elif beds is not None:
            groups[(postal_code, beds, _round_half(baths))] += 1

    return groups


def covered_by_rent_comps(con):
    """Return set of (postal_code, beds, baths) groups where a matching
    city-specific rent_comps row exists for at least one property in that zip.
    """
    # Join properties to rent_comps via city to find covered (zip, beds, baths).
    rows = con.execute(
        """
        SELECT DISTINCT p.postal_code, rc.bedrooms, rc.baths
        FROM rent_comps rc
        JOIN properties p ON p.city = rc.city
        WHERE rc.city IS NOT NULL AND p.status='for_sale' AND p.is_active=1
        """
    ).fetchall()
    return {(r[0], r[1], r[2]) for r in rows}


def already_in_external(con):
    """Return set of (postal_code, beds, baths) that already have a Rentcast estimate.
    HUD FMR estimates are excluded so Rentcast can still fill those groups with
    more precise market data.
    """
    rows = con.execute(
        "SELECT DISTINCT postal_code, bedrooms, baths FROM external_rent_estimates WHERE source='rentcast'"
    ).fetchall()
    return {(r[0], r[1], r[2]) for r in rows}


def _ensure_call_log(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS rentcast_call_log (
            postal_code TEXT NOT NULL,
            called_at   TEXT NOT NULL
        )
        """
    )


def rentcast_calls_this_month(con):
    """Count every Rentcast API call made this calendar month, including failures."""
    _ensure_call_log(con)
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    row = con.execute(
        "SELECT COUNT(*) FROM rentcast_call_log WHERE called_at LIKE ?",
        (month + "%",),
    ).fetchone()
    return row[0] if row else 0


def fetch_rentcast_zip(con, postal_code):
    """Fetch Rentcast market data for a zip. Logs the call regardless of outcome.
    Returns ({beds: median_rent}, {beds: raw_entry}) — the raw entries are
    surfaced so callers can cache the unprocessed payload in extra_info.
    """
    _ensure_call_log(con)
    now = datetime.now(timezone.utc).isoformat()
    with con:
        con.execute("INSERT INTO rentcast_call_log VALUES (?, ?)", (postal_code, now))
    headers = {"X-Api-Key": RENTCAST_KEY}
    try:
        r = requests.get(RENTCAST_MARKETS_URL, headers=headers,
                         params={"zipCode": postal_code, "historyRange": 1}, timeout=15)
        if r.status_code in (404, 400):
            return {}, {}
        r.raise_for_status()
        data = r.json()
        by_beds, raw_by_beds = {}, {}
        for entry in (data.get("rentalData") or {}).get("dataByBedrooms") or []:
            beds = entry.get("bedrooms")
            rent = entry.get("medianRent")
            if beds is not None and rent:
                by_beds[int(beds)] = float(rent)
                raw_by_beds[int(beds)] = entry
        return by_beds, raw_by_beds
    except Exception as e:
        print(f"  Rentcast error ({postal_code}): {e}")
        return {}, {}


def fetch_hud_fmr(con, gaps):
    """Fetch HUD FMR for NJ. Maps FMR bedroom counts to our gaps.
    HUD FMR provides 0-4 bedroom estimates at the county level; we approximate
    zip -> county via the county_fips column and assign the FMR for the closest
    bedroom count. Returns ({(postal_code, beds, baths): rent},
    {(postal_code, beds, baths): raw_county_payload}).
    """
    headers = {"Authorization": f"Bearer {HUD_TOKEN}"}
    try:
        r = requests.get(HUD_FMR_URL, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  HUD FMR fetch error: {e}")
        return {}, {}

    # Build county_fips (5-digit) -> fmr lookup: {county_fips: {beds: monthly_rent}}
    # HUD response uses named keys; fips_code is 10 digits, first 5 are standard FIPS.
    BED_KEYS = {"Efficiency": 0, "One-Bedroom": 1, "Two-Bedroom": 2,
                "Three-Bedroom": 3, "Four-Bedroom": 4}
    county_fmr = {}
    county_raw = {}
    for county in (data.get("data") or {}).get("counties") or []:
        fips = str(county.get("fips_code") or "")[:5]
        if not fips:
            continue
        fmr_by_beds = {}
        for label, idx in BED_KEYS.items():
            v = county.get(label)
            if v:
                fmr_by_beds[idx] = float(v)
        if fmr_by_beds:
            county_fmr[fips] = fmr_by_beds
            county_raw[fips] = county

    if not county_fmr:
        print("  HUD: no FMR data found in response")
        return {}, {}

    # Build zip -> 5-digit county_fips map from DB
    zip_county = {}
    rows = con.execute(
        "SELECT DISTINCT postal_code, county_fips FROM properties WHERE postal_code IS NOT NULL AND county_fips IS NOT NULL"
    ).fetchall()
    for postal_code, fips in rows:
        zip_county[postal_code] = str(fips)[:5]

    results = {}
    raw_by_key = {}
    for postal_code, beds, baths in gaps:
        fips = zip_county.get(postal_code)
        if not fips:
            continue
        fmr = county_fmr.get(fips)
        if not fmr:
            continue
        bed_key = min(int(beds), 4)
        rent = fmr.get(bed_key)
        if rent:
            results[(postal_code, beds, baths)] = rent  # already monthly
            raw_by_key[(postal_code, beds, baths)] = county_raw.get(fips)
    return results, raw_by_key


def store_estimates(con, estimates, source, raw_by_key=None):
    """Insert rent estimates. ``raw_by_key`` optionally maps
    (postal_code, beds, baths) -> raw API payload slice; stored in extra_info
    so the original response can be re-examined without another API call."""
    now = datetime.now(timezone.utc).isoformat()
    raw_by_key = raw_by_key or {}
    con.executemany(
        """
        INSERT OR REPLACE INTO external_rent_estimates
            (postal_code, bedrooms, baths, rent_estimate, source, fetched_at, extra_info)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (postal_code, beds, baths, rent, source, now,
             json.dumps(raw_by_key[(postal_code, beds, baths)])
             if (postal_code, beds, baths) in raw_by_key else None)
            for (postal_code, beds, baths), rent in estimates.items()
        ],
    )


def main():
    if os.environ.get("RENTCAST_FILL_ENABLE") != "1":
        raise SystemExit(
            "rentcast_fill.py is disabled. Set RENTCAST_FILL_ENABLE=1 to run it."
        )
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    # Ensure external_rent_estimates exists (created by fetch.py schema, but
    # may be missing on older DBs).
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS external_rent_estimates (
            postal_code   TEXT NOT NULL,
            bedrooms      INTEGER NOT NULL,
            baths         REAL NOT NULL,
            rent_estimate REAL NOT NULL,
            source        TEXT NOT NULL,
            fetched_at    TEXT NOT NULL,
            extra_info    TEXT,
            PRIMARY KEY (postal_code, bedrooms, baths, source)
        )
        """
    )
    from fetch import _ensure_extra_info
    _ensure_extra_info(con, "external_rent_estimates")
    con.commit()

    print("Analysing gaps...")
    groups   = build_gap_groups(con)
    covered  = covered_by_rent_comps(con)
    external = already_in_external(con)

    gaps = {
        k: v for k, v in groups.items()
        if k not in covered and k not in external
    }
    sorted_gaps = sorted(gaps.items(), key=lambda x: -x[1])

    print(f"\nTotal unserved groups: {len(gaps)}")
    print(f"{'Zip':>8}  {'Beds':>4}  {'Baths':>5}  {'Count':>6}")
    print("-" * 32)
    for (postal_code, beds, baths), cnt in sorted_gaps[:TOP_N]:
        print(f"{postal_code:>8}  {beds:>4}  {baths:>5.1f}  {cnt:>6}")

    top_gaps = [k for k, _ in sorted_gaps[:TOP_N]]

    # --- Rentcast ---
    rentcast_estimates = {}
    if RENTCAST_KEY and top_gaps:
        used = rentcast_calls_this_month(con)
        budget = RENTCAST_MONTHLY_LIMIT - used
        print(f"\nRentcast budget: {used}/{RENTCAST_MONTHLY_LIMIT} calls used this month, {budget} remaining.")
        if budget <= 0:
            print("Monthly limit reached -- skipping Rentcast fetch.")
        else:
            # Each unique zip = 1 API call; one call covers all bed counts for that zip.
            unique_zips = list(dict.fromkeys(z for z, _, _ in top_gaps))
            zips_to_fetch = unique_zips[:budget]
            if len(unique_zips) > budget:
                print(f"  Fetching {budget} of {len(unique_zips)} unique zips (budget limit).")
            else:
                print(f"  Fetching {len(zips_to_fetch)} unique zip(s)...")

            zip_data, zip_raw = {}, {}
            for postal_code in zips_to_fetch:
                by_beds, raw_by_beds = fetch_rentcast_zip(con, postal_code)
                zip_data[postal_code] = by_beds
                zip_raw[postal_code] = raw_by_beds
                hits = len(by_beds)
                print(f"  {postal_code}: {hits} bedroom bracket(s) returned")
                time.sleep(0.25)

            rentcast_raw = {}
            for postal_code, beds, baths in top_gaps:
                if postal_code not in zip_data:
                    continue
                rent = zip_data[postal_code].get(int(beds))
                if rent:
                    rentcast_estimates[(postal_code, beds, baths)] = rent
                    raw_entry = zip_raw.get(postal_code, {}).get(int(beds))
                    if raw_entry is not None:
                        rentcast_raw[(postal_code, beds, baths)] = raw_entry

            if rentcast_estimates:
                with con:
                    store_estimates(con, rentcast_estimates, "rentcast", rentcast_raw)
                print(f"Stored {len(rentcast_estimates)} Rentcast estimates across {len(zips_to_fetch)} zip(s).")
            else:
                print("No Rentcast estimates retrieved.")
    elif not RENTCAST_KEY:
        print("\nRENTCAST_KEY not set -- skipping Rentcast fetch.")

    # --- HUD FMR ---
    if HUD_TOKEN and top_gaps:
        already_fetched = rentcast_estimates if RENTCAST_KEY else {}
        hud_gaps = [k for k in top_gaps if k not in already_fetched]
        print(f"\nFetching HUD Fair Market Rents for {len(hud_gaps)} gaps...")
        hud_estimates, hud_raw = fetch_hud_fmr(con, hud_gaps)
        if hud_estimates:
            with con:
                store_estimates(con, hud_estimates, "hud_fmr", hud_raw)
            print(f"Stored {len(hud_estimates)} HUD FMR estimates.")
        else:
            print("No HUD FMR estimates retrieved.")
    elif not HUD_TOKEN:
        print("HUD_TOKEN not set -- skipping HUD FMR fetch.")

    con.close()


if __name__ == "__main__":
    main()
