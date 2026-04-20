"""Fill external_rent_estimates from Rentcast AVM and/or HUD Fair Market Rents.

Steps:
  1. Gap analysis — group all active for-sale properties by (postal_code, beds, baths),
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

RENTCAST_URL  = "https://api.rentcast.io/v1/avm/rent/long-term"
HUD_FMR_URL   = "https://www.huduser.gov/hudapi/public/fmr/statedata/NJ"

# How many top gaps to fetch from Rentcast (API calls cost money).
TOP_N = 20


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


def fetch_rentcast(postal_code, beds, baths):
    """Fetch Rentcast AVM long-term rent estimate. Returns float or None."""
    headers = {"X-Api-Key": RENTCAST_KEY}
    params = {
        "zipCode":   postal_code,
        "bedrooms":  beds,
        "bathrooms": baths,
        "propertyType": "Apartment",
    }
    try:
        r = requests.get(RENTCAST_URL, headers=headers, params=params, timeout=15)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
        return data.get("rent") or data.get("rentEstimate")
    except Exception as e:
        print(f"  Rentcast error ({postal_code} {beds}bd {baths}ba): {e}")
        return None


def fetch_hud_fmr(con, gaps):
    """Fetch HUD FMR for NJ. Maps FMR bedroom counts to our gaps.
    HUD FMR provides 0-4 bedroom estimates at the county level; we approximate
    zip → county via the county_fips column and assign the FMR for the closest
    bedroom count. Returns dict of (postal_code, beds, baths) -> rent.
    """
    headers = {"Authorization": f"Bearer {HUD_TOKEN}"}
    try:
        r = requests.get(HUD_FMR_URL, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  HUD FMR fetch error: {e}")
        return {}

    # Build county_fips -> fmr lookup: {county_fips: {beds: rent}}
    county_fmr = {}
    for county in (data.get("data") or {}).get("counties") or []:
        fips = county.get("fips_code") or county.get("countyCode")
        if not fips:
            continue
        basic = county.get("basicFMRs") or county.get("fmrs") or {}
        fmr_by_beds = {}
        # HUD keys: fmr_0, fmr_1, ..., fmr_4
        for i in range(5):
            v = basic.get(f"fmr_{i}") or basic.get(str(i))
            if v:
                fmr_by_beds[i] = float(v)
        if fmr_by_beds:
            county_fmr[str(fips)] = fmr_by_beds

    if not county_fmr:
        print("  HUD: no FMR data found in response")
        return {}

    # Build zip -> county_fips map from DB
    zip_county = {}
    rows = con.execute(
        "SELECT DISTINCT postal_code, county_fips FROM properties WHERE postal_code IS NOT NULL AND county_fips IS NOT NULL"
    ).fetchall()
    for postal_code, fips in rows:
        zip_county[postal_code] = str(fips)

    results = {}
    for postal_code, beds, baths in gaps:
        fips = zip_county.get(postal_code)
        if not fips:
            continue
        fmr = county_fmr.get(fips) or county_fmr.get(fips[:5])
        if not fmr:
            continue
        # HUD FMR beds cap at 4; clamp and pick nearest
        bed_key = min(int(beds), 4)
        rent = fmr.get(bed_key)
        if rent:
            results[(postal_code, beds, baths)] = rent / 12.0  # HUD is annual
    return results


def store_estimates(con, estimates, source):
    now = datetime.now(timezone.utc).isoformat()
    con.executemany(
        """
        INSERT OR REPLACE INTO external_rent_estimates
            (postal_code, bedrooms, baths, rent_estimate, source, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (postal_code, beds, baths, rent, source, now)
            for (postal_code, beds, baths), rent in estimates.items()
        ],
    )


def main():
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
            PRIMARY KEY (postal_code, bedrooms, baths, source)
        )
        """
    )
    con.commit()

    print("Analysing gaps…")
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
    if RENTCAST_KEY and top_gaps:
        print(f"\nFetching Rentcast AVM for top {len(top_gaps)} gaps…")
        rentcast_estimates = {}
        for postal_code, beds, baths in top_gaps:
            rent = fetch_rentcast(postal_code, beds, baths)
            if rent:
                print(f"  {postal_code} {beds}bd {baths}ba → ${rent:,.0f}/mo")
                rentcast_estimates[(postal_code, beds, baths)] = rent
            else:
                print(f"  {postal_code} {beds}bd {baths}ba → no data")
            time.sleep(0.25)  # stay within rate limits

        if rentcast_estimates:
            with con:
                store_estimates(con, rentcast_estimates, "rentcast")
            print(f"Stored {len(rentcast_estimates)} Rentcast estimates.")
        else:
            print("No Rentcast estimates retrieved.")
    elif not RENTCAST_KEY:
        print("\nRENTCAST_KEY not set — skipping Rentcast fetch.")

    # --- HUD FMR ---
    if HUD_TOKEN and top_gaps:
        already_fetched = rentcast_estimates if RENTCAST_KEY else {}
        hud_gaps = [k for k in top_gaps if k not in already_fetched]
        print(f"\nFetching HUD Fair Market Rents for {len(hud_gaps)} gaps…")
        hud_estimates = fetch_hud_fmr(con, hud_gaps)
        if hud_estimates:
            with con:
                store_estimates(con, hud_estimates, "hud_fmr")
            print(f"Stored {len(hud_estimates)} HUD FMR estimates.")
        else:
            print("No HUD FMR estimates retrieved.")
    elif not HUD_TOKEN:
        print("HUD_TOKEN not set — skipping HUD FMR fetch.")

    con.close()


if __name__ == "__main__":
    main()
