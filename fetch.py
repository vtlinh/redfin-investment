"""Fetch Realtor.com listings (via RapidAPI realty-in-us) and store in SQLite.

The `properties` table is **persistent** across runs: each run marks all rows
inactive, then UPSERTs set `is_active=1` and refresh `last_seen_at` for rows
seen this run. This preserves `detail_fetched_at` and per-unit breakdowns from
earlier runs so we only call the detail endpoint for properties we haven't
already looked at.

Multi-family unit detection uses a signal chain (cheapest signals first):
  1. sub_type literal on the list response — no extra call
  2. property_type classification — no extra call
  3. detail: description.units
  4. detail: MLS "Source Property Type" (e.g. "2 Family")
  5. detail: description.text regex ("two-family", "3 unit", ...)
  6. fallback default

Set RAPIDAPI_KEY in the environment before running.
"""

import json
import os
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://realty-in-us.p.rapidapi.com/properties/v3/list"
DETAIL_URL = "https://realty-in-us.p.rapidapi.com/properties/v3/detail"
API_HOST = "realty-in-us.p.rapidapi.com"
DB_PATH = Path(__file__).parent / "properties.db"
PAGE_SIZE = 200

COUNTIES = [
    ("Essex",  "NJ"),
    ("Bergen", "NJ"),
]
STATUSES = ["for_sale", "ready_to_build", "for_rent"]
MAX_PER_QUERY = 10000

MIN_COMP_SAMPLES = 3

# Rental listings above this *monthly* price are excluded from comp calculations
# (luxury/penthouse outliers that skew medians for normal rentals). `list_price`
# on for_rent rows is the monthly asking rent, so the threshold is in dollars/month.
MAX_COMP_RENT = 10_000

MULTI_FAMILY_TYPES = {"multi_family", "duplex_triplex_quadplex"}

# sub_type string → unit count (no extra API call required)
SUB_TYPE_UNITS = {
    "duplex":    2,
    "triplex":   3,
    "quadplex":  4,
    "fourplex":  4,
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS properties (
    property_id         TEXT PRIMARY KEY,
    listing_id          TEXT,
    status              TEXT,
    list_price          INTEGER,
    list_date           TEXT,
    last_update         TEXT,
    property_type       TEXT,
    sub_type            TEXT,
    bedrooms            INTEGER,
    baths_full          INTEGER,
    baths_half          INTEGER,
    baths_total         REAL,
    area_sqft           INTEGER,
    lot_sqft            INTEGER,
    year_built          INTEGER,
    stories             INTEGER,
    address_line        TEXT,
    city                TEXT,
    state               TEXT,
    postal_code         TEXT,
    latitude            REAL,
    longitude           REAL,
    county_fips         TEXT,
    hoa_fee             INTEGER,
    agent_name          TEXT,
    office_name         TEXT,
    primary_photo       TEXT,
    url                 TEXT,
    tags_json           TEXT,
    extra_info          TEXT,
    fetched_at          TEXT NOT NULL,
    last_seen_at        TEXT,
    is_active           INTEGER DEFAULT 0,
    num_units           INTEGER,
    beds_per_unit_json  TEXT,
    baths_per_unit_json TEXT,
    units_source        TEXT,
    source_listing_status TEXT,
    detail_fetched_at   TEXT,
    is_pending          INTEGER,
    is_contingent       INTEGER
);

DROP TABLE IF EXISTS rent_comps;
CREATE TABLE rent_comps (
    city         TEXT,
    bedrooms     INTEGER NOT NULL,
    baths        INTEGER NOT NULL,
    median_rent  REAL NOT NULL,
    sample_size  INTEGER NOT NULL,
    PRIMARY KEY (city, bedrooms, baths)
);
"""

# Columns added after initial schema — ALTER existing DBs on startup.
_MIGRATION_COLUMNS = [
    ("last_seen_at",          "TEXT"),
    ("is_active",             "INTEGER DEFAULT 0"),
    ("num_units",             "INTEGER"),
    ("beds_per_unit_json",    "TEXT"),
    ("baths_per_unit_json",   "TEXT"),
    ("units_source",          "TEXT"),
    ("source_listing_status", "TEXT"),
    ("detail_fetched_at",     "TEXT"),
    ("is_pending",            "INTEGER"),
    ("is_contingent",         "INTEGER"),
    ("url",                   "TEXT"),
]

# UPSERT preserves detail-derived columns (num_units, *_per_unit_json,
# units_source, source_listing_status, detail_fetched_at) on conflict so we
# don't re-fetch detail for listings we've already inspected.
UPSERT = """
INSERT INTO properties (
    property_id, listing_id, status, list_price, list_date, last_update,
    property_type, sub_type, bedrooms, baths_full, baths_half, baths_total,
    area_sqft, lot_sqft, year_built, stories, address_line, city, state,
    postal_code, latitude, longitude, county_fips, hoa_fee, agent_name,
    office_name, primary_photo, url, tags_json, extra_info, fetched_at,
    last_seen_at, is_active, is_pending, is_contingent
) VALUES (
    :property_id, :listing_id, :status, :list_price, :list_date, :last_update,
    :property_type, :sub_type, :bedrooms, :baths_full, :baths_half, :baths_total,
    :area_sqft, :lot_sqft, :year_built, :stories, :address_line, :city, :state,
    :postal_code, :latitude, :longitude, :county_fips, :hoa_fee, :agent_name,
    :office_name, :primary_photo, :url, :tags_json, :extra_info, :fetched_at,
    :fetched_at, 1, :is_pending, :is_contingent
)
ON CONFLICT(property_id) DO UPDATE SET
    status        = excluded.status,
    list_price    = excluded.list_price,
    last_update   = excluded.last_update,
    fetched_at    = excluded.fetched_at,
    last_seen_at  = excluded.fetched_at,
    is_active     = 1,
    is_pending    = excluded.is_pending,
    is_contingent = excluded.is_contingent,
    url           = COALESCE(excluded.url, properties.url)
;
"""


_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_properties_city   ON properties(city)",
    "CREATE INDEX IF NOT EXISTS idx_properties_status ON properties(status)",
    "CREATE INDEX IF NOT EXISTS idx_properties_price  ON properties(list_price)",
    "CREATE INDEX IF NOT EXISTS idx_properties_active ON properties(is_active)",
]


def migrate(con):
    """Add any missing columns to an existing properties table, then ensure
    indexes exist (indexes are created after migrate so columns exist first).
    """
    existing = {r[1] for r in con.execute("PRAGMA table_info(properties)")}
    for name, decl in _MIGRATION_COLUMNS:
        if name not in existing:
            con.execute(f"ALTER TABLE properties ADD COLUMN {name} {decl}")
    for stmt in _INDEXES:
        con.execute(stmt)


def flatten(home):
    loc = home.get("location") or {}
    addr = loc.get("address") or {}
    coord = addr.get("coordinate") or {}
    county = loc.get("county") or {}
    desc = home.get("description") or {}
    hoa = home.get("hoa") or {}
    photo = home.get("primary_photo") or {}
    advertisers = home.get("advertisers") or []
    branding = home.get("branding") or []
    flags = home.get("flags") or {}

    agent_name = advertisers[0].get("name") if advertisers else None
    office_name = branding[0].get("name") if branding else None

    return {
        "property_id":   home.get("property_id"),
        "listing_id":    home.get("listing_id"),
        "status":        home.get("status"),
        "list_price":    home.get("list_price"),
        "list_date":     home.get("list_date"),
        "last_update":   home.get("last_update_date"),
        "property_type": desc.get("type"),
        "sub_type":      desc.get("sub_type"),
        "bedrooms":      desc.get("beds"),
        "baths_full":    desc.get("baths_full"),
        "baths_half":    desc.get("baths_half"),
        "baths_total":   desc.get("baths"),
        "area_sqft":     desc.get("sqft"),
        "lot_sqft":      desc.get("lot_sqft"),
        "year_built":    desc.get("year_built"),
        "stories":       desc.get("stories"),
        "address_line":  addr.get("line"),
        "city":          addr.get("city"),
        "state":         addr.get("state_code"),
        "postal_code":   addr.get("postal_code"),
        "latitude":      coord.get("lat"),
        "longitude":     coord.get("lon"),
        "county_fips":   county.get("fips_code"),
        "hoa_fee":       hoa.get("fee"),
        "agent_name":    agent_name,
        "office_name":   office_name,
        "primary_photo": photo.get("href"),
        "url":           home.get("href"),
        "tags_json":     json.dumps(home.get("tags") or []),
        "extra_info":    json.dumps({
            "flags":         home.get("flags"),
            "open_houses":   home.get("open_houses"),
            "virtual_tours": home.get("virtual_tours"),
            "matterport":    home.get("matterport"),
            "photo_count":   home.get("photo_count"),
        }),
        "fetched_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "is_pending":    1 if flags.get("is_pending") else 0,
        "is_contingent": 1 if flags.get("is_contingent") else 0,
    }


def fetch_page(api_key, county, state_code, status, limit, offset):
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": API_HOST,
        "Content-Type": "application/json",
    }
    payload = {
        "limit": limit,
        "offset": offset,
        "county": county,
        "state_code": state_code,
        "status": status,
        "sort": {"direction": "desc", "field": "list_date"},
    }
    r = requests.post(API_URL, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json().get("data") or {}
    hs = data.get("home_search") or {}
    return hs.get("results") or [], hs.get("total") or 0


def fetch_query(api_key, county, state_code, status, max_rows):
    collected, offset = [], 0
    while len(collected) < max_rows:
        remaining = max_rows - len(collected)
        page, total = fetch_page(api_key, county, state_code, status,
                                 min(PAGE_SIZE, remaining), offset)
        if not page:
            break
        collected.extend(page)
        offset += len(page)
        if offset >= total:
            break
    return collected


# --- Unit detection --------------------------------------------------------

_TEXT_PATTERNS = [
    (re.compile(r"\b(two|2)[-\s]?family\b",      re.I), 2),
    (re.compile(r"\b(three|3)[-\s]?family\b",    re.I), 3),
    (re.compile(r"\b(four|4)[-\s]?family\b",     re.I), 4),
    (re.compile(r"\b(five|5)[-\s]?family\b",     re.I), 5),
    (re.compile(r"\b(6|six)[-\s]?family\b",      re.I), 6),
    # Named multi-family types — appear in descriptions even when the listing
    # is mis-classified as single_family (e.g. "duplex with flexible space").
    (re.compile(r"\bduplex\b",                   re.I), 2),
    (re.compile(r"\btriplex\b",                  re.I), 3),
    (re.compile(r"\b(quadplex|fourplex|four[-\s]?plex)\b", re.I), 4),
    # Worded unit counts — "two-unit", "three-unit", ...
    (re.compile(r"\b(two|2)[-\s]?unit\b",        re.I), 2),
    (re.compile(r"\b(three|3)[-\s]?unit\b",      re.I), 3),
    (re.compile(r"\b(four|4)[-\s]?unit\b",       re.I), 4),
    (re.compile(r"\b(five|5)[-\s]?unit\b",       re.I), 5),
    (re.compile(r"\b(\d+)\s*[-\s]?unit\b",       re.I), None),  # capture group
]

# single_family listings with at least this many bedrooms are treated as
# potentially mis-classified multi-family (e.g. 6-bed "single_family" that is
# actually a duplex) and get a detail fetch instead of the property_type shortcut.
SUSPECT_SFH_MIN_BEDS = 5

_MLS_FAMILY_RE = re.compile(r"(\d+)\s*[-\s]?family", re.I)
_MLS_UNIT_RE   = re.compile(r"(\d+)\s*[-\s]?unit",   re.I)


def units_from_sub_type(sub_type):
    if not sub_type:
        return None
    return SUB_TYPE_UNITS.get(sub_type.lower())


def units_from_list_row(row):
    """Try to determine unit count using only data from v3/list (no extra call).

    Returns (units, source) or (None, None) if detail call is needed.
    Large single_family listings (bedrooms >= SUSPECT_SFH_MIN_BEDS) are treated
    as suspect — they frequently turn out to be mis-classified multi-family
    where the description says "duplex" / "two-unit" — so we force a detail
    fetch instead of trusting the property_type.
    """
    pt = row["property_type"]
    st = row["sub_type"]
    n = units_from_sub_type(st)
    if n is not None:
        return n, "sub_type"
    if pt and pt not in MULTI_FAMILY_TYPES:
        beds = row["bedrooms"] if "bedrooms" in row.keys() else None
        if pt == "single_family" and beds is not None and beds >= SUSPECT_SFH_MIN_BEDS:
            return None, None
        return 1, "property_type"
    return None, None


def units_from_detail(detail):
    """Signal chain inside detail payload. Returns (units, source) or (None, None)."""
    desc = (detail.get("description") or {})

    # 1. description.units — explicit field when present
    u = desc.get("units")
    if isinstance(u, int) and u >= 1:
        return u, "description_units"

    # 2. MLS "Source Property Type" — e.g. "2 Family", "Residential - 3 Family"
    for key in ("source_type", "source_property_type", "type"):
        val = detail.get(key) or desc.get(key)
        if isinstance(val, str):
            m = _MLS_FAMILY_RE.search(val) or _MLS_UNIT_RE.search(val)
            if m:
                return int(m.group(1)), "mls_type"

    # 3. unit_count_summary / units array
    for key in ("unit_count_summary", "units_summary", "unit_summary"):
        arr = detail.get(key) or desc.get(key)
        if isinstance(arr, list) and arr:
            return len(arr), "unit_array"

    # 4. description.text free-text regex
    text = desc.get("text") or ""
    for pat, fixed in _TEXT_PATTERNS:
        m = pat.search(text)
        if m:
            if fixed is not None:
                return fixed, "text_heuristic"
            try:
                val = int(m.group(1))
                if 1 <= val <= 20:
                    return val, "text_heuristic"
            except (ValueError, IndexError):
                pass

    return None, None


def parse_detail_payload(row, detail):
    """Combine list-row signals with detail-endpoint signals into a single
    set of unit fields. Returns dict with num_units, beds_per_unit_json,
    baths_per_unit_json, units_source, source_listing_status.
    """
    # Prefer any non-default signal that asserts a unit count. Detail text
    # signals (MLS type, description.units, "duplex"/"two-unit" wording) take
    # precedence over the list-row property_type=single_family shortcut when
    # they indicate >1 units, since the source data's classification is often
    # wrong for small multi-family homes.
    list_units, list_src = units_from_list_row(row)
    detail_units, detail_src = units_from_detail(detail)

    if detail_units is not None and detail_units > 1:
        num_units = detail_units
        source = detail_src
    elif list_units is not None:
        num_units = list_units
        source = list_src
    elif detail_units is not None:
        num_units = detail_units
        source = detail_src
    else:
        # fallback: multi-family gets 2; otherwise 1
        num_units = 2 if row["property_type"] in MULTI_FAMILY_TYPES else 1
        source = "default"

    # Multi-family listings always have ≥2 units by definition; if any signal
    # collapsed to 1 for a row classified as multi-family, force it back up.
    if row["property_type"] in MULTI_FAMILY_TYPES and num_units < 2:
        num_units = 2
        source = f"{source}+mf_floor"

    beds = row["bedrooms"]
    baths = row["baths_total"] if row["baths_total"] is not None else row["baths_full"]
    if num_units >= 1 and beds is not None:
        base = beds // num_units
        extra = beds - base * num_units
        beds_per_unit = [base + (1 if i < extra else 0) for i in range(num_units)]
    else:
        beds_per_unit = []
    if num_units >= 1 and baths is not None:
        b = baths / num_units
        baths_per_unit = [round(b, 1)] * num_units
    else:
        baths_per_unit = []

    # Source listing status from detail (e.g. "Active", "Pending", "Under Contract")
    sls = None
    for key in ("source_listing_status", "listing_status", "mls_status"):
        v = detail.get(key)
        if isinstance(v, str) and v:
            sls = v
            break

    return {
        "num_units":             num_units,
        "beds_per_unit_json":    json.dumps(beds_per_unit),
        "baths_per_unit_json":   json.dumps(baths_per_unit),
        "units_source":          source,
        "source_listing_status": sls,
    }


def fetch_detail(api_key, property_id):
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": API_HOST,
    }
    r = requests.get(DETAIL_URL, headers=headers,
                     params={"property_id": property_id}, timeout=30)
    r.raise_for_status()
    data = r.json().get("data") or {}
    return data.get("home") or {}


def enrich_pending_details(con, api_key):
    """Call the detail endpoint for every active for-sale listing that we
    haven't already detailed. Multi-family rows get unit breakdowns; all
    rows get `source_listing_status`. Returns the count of rows enriched.
    """
    pending = con.execute(
        """
        SELECT property_id, property_type, sub_type, bedrooms,
               baths_full, baths_total
        FROM properties
        WHERE is_active=1 AND status='for_sale' AND detail_fetched_at IS NULL
        """
    ).fetchall()

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    enriched = 0
    for row in pending:
        row_d = dict(row)
        # Shortcut: non-multi-family rows we can resolve without the call.
        list_units, list_src = units_from_list_row(row_d)
        if list_units is not None and list_src == "property_type":
            fields = parse_detail_payload(row_d, {})
            fields["num_units"] = 1
            fields["units_source"] = "property_type"
            fields["source_listing_status"] = None
        else:
            try:
                detail = fetch_detail(api_key, row_d["property_id"])
            except requests.RequestException as e:
                print(f"  detail fetch failed for {row_d['property_id']}: {e}")
                continue
            fields = parse_detail_payload(row_d, detail)

        con.execute(
            """
            UPDATE properties SET
                num_units             = :num_units,
                beds_per_unit_json    = :beds_per_unit_json,
                baths_per_unit_json   = :baths_per_unit_json,
                units_source          = :units_source,
                source_listing_status = :source_listing_status,
                detail_fetched_at     = :detail_fetched_at
            WHERE property_id = :property_id
            """,
            {**fields, "detail_fetched_at": now, "property_id": row_d["property_id"]},
        )
        enriched += 1
    return enriched


def build_rent_comps(con):
    """Bucket every for-rent listing by (city, bedrooms, round(baths)) and
    write the median rent per bucket into `rent_comps`. Also writes a
    city=NULL fallback row for each (beds, baths) bucket.
    """
    rentals = con.execute(
        """
        SELECT city, bedrooms,
               CAST(ROUND(COALESCE(baths_total, baths_full, 1)) AS INTEGER) AS baths,
               list_price
        FROM properties
        WHERE status='for_rent' AND list_price IS NOT NULL AND list_price > 0
              AND list_price <= ?
              AND bedrooms IS NOT NULL AND is_active=1
        """,
        (MAX_COMP_RENT,),
    ).fetchall()

    by_city = defaultdict(list)
    by_any = defaultdict(list)
    for city, beds, baths, price in rentals:
        by_city[(city, beds, baths)].append(price)
        by_any[(beds, baths)].append(price)

    rows = []
    for (city, beds, baths), prices in by_city.items():
        if len(prices) >= MIN_COMP_SAMPLES:
            rows.append((city, beds, baths, median(prices), len(prices)))
    for (beds, baths), prices in by_any.items():
        if prices:
            rows.append((None, beds, baths, median(prices), len(prices)))

    con.executemany(
        "INSERT OR REPLACE INTO rent_comps VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def main():
    api_key = os.environ.get("RAPIDAPI_KEY")
    if not api_key:
        raise SystemExit("RAPIDAPI_KEY environment variable is required")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    migrate(con)

    total_inserted = 0
    with con:
        # Mark every row stale; UPSERTs below re-flag seen rows as active.
        con.execute("UPDATE properties SET is_active=0")

        for county, state in COUNTIES:
            homes = fetch_query(api_key, county, state, STATUSES, MAX_PER_QUERY)
            for home in homes:
                row = flatten(home)
                if not row["property_id"]:
                    continue
                con.execute(UPSERT, row)
                total_inserted += 1
            print(f"  {county} County, {state}: {len(homes)} rows")

        enriched = enrich_pending_details(con, api_key)
        comp_rows = build_rent_comps(con)

    active = con.execute("SELECT COUNT(*) FROM properties WHERE is_active=1").fetchone()[0]
    con.close()
    print(f"Upserted {total_inserted} listings ({active} active); "
          f"enriched {enriched} with detail; {comp_rows} rent comps -> {DB_PATH}")


if __name__ == "__main__":
    main()
