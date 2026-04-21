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

import argparse
import json
import os
import re
import sqlite3
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://realty-in-us.p.rapidapi.com/properties/v3/list"
DETAIL_URL = "https://realty-in-us.p.rapidapi.com/properties/v3/detail"
API_HOST = "realty-in-us.p.rapidapi.com"
DB_PATH = Path(os.environ.get("DB_PATH", Path(__file__).parent / "properties.db"))
PAGE_SIZE = 200
DETAIL_WORKERS = 5
DETAIL_MAX_RETRIES = 5
DETAIL_BACKOFF_BASE = 2.0  # seconds; doubled each retry

COUNTIES = [
    ("Essex",   "NJ"),
    ("Bergen",  "NJ"),
    ("Hudson",  "NJ"),
    ("Passaic", "NJ"),
    ("Morris",  "NJ"),
    ("Union",   "NJ"),
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

CREATE TABLE IF NOT EXISTS rent_comps (
    city           TEXT,
    bedrooms       INTEGER NOT NULL,
    baths          REAL NOT NULL,
    median_rent    REAL NOT NULL,
    sample_size    INTEGER NOT NULL,
    comp_ids_json  TEXT,
    PRIMARY KEY (city, bedrooms, baths)
);

CREATE TABLE IF NOT EXISTS external_rent_estimates (
    postal_code  TEXT NOT NULL,
    bedrooms     INTEGER NOT NULL,
    baths        REAL NOT NULL,
    rent_estimate REAL NOT NULL,
    source       TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    extra_info   TEXT,
    PRIMARY KEY (postal_code, bedrooms, baths, source)
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
    ("hoa_fee",               "INTEGER"),
    ("tract_fips",            "TEXT"),
    ("management_fee",        "REAL"),
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
    -- Merge new list-endpoint extra_info on top of whatever is there, so
    -- cached detail (extra_info.detail) survives a list refresh.
    extra_info    = json_patch(COALESCE(properties.extra_info, '{}'), excluded.extra_info),
    url           = COALESCE(excluded.url, properties.url)
;
"""


_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_properties_city   ON properties(city)",
    "CREATE INDEX IF NOT EXISTS idx_properties_status ON properties(status)",
    "CREATE INDEX IF NOT EXISTS idx_properties_price  ON properties(list_price)",
    "CREATE INDEX IF NOT EXISTS idx_properties_active ON properties(is_active)",
]


# Tables that receive API-derived data. `extra_info` caches the raw payload
# slice that produced the row so downstream code can re-extract fields later
# without another API call.
_EXTRA_INFO_TABLES = (
    "properties", "external_rent_estimates",
    "zip_demographics", "tract_demographics",
)


def _ensure_extra_info(con, table):
    """Idempotently add `extra_info TEXT` to a table that may predate the
    column (older DBs)."""
    if not con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone():
        return
    cols = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
    if "extra_info" not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN extra_info TEXT")


def migrate(con):
    """Add any missing columns to an existing properties table, then ensure
    indexes exist (indexes are created after migrate so columns exist first).
    Also ensures `extra_info` exists on every API-derived table so raw
    payloads can be cached consistently.
    """
    existing = {r[1] for r in con.execute("PRAGMA table_info(properties)")}
    for name, decl in _MIGRATION_COLUMNS:
        if name not in existing:
            con.execute(f"ALTER TABLE properties ADD COLUMN {name} {decl}")
    for stmt in _INDEXES:
        con.execute(stmt)
    for t in _EXTRA_INFO_TABLES:
        _ensure_extra_info(con, t)


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
        # Preserve the full list-endpoint payload under `list` so we can
        # re-extract fields later without another API call. Curated top-level
        # keys stay for existing json_extract queries (e.g. webapp's
        # `$.photo_count` filter).
        "extra_info":    json.dumps({
            "flags":         home.get("flags"),
            "open_houses":   home.get("open_houses"),
            "virtual_tours": home.get("virtual_tours"),
            "matterport":    home.get("matterport"),
            "photo_count":   home.get("photo_count"),
            "list":          home,
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
        "search_location": {"location": f"{county} County, {state_code}"},
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


# Coop maintenance / management fees are usually buried in description.text
# rather than the structured `hoa.fee` field. Regex-scan for "$N/mo",
# "$N monthly", "maintenance ... $N", etc. and take the largest plausible hit.
# "Total" patterns win when present since they capture the true all-in monthly
# (e.g., "TOTAL MONTHLY $1,978.65" summing HOA + assessments + cable).
_TOTAL_PATTERNS = [
    re.compile(r"(?:total|grand\s+total)[^$\n]{0,40}\$\s?([\d,]{2,7})", re.I),
    re.compile(r"\$\s?([\d,]{2,7})[^$\n]{0,20}(?:total|grand\s+total)", re.I),
]
_FEE_PATTERNS = [
    re.compile(r"\$\s?([\d,]{2,7})\s*(?:/|\s+per\s+|\s+a\s+)?\s*(?:mo(?:nth)?(?:ly)?)", re.I),
    re.compile(r"(?:maintenance|management|monthly\s+fee|coop\s+fee|co-?op\s+fee)"
               r"[^$\n]{0,30}\$\s?([\d,]{2,7})", re.I),
    re.compile(r"\$\s?([\d,]{2,7})[^$\n]{0,20}(?:maintenance|management)", re.I),
]


def _max_match(patterns, text):
    best = None
    for pat in patterns:
        for m in pat.finditer(text):
            try:
                val = float(m.group(1).replace(",", ""))
            except ValueError:
                continue
            if 100 <= val <= 10000 and (best is None or val > best):
                best = val
    return best


def extract_management_fee(text):
    if not text:
        return None
    total = _max_match(_TOTAL_PATTERNS, text)
    if total is not None:
        return total
    return _max_match(_FEE_PATTERNS, text)


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

    # Text heuristics ("duplex", "2-family") on single-unit property types like
    # coops/condos almost always refer to a bi-level unit, not a two-family
    # building. Trust list_units=1 for those types and only let detail override
    # when it comes from a structured signal (description.units / MLS type).
    structured_detail = detail_src in {"description_units", "mls_type", "unit_array"}
    if detail_units is not None and detail_units > 1 and (
        list_src != "property_type" or structured_detail
    ):
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

    hoa_fee = None
    hoa = detail.get("hoa") or {}
    if hoa.get("fee") is not None:
        try:
            hoa_fee = int(hoa["fee"])
        except (ValueError, TypeError):
            pass

    # Maintenance / management fees are often buried in description.text
    # instead of the structured `hoa.fee` field (especially for coops, but
    # also for some condos/townhomes). Scrape every listing and fold the
    # extracted fee into hoa_fee (max) so downstream analysis sees a single
    # combined monthly fee.
    desc_text = ((detail.get("description") or {}).get("text")) or ""
    management_fee = extract_management_fee(desc_text)
    if management_fee is not None:
        mf_int = int(management_fee)
        hoa_fee = mf_int if hoa_fee is None else max(hoa_fee, mf_int)

    return {
        "num_units":             num_units,
        "beds_per_unit_json":    json.dumps(beds_per_unit),
        "baths_per_unit_json":   json.dumps(baths_per_unit),
        "units_source":          source,
        "source_listing_status": sls,
        "hoa_fee":               hoa_fee,
        "management_fee":        management_fee,
    }


def fetch_detail(api_key, property_id):
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": API_HOST,
    }
    for attempt in range(DETAIL_MAX_RETRIES + 1):
        r = requests.get(DETAIL_URL, headers=headers,
                         params={"property_id": property_id}, timeout=30)
        if r.status_code == 429 and attempt < DETAIL_MAX_RETRIES:
            # Honor Retry-After when RapidAPI supplies it; else exponential backoff.
            retry_after = r.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else DETAIL_BACKOFF_BASE * (2 ** attempt)
            except ValueError:
                delay = DETAIL_BACKOFF_BASE * (2 ** attempt)
            time.sleep(delay)
            continue
        r.raise_for_status()
        data = r.json().get("data") or {}
        return data.get("home") or {}


# Property types that commonly carry HOA fees — always fetch detail for these
# so we can populate hoa_fee even when unit count is already known.
HOA_PRONE_TYPES = {"condos", "townhomes", "coop"}


def enrich_pending_details(con, api_key, refresh_existing=False):
    """Populate detail-derived fields (unit breakdown, hoa_fee, management_fee,
    source_listing_status). Uses the cached detail payload at
    ``extra_info.detail`` when present so no API call is needed; falls back to
    the detail endpoint otherwise.

    With ``refresh_existing=True``, also re-processes rows missing hoa_fee or
    management_fee so extraction-logic changes can backfill from cached data.
    """
    where_extra = ""
    if refresh_existing:
        where_extra = (
            " OR hoa_fee IS NULL"
            " OR management_fee IS NULL"
        )
    pending = con.execute(
        f"""
        SELECT property_id, property_type, sub_type, bedrooms,
               baths_full, baths_total, extra_info
        FROM properties
        WHERE is_active=1 AND status='for_sale'
          AND (detail_fetched_at IS NULL{where_extra})
        """
    ).fetchall()

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    enriched = 0

    def _load_extra(row_d):
        if not row_d.get("extra_info"):
            return {}
        try:
            return json.loads(row_d["extra_info"]) or {}
        except (ValueError, TypeError):
            return {}

    def _fetch(row):
        row_d = dict(row)
        extra = _load_extra(row_d)
        cached = extra.get("detail")
        if cached:
            return row_d, cached, None, extra, True
        try:
            return row_d, fetch_detail(api_key, row_d["property_id"]), None, extra, False
        except requests.RequestException as e:
            return row_d, None, e, extra, False

    try:
        with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
            futures = [ex.submit(_fetch, row) for row in pending]
            for fut in as_completed(futures):
                row_d, detail, err, extra, from_cache = fut.result()
                if err is not None:
                    print(f"  detail fetch failed for {row_d['property_id']}: {err}")
                    # Persist progress so far so a failure partway through
                    # doesn't lose already-enriched rows.
                    con.commit()
                    continue
                fields = parse_detail_payload(row_d, detail)
                if not from_cache:
                    extra["detail"] = detail

                update_hoa = "hoa_fee = :hoa_fee," if fields.get("hoa_fee") is not None else ""
                update_mgmt = "management_fee = :management_fee," if fields.get("management_fee") is not None else ""
                con.execute(
                    f"""
                    UPDATE properties SET
                        num_units             = :num_units,
                        beds_per_unit_json    = :beds_per_unit_json,
                        baths_per_unit_json   = :baths_per_unit_json,
                        units_source          = :units_source,
                        source_listing_status = :source_listing_status,
                        extra_info            = :extra_info,
                        {update_hoa}
                        {update_mgmt}
                        detail_fetched_at     = :detail_fetched_at
                    WHERE property_id = :property_id
                    """,
                    {**fields, "detail_fetched_at": now,
                     "property_id": row_d["property_id"],
                     "extra_info": json.dumps(extra)},
                )
                enriched += 1
                if enriched % 50 == 0:
                    con.commit()
    finally:
        # Flush on normal exit, interrupt, or exception so partial work is saved.
        con.commit()
    return enriched


def _top10_near_median(entries, med):
    """Return up to 10 property_ids whose price is closest to the median."""
    return [pid for _, pid in sorted(entries, key=lambda x: abs(x[0] - med))[:10]]


def build_rent_comps(con):
    """Bucket every for-rent listing by (city, bedrooms, round(baths)) and
    write the median rent + 10 nearest-to-median comp IDs into `rent_comps`.
    Also writes a city=NULL fallback row for each (beds, baths) bucket.
    """
    rentals = con.execute(
        """
        SELECT property_id, city, bedrooms,
               ROUND(COALESCE(baths_total, baths_full, 1) * 2) / 2.0 AS baths,
               list_price
        FROM properties
        WHERE status='for_rent' AND list_price IS NOT NULL AND list_price > 0
              AND list_price <= ?
              AND bedrooms IS NOT NULL AND is_active=1
        """,
        (MAX_COMP_RENT,),
    ).fetchall()

    by_city = defaultdict(list)  # (city, beds, baths) -> [(price, property_id)]
    by_any  = defaultdict(list)  # (beds, baths)       -> [(price, property_id)]
    for prop_id, city, beds, baths, price in rentals:
        by_city[(city, beds, baths)].append((price, prop_id))
        by_any[(beds, baths)].append((price, prop_id))

    rows = []
    for (city, beds, baths), entries in by_city.items():
        if len(entries) >= MIN_COMP_SAMPLES:
            prices = [p for p, _ in entries]
            med    = median(prices)
            rows.append((city, beds, baths, med, len(entries),
                         json.dumps(_top10_near_median(entries, med))))
    for (beds, baths), entries in by_any.items():
        if entries:
            prices = [p for p, _ in entries]
            med    = median(prices)
            rows.append((None, beds, baths, med, len(entries),
                         json.dumps(_top10_near_median(entries, med))))

    con.executemany(
        "INSERT OR REPLACE INTO rent_comps VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap total properties upserted across all counties")
    parser.add_argument("--per-county-limit", type=int, default=None,
                        help="Cap per-county fetch size (overrides MAX_PER_QUERY)")
    parser.add_argument("--refresh-detail", action="store_true",
                        help="Re-fetch detail for rows missing hoa_fee (condos/townhomes/coop) "
                             "even if detail_fetched_at is already set")
    parser.add_argument("--skip-detail", action="store_true",
                        help="Skip detail enrichment entirely")
    parser.add_argument("--counties", type=str, default=None,
                        help="Comma-separated county names to fetch (subset of COUNTIES). "
                             "When set, properties outside these counties keep their is_active flag.")
    args = parser.parse_args()

    api_key = os.environ.get("RAPIDAPI_KEY")
    if not api_key:
        raise SystemExit("RAPIDAPI_KEY environment variable is required")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    migrate(con)

    if args.counties:
        wanted = {c.strip().lower() for c in args.counties.split(",") if c.strip()}
        counties_to_fetch = [(c, s) for c, s in COUNTIES if c.lower() in wanted]
        if not counties_to_fetch:
            raise SystemExit(f"No counties from --counties matched COUNTIES: {args.counties}")
    else:
        counties_to_fetch = COUNTIES

    total_inserted = 0
    # When fetching the full COUNTIES list, mark every row stale so stale
    # listings get dropped. For a --counties subset, skip this: we'd mark
    # counties we're not refetching as inactive forever.
    if not args.counties:
        con.execute("UPDATE properties SET is_active=0")
        con.commit()

    def ingest(homes):
        nonlocal total_inserted
        added = 0
        for home in homes:
            row = flatten(home)
            if not row["property_id"]:
                continue
            con.execute(UPSERT, row)
            total_inserted += 1
            added += 1
            if args.limit is not None and total_inserted >= args.limit:
                return added, True
        return added, False

    county_cap = args.per_county_limit if args.per_county_limit is not None else MAX_PER_QUERY
    # Fetch every county's listings in parallel (IO-bound). SQLite writes stay
    # on the main thread as each future completes. --limit is enforced across
    # the combined results, so parallel fetches may overshoot slightly before
    # ingest trims.
    with ThreadPoolExecutor(max_workers=max(1, len(counties_to_fetch))) as ex:
        futures = {
            ex.submit(fetch_query, api_key, c, s, STATUSES, county_cap): (c, s)
            for c, s in counties_to_fetch
        }
        stop = False
        for fut in as_completed(futures):
            county, state = futures[fut]
            homes = fut.result()
            print(f"  {county} County, {state}: {len(homes)} rows")
            if stop:
                continue
            _, stop = ingest(homes)
            con.commit()

    if args.skip_detail:
        enriched = 0
        print("  (--skip-detail: skipping detail enrichment)")
    else:
        enriched = enrich_pending_details(con, api_key,
                                          refresh_existing=args.refresh_detail)
        con.commit()
    comp_rows = build_rent_comps(con)
    con.commit()

    active = con.execute("SELECT COUNT(*) FROM properties WHERE is_active=1").fetchone()[0]
    con.close()
    print(f"Upserted {total_inserted} listings ({active} active); "
          f"enriched {enriched} with detail; {comp_rows} rent comps -> {DB_PATH}")


if __name__ == "__main__":
    main()
