"""Fetch SimplyRETS listings and upsert them into a local SQLite database."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests

API_URL = "https://api.simplyrets.com/properties"
AUTH = ("simplyrets", "simplyrets")
DB_PATH = Path(__file__).parent / "properties.db"
PAGE_SIZE = 500

CORE_TOP_LEVEL = {
    "mlsId", "listPrice", "listDate", "modificationTimestamp",
    "mls", "property", "address", "geo", "remarks", "photos",
    "listingAgent", "office", "tax", "school",
}

CORE_MLS = {"status", "daysOnMarket"}
CORE_PROPERTY = {
    "type", "subType", "bedrooms", "bathsFull", "bathsHalf",
    "area", "yearBuilt", "lotSize", "stories",
}
CORE_ADDRESS = {"full", "streetName", "city", "state", "postalCode", "country"}
CORE_AGENT = {"firstName", "lastName"}
CORE_OFFICE = {"name"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS properties (
    mls_id          INTEGER PRIMARY KEY,
    list_price      INTEGER,
    list_date       TEXT,
    modification_ts TEXT,
    status          TEXT,
    days_on_market  INTEGER,
    property_type   TEXT,
    sub_type        TEXT,
    bedrooms        INTEGER,
    baths_full      INTEGER,
    baths_half      INTEGER,
    area_sqft       INTEGER,
    year_built      INTEGER,
    lot_size        REAL,
    stories         INTEGER,
    address_full    TEXT,
    street          TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     TEXT,
    country         TEXT,
    latitude        REAL,
    longitude       REAL,
    remarks            TEXT,
    agent_name         TEXT,
    office_name        TEXT,
    tax_id             TEXT,
    tax_annual_amount  REAL,
    tax_year           INTEGER,
    school_district    TEXT,
    elementary_school  TEXT,
    middle_school      TEXT,
    high_school        TEXT,
    photos_json        TEXT,
    extra_info         TEXT,
    fetched_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_properties_city   ON properties(city);
CREATE INDEX IF NOT EXISTS idx_properties_status ON properties(status);
CREATE INDEX IF NOT EXISTS idx_properties_price  ON properties(list_price);
"""

UPSERT = """
INSERT INTO properties (
    mls_id, list_price, list_date, modification_ts, status, days_on_market,
    property_type, sub_type, bedrooms, baths_full, baths_half, area_sqft,
    year_built, lot_size, stories, address_full, street, city, state,
    postal_code, country, latitude, longitude, remarks, agent_name,
    office_name, tax_id, tax_annual_amount, tax_year, school_district,
    elementary_school, middle_school, high_school, photos_json, extra_info,
    fetched_at
) VALUES (
    :mls_id, :list_price, :list_date, :modification_ts, :status, :days_on_market,
    :property_type, :sub_type, :bedrooms, :baths_full, :baths_half, :area_sqft,
    :year_built, :lot_size, :stories, :address_full, :street, :city, :state,
    :postal_code, :country, :latitude, :longitude, :remarks, :agent_name,
    :office_name, :tax_id, :tax_annual_amount, :tax_year, :school_district,
    :elementary_school, :middle_school, :high_school, :photos_json, :extra_info,
    :fetched_at
)
ON CONFLICT(mls_id) DO UPDATE SET
    list_price        = excluded.list_price,
    list_date         = excluded.list_date,
    modification_ts   = excluded.modification_ts,
    status            = excluded.status,
    days_on_market    = excluded.days_on_market,
    property_type     = excluded.property_type,
    sub_type          = excluded.sub_type,
    bedrooms          = excluded.bedrooms,
    baths_full        = excluded.baths_full,
    baths_half        = excluded.baths_half,
    area_sqft         = excluded.area_sqft,
    year_built        = excluded.year_built,
    lot_size          = excluded.lot_size,
    stories           = excluded.stories,
    address_full      = excluded.address_full,
    street            = excluded.street,
    city              = excluded.city,
    state             = excluded.state,
    postal_code       = excluded.postal_code,
    country           = excluded.country,
    latitude          = excluded.latitude,
    longitude         = excluded.longitude,
    remarks           = excluded.remarks,
    agent_name        = excluded.agent_name,
    office_name       = excluded.office_name,
    tax_id            = excluded.tax_id,
    tax_annual_amount = excluded.tax_annual_amount,
    tax_year          = excluded.tax_year,
    school_district   = excluded.school_district,
    elementary_school = excluded.elementary_school,
    middle_school     = excluded.middle_school,
    high_school       = excluded.high_school,
    photos_json       = excluded.photos_json,
    extra_info        = excluded.extra_info,
    fetched_at        = excluded.fetched_at
;
"""


def _leftover(src, core_keys):
    return {k: v for k, v in (src or {}).items() if k not in core_keys}


def flatten(listing):
    mls = listing.get("mls") or {}
    prop = listing.get("property") or {}
    addr = listing.get("address") or {}
    geo = listing.get("geo") or {}
    agent = listing.get("listingAgent") or {}
    office = listing.get("office") or {}
    tax = listing.get("tax") or {}
    school = listing.get("school") or {}

    agent_name = " ".join(
        p for p in (agent.get("firstName"), agent.get("lastName")) if p
    ) or None

    extra = {k: v for k, v in listing.items() if k not in CORE_TOP_LEVEL}
    nested_leftovers = {
        "mls": _leftover(mls, CORE_MLS),
        "property": _leftover(prop, CORE_PROPERTY),
        "address": _leftover(addr, CORE_ADDRESS),
        "listingAgent": _leftover(agent, CORE_AGENT),
        "office": _leftover(office, CORE_OFFICE),
    }
    for key, leftover in nested_leftovers.items():
        if leftover:
            extra[key] = leftover

    return {
        "mls_id": listing.get("mlsId"),
        "list_price": listing.get("listPrice"),
        "list_date": listing.get("listDate"),
        "modification_ts": listing.get("modificationTimestamp"),
        "status": mls.get("status"),
        "days_on_market": mls.get("daysOnMarket"),
        "property_type": prop.get("type"),
        "sub_type": prop.get("subType"),
        "bedrooms": prop.get("bedrooms"),
        "baths_full": prop.get("bathsFull"),
        "baths_half": prop.get("bathsHalf"),
        "area_sqft": prop.get("area"),
        "year_built": prop.get("yearBuilt"),
        "lot_size": prop.get("lotSize"),
        "stories": prop.get("stories"),
        "address_full": addr.get("full"),
        "street": addr.get("streetName"),
        "city": addr.get("city"),
        "state": addr.get("state"),
        "postal_code": addr.get("postalCode"),
        "country": addr.get("country"),
        "latitude": geo.get("lat"),
        "longitude": geo.get("lng"),
        "remarks": listing.get("remarks"),
        "agent_name": agent_name,
        "office_name": office.get("name"),
        "tax_id": tax.get("id"),
        "tax_annual_amount": tax.get("taxAnnualAmount"),
        "tax_year": tax.get("taxYear"),
        "school_district": school.get("district"),
        "elementary_school": school.get("elementarySchool"),
        "middle_school": school.get("middleSchool"),
        "high_school": school.get("highSchool"),
        "photos_json": json.dumps(listing.get("photos") or []),
        "extra_info": json.dumps(extra),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def fetch_all():
    last_id = None
    while True:
        params = {"limit": PAGE_SIZE}
        if last_id is not None:
            params["lastId"] = last_id
        r = requests.get(API_URL, auth=AUTH, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            return
        for item in batch:
            yield item
        if len(batch) < PAGE_SIZE:
            return
        new_last = batch[-1].get("mlsId")
        if new_last is None or new_last == last_id:
            return
        last_id = new_last


def main():
    con = sqlite3.connect(DB_PATH)
    con.executescript(SCHEMA)
    count = 0
    with con:
        for listing in fetch_all():
            row = flatten(listing)
            if row["mls_id"] is None:
                continue
            con.execute(UPSERT, row)
            count += 1
    con.close()
    print(f"Upserted {count} listings into {DB_PATH}")


if __name__ == "__main__":
    main()
