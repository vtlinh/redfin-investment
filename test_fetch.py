"""Tests for fetch.flatten and fetch.build_rent_comps."""

import json
import sqlite3

import fetch
from fetch import flatten


SAMPLE = {
    "property_id": "5578691819",
    "listing_id": "2994102572",
    "status": "for_sale",
    "list_price": 525000,
    "list_date": "2026-04-01T12:00:00Z",
    "last_update_date": "2026-04-10T08:00:00Z",
    "photo_count": 36,
    "matterport": False,
    "virtual_tours": None,
    "open_houses": None,
    "flags": {"is_new_listing": False, "is_contingent": None, "is_pending": True},
    "tags": ["central_air", "garage"],
    "branding": [{"name": "NJ METRO GROUP"}],
    "advertisers": [{"name": "Jessica Keefe, Agent"}],
    "primary_photo": {"href": "https://example.com/photo.jpg"},
    "href": "https://www.realtor.com/realestateandhomes-detail/31-33-Smallwood-Ave_Belleville_NJ_07109_M55786-91819",
    "hoa": {"fee": 150},
    "location": {
        "address": {
            "line": "31-33 Smallwood Ave",
            "city": "Belleville",
            "state_code": "NJ",
            "state": "New Jersey",
            "postal_code": "07109",
            "coordinate": {"lat": 40.79, "lon": -74.17},
        },
        "county": {"fips_code": "34013"},
    },
    "description": {
        "type": "single_family",
        "sub_type": None,
        "beds": 3,
        "baths": 3,
        "baths_full": 2,
        "baths_half": 1,
        "sqft": 1850,
        "lot_sqft": 4000,
        "year_built": 1935,
        "stories": 2,
    },
}


def test_flatten_core_fields():
    row = flatten(SAMPLE)
    assert row["property_id"] == "5578691819"
    assert row["status"] == "for_sale"
    assert row["list_price"] == 525000
    assert row["city"] == "Belleville"
    assert row["state"] == "NJ"
    assert row["postal_code"] == "07109"
    assert row["latitude"] == 40.79
    assert row["longitude"] == -74.17
    assert row["bedrooms"] == 3
    assert row["baths_full"] == 2
    assert row["baths_half"] == 1
    assert row["area_sqft"] == 1850
    assert row["year_built"] == 1935
    assert row["agent_name"] == "Jessica Keefe, Agent"
    assert row["office_name"] == "NJ METRO GROUP"
    assert row["hoa_fee"] == 150
    assert row["county_fips"] == "34013"
    assert row["primary_photo"] == "https://example.com/photo.jpg"
    assert row["url"] == "https://www.realtor.com/realestateandhomes-detail/31-33-Smallwood-Ave_Belleville_NJ_07109_M55786-91819"
    assert row["fetched_at"]  # non-empty ISO timestamp


def test_flatten_promotes_pending_and_contingent_flags():
    row = flatten(SAMPLE)
    assert row["is_pending"] == 1
    assert row["is_contingent"] == 0


def test_flatten_defaults_flags_to_zero_when_missing():
    row = flatten({"property_id": "x"})
    assert row["is_pending"] == 0
    assert row["is_contingent"] == 0


def test_flatten_tags_and_extra_info_are_json():
    row = flatten(SAMPLE)
    assert json.loads(row["tags_json"]) == ["central_air", "garage"]
    extra = json.loads(row["extra_info"])
    assert extra["photo_count"] == 36
    assert extra["flags"] == {"is_new_listing": False, "is_contingent": None, "is_pending": True}


def test_flatten_handles_missing_nested_objects():
    minimal = {"property_id": "x", "status": "for_rent", "list_price": 2100}
    row = flatten(minimal)
    assert row["property_id"] == "x"
    assert row["list_price"] == 2100
    assert row["city"] is None
    assert row["bedrooms"] is None
    assert row["agent_name"] is None
    assert row["office_name"] is None
    assert json.loads(row["tags_json"]) == []


def _build_rentals_db(rows):
    """In-memory DB with just the columns build_rent_comps touches."""
    con = sqlite3.connect(":memory:")
    con.executescript(fetch.SCHEMA)  # creates properties + rent_comps
    for r in rows:
        con.execute(
            "INSERT INTO properties (property_id, status, list_price, city, "
            "bedrooms, baths_full, baths_total, fetched_at, is_active) "
            "VALUES (?, 'for_rent', ?, ?, ?, ?, ?, '2026-01-01T00:00:00', 1)",
            r,
        )
    return con


def test_build_rent_comps_writes_city_and_fallback_rows():
    # Three Belleville 2-bed/1-bath rentals: median = 2200
    # Two Nutley 2-bed/1-bath rentals (below MIN_COMP_SAMPLES of 3): no local row
    rows = [
        ("b1", 2000, "Belleville", 2, 1, 1.0),
        ("b2", 2200, "Belleville", 2, 1, 1.0),
        ("b3", 2400, "Belleville", 2, 1, 1.0),
        ("n1", 2600, "Nutley",     2, 1, 1.0),
        ("n2", 2800, "Nutley",     2, 1, 1.0),
    ]
    con = _build_rentals_db(rows)
    fetch.build_rent_comps(con)

    # Belleville has its own row (3 samples, median 2200)
    r = con.execute(
        "SELECT median_rent, sample_size FROM rent_comps WHERE city='Belleville' AND bedrooms=2 AND baths=1"
    ).fetchone()
    assert r == (2200.0, 3)

    # Nutley has too few samples → no city-specific row written
    assert con.execute(
        "SELECT 1 FROM rent_comps WHERE city='Nutley' AND bedrooms=2 AND baths=1"
    ).fetchone() is None

    # Fallback row (city IS NULL) pools all 5 rentals: median 2400
    r = con.execute(
        "SELECT median_rent, sample_size FROM rent_comps WHERE city IS NULL AND bedrooms=2 AND baths=1"
    ).fetchone()
    assert r == (2400.0, 5)


def test_units_from_list_row_subtype_wins():
    # sub_type gives an exact count — no detail call needed.
    row = {"property_type": "duplex_triplex_quadplex", "sub_type": "triplex"}
    assert fetch.units_from_list_row(row) == (3, "sub_type")


def test_units_from_list_row_non_multi_family():
    row = {"property_type": "single_family", "sub_type": None}
    assert fetch.units_from_list_row(row) == (1, "property_type")


def test_units_from_list_row_multi_family_needs_detail():
    row = {"property_type": "multi_family", "sub_type": None}
    assert fetch.units_from_list_row(row) == (None, None)


def test_units_from_detail_explicit_field():
    assert fetch.units_from_detail({"description": {"units": 3}}) == (3, "description_units")


def test_units_from_detail_mls_type_regex():
    # MLS "Source Property Type: 2 Family" pattern
    detail = {"source_type": "Residential - 2 Family"}
    assert fetch.units_from_detail(detail) == (2, "mls_type")


def test_units_from_detail_text_heuristic():
    detail = {"description": {"text": "Spacious three-family home with parking."}}
    assert fetch.units_from_detail(detail) == (3, "text_heuristic")


def test_units_from_detail_returns_none_when_no_signal():
    assert fetch.units_from_detail({"description": {"text": "A lovely home."}}) == (None, None)


def test_parse_detail_payload_splits_beds_across_units():
    row = {"property_type": "multi_family", "sub_type": None,
           "bedrooms": 5, "baths_total": 3.0, "baths_full": 3}
    # detail says 2 units → 5 beds split as [3, 2]
    result = fetch.parse_detail_payload(row, {"description": {"units": 2}})
    assert result["num_units"] == 2
    assert json.loads(result["beds_per_unit_json"]) == [3, 2]
    assert result["units_source"] == "description_units"


def test_parse_detail_payload_falls_back_to_default_for_multi_family():
    row = {"property_type": "multi_family", "sub_type": None,
           "bedrooms": 4, "baths_total": 2.0, "baths_full": 2}
    # No list signal, no detail signal → default 2 units.
    result = fetch.parse_detail_payload(row, {})
    assert result["num_units"] == 2
    assert result["units_source"] == "default"


def test_parse_detail_payload_preserves_source_listing_status():
    row = {"property_type": "single_family", "sub_type": None,
           "bedrooms": 3, "baths_total": 2.0, "baths_full": 2}
    result = fetch.parse_detail_payload(row, {"source_listing_status": "Pending"})
    assert result["source_listing_status"] == "Pending"


def test_enrich_pending_details_only_touches_new_rows(monkeypatch):
    """Only rows with is_active=1, status='for_sale', and detail_fetched_at
    IS NULL should trigger detail fetches.
    """
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(fetch.SCHEMA)
    ts = "2026-04-19T00:00:00"
    con.execute(
        "INSERT INTO properties (property_id, status, fetched_at, is_active, "
        "property_type, sub_type, bedrooms, baths_total) "
        "VALUES ('new_mf', 'for_sale', ?, 1, 'multi_family', NULL, 4, 2)", (ts,))
    con.execute(
        "INSERT INTO properties (property_id, status, fetched_at, is_active, "
        "property_type, sub_type, bedrooms, baths_total, detail_fetched_at) "
        "VALUES ('old_mf', 'for_sale', ?, 1, 'multi_family', NULL, 4, 2, ?)", (ts, ts))
    con.execute(
        "INSERT INTO properties (property_id, status, fetched_at, is_active, "
        "property_type, sub_type, bedrooms, baths_total) "
        "VALUES ('stale', 'for_sale', ?, 0, 'multi_family', NULL, 4, 2)", (ts,))
    con.execute(
        "INSERT INTO properties (property_id, status, fetched_at, is_active, "
        "property_type, sub_type, bedrooms, baths_total) "
        "VALUES ('sf', 'for_sale', ?, 1, 'single_family', NULL, 3, 2)", (ts,))

    calls = []

    def fake_fetch(_key, pid):
        calls.append(pid)
        return {"description": {"units": 2}}

    monkeypatch.setattr(fetch, "fetch_detail", fake_fetch)

    enriched = fetch.enrich_pending_details(con, "fake-key")
    # Detail endpoint is called for every eligible row (no type shortcut) —
    # both 'new_mf' and 'sf'; 'old_mf' is skipped (already detailed) and
    # 'stale' is skipped (inactive). Order is nondeterministic under parallel
    # execution, so compare sets.
    assert set(calls) == {"new_mf", "sf"}
    assert enriched == 2


def test_build_rent_comps_skips_rows_without_bedrooms():
    rows = [
        ("a1", 2000, "Belleville", None, 1, 1.0),
        ("a2", 2200, "Belleville", None, 1, 1.0),
    ]
    con = _build_rentals_db(rows)
    fetch.build_rent_comps(con)
    assert con.execute("SELECT COUNT(*) FROM rent_comps").fetchone()[0] == 0
