"""Tests for analyze.py cash-flow calculation."""

import sqlite3

import analyze


def test_monthly_mortgage_payment_known_value():
    # $100k @ 6% / 30 yr ≈ $599.55
    pmt = analyze.monthly_mortgage_payment(100_000, 0.06, 30)
    assert abs(pmt - 599.55) < 0.5


def test_monthly_mortgage_payment_zero_rate():
    # interest-free loan amortizes linearly
    pmt = analyze.monthly_mortgage_payment(120_000, 0.0, 30)
    assert abs(pmt - 120_000 / 360) < 1e-6


def test_estimate_units_single_family():
    assert analyze.estimate_units("single_family", 2.0) == 1


def test_estimate_units_multi_family_uses_baths():
    assert analyze.estimate_units("multi_family", 3.0) == 3
    assert analyze.estimate_units("multi_family", None) == 2  # fallback


def _seed_db():
    """In-memory DB with the minimal schema `analyze.py` reads: `properties`
    (for_sale rows) and a precomputed `rent_comps` cache.
    """
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript("""
        CREATE TABLE properties (
            property_id TEXT PRIMARY KEY, status TEXT, list_price INTEGER,
            property_type TEXT, city TEXT, bedrooms INTEGER,
            baths_full INTEGER, baths_total REAL, hoa_fee INTEGER,
            latitude REAL, longitude REAL, address_line TEXT, postal_code TEXT,
            list_date TEXT,
            is_active INTEGER DEFAULT 1,
            num_units INTEGER, beds_per_unit_json TEXT, baths_per_unit_json TEXT,
            source_listing_status TEXT,
            is_pending INTEGER DEFAULT 0, is_contingent INTEGER DEFAULT 0
        );
        CREATE TABLE rent_comps (
            city TEXT, bedrooms INTEGER, baths INTEGER,
            median_rent REAL, sample_size INTEGER,
            PRIMARY KEY (city, bedrooms, baths)
        );
    """)
    con.execute("INSERT INTO rent_comps VALUES ('Belleville', 2, 1, 2200, 3)")
    con.execute("INSERT INTO rent_comps VALUES (NULL, 2, 1, 2100, 10)")
    con.execute(
        "INSERT INTO properties (property_id, status, list_price, property_type, city, "
        "bedrooms, baths_full, baths_total, num_units) "
        "VALUES ('sfh1', 'for_sale', 400000, 'single_family', 'Belleville', 2, 1, 1.0, 1)"
    )
    # Multi-family: num_units precomputed by fetch.py as 2
    con.execute(
        "INSERT INTO properties (property_id, status, list_price, property_type, city, "
        "bedrooms, baths_full, baths_total, num_units, beds_per_unit_json, baths_per_unit_json) "
        "VALUES ('mf1', 'for_sale', 600000, 'multi_family', 'Belleville', 4, 2, 2.0, 2, '[2,2]', '[1.0,1.0]')"
    )
    return con


def test_comp_rent_uses_city_specific_row():
    con = _seed_db()
    assert analyze.comp_rent(con, beds=2, baths=1.0, city="Belleville") == 2200


def test_comp_rent_falls_back_to_null_city():
    con = _seed_db()
    # Elsewhere has no local row for (beds=2, baths=1) → fallback row (2100)
    assert analyze.comp_rent(con, beds=2, baths=1.0, city="Elsewhere") == 2100


def test_comp_rent_returns_none_when_no_bucket():
    con = _seed_db()
    assert analyze.comp_rent(con, beds=7, baths=3.0, city="Belleville") is None


def test_analyze_single_family_cashflow():
    con = _seed_db()
    results = {r["property_id"]: r for r in analyze.analyze(con)}
    sfh = results["sfh1"]
    assert sfh["annual_income"] == 2200 * 12  # 26,400
    # Rough sanity: mortgage on $300k loan @ 6.5%/30y ≈ $22,760/yr
    assert 21_000 < sfh["mortgage"] < 24_000
    # Expenses well above $26k/yr on a $400k property → negative cashflow expected
    assert sfh["cash_flow"] < 0
    # Cash-on-cash = cashflow / (down payment + closing costs)
    # rounded to 4 decimals in analyze.py, so allow 5e-5 tolerance
    upfront = 400_000 * (analyze.DEFAULTS["down_payment_pct"] + analyze.DEFAULTS["closing_cost_pct"])
    assert abs(sfh["cash_on_cash_return"] - sfh["cash_flow"] / upfront) < 5e-5


def test_analyze_multi_family_sums_units():
    con = _seed_db()
    results = {r["property_id"]: r for r in analyze.analyze(con)}
    mf = results["mf1"]
    # 2 units × $2200 each × 12 = $52,800/yr
    assert mf["annual_income"] == 2200 * 2 * 12


def test_analyze_skips_inactive_listings():
    con = _seed_db()
    con.execute("UPDATE properties SET is_active=0 WHERE property_id='sfh1'")
    ids = {r["property_id"] for r in analyze.analyze(con)}
    assert "sfh1" not in ids
    assert "mf1" in ids


def test_analyze_skips_pending_source_status():
    con = _seed_db()
    con.execute("UPDATE properties SET source_listing_status='Pending' WHERE property_id='mf1'")
    ids = {r["property_id"] for r in analyze.analyze(con)}
    assert "mf1" not in ids


def test_analyze_skips_pending_flag():
    con = _seed_db()
    con.execute("UPDATE properties SET is_pending=1 WHERE property_id='sfh1'")
    ids = {r["property_id"] for r in analyze.analyze(con)}
    assert "sfh1" not in ids


def test_analyze_skips_contingent_flag():
    con = _seed_db()
    con.execute("UPDATE properties SET is_contingent=1 WHERE property_id='mf1'")
    ids = {r["property_id"] for r in analyze.analyze(con)}
    assert "mf1" not in ids


def test_analyze_dedupes_same_coordinate_listings():
    """Two listings with near-identical lat/lon and same price should be
    collapsed to the one with the most recent list_date.
    """
    con = _seed_db()
    # Two listings for the same physical property at 14(-16) Crown St.
    # Lat/lon differ at 6th decimal; round(_, 4) merges them.
    con.execute(
        "INSERT INTO properties (property_id, status, list_price, property_type, city, "
        "bedrooms, baths_full, baths_total, num_units, beds_per_unit_json, "
        "baths_per_unit_json, latitude, longitude, address_line, postal_code, list_date) "
        "VALUES ('dup_old', 'for_sale', 400000, 'single_family', 'Belleville', 2, 1, 1.0, 1, "
        "'[2]', '[1.0]', 40.78814, -74.19655, '14-16 Crown St', '07003', '2026-04-14T00:00:00Z')"
    )
    con.execute(
        "INSERT INTO properties (property_id, status, list_price, property_type, city, "
        "bedrooms, baths_full, baths_total, num_units, beds_per_unit_json, "
        "baths_per_unit_json, latitude, longitude, address_line, postal_code, list_date) "
        "VALUES ('dup_new', 'for_sale', 400000, 'single_family', 'Belleville', 2, 1, 1.0, 1, "
        "'[2]', '[1.0]', 40.78818, -74.19654, '14 Crown St', '07003', '2026-04-15T00:00:00Z')"
    )
    ids = {r["property_id"] for r in analyze.analyze(con)}
    assert "dup_new" in ids
    assert "dup_old" not in ids


def test_analyze_keeps_null_source_status():
    # NULL means detail wasn't fetched yet or field wasn't exposed — keep row.
    con = _seed_db()
    ids = {r["property_id"] for r in analyze.analyze(con)}
    assert "sfh1" in ids and "mf1" in ids
