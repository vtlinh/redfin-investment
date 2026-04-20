"""Compute year-1 cash-flow projection for every for-sale property in the DB.

Rental comps are **precomputed by fetch.py** into the `rent_comps` table; this
script just looks them up by (city, bedrooms, round(baths)), falling back to
the city=NULL row when a town lacks local samples. For multi-family sales, we
estimate unit count from bathroom count, split the bedrooms across units, look
up a per-unit comp, and sum.

Results are written to a `cashflow_analysis` table. Tune the globals below to
update assumptions.
"""

import json
import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.environ.get("DB_PATH", Path(__file__).parent / "properties.db"))

# Configurable assumptions. The webapp exposes these via a settings panel and
# can re-run analyze() with an overridden dict. Anything not in this dict
# (property tax, rent/expense growth, MIN_LIST_PRICE) is fixed.
DEFAULTS = {
    "down_payment_pct":    0.25,   # fraction of list price paid up front
    "interest_rate":       0.065,  # annual mortgage interest rate
    "loan_term_years":     30,
    "closing_cost_pct":    0.02,   # one-time, added to upfront cash invested
    "insurance_rate":      0.003,  # % of list price / yr
    "maintenance_rate":    0.002,
    "other_costs_rate":    0.0005,
    "vacancy_rate":        0.05,   # fraction of gross annual rent
    "management_fee_rate": 0.0,    # fraction of gross annual rent
    "value_growth":        0.03,   # per-year appreciation
    "holding_years":       15,
    "sell_cost_pct":       0.08,
    # Per-year growth rates for each cost line.
    "rent_growth":         0.05,
    "tax_growth":          0.03,
    "insurance_growth":    0.03,
    "hoa_growth":          0.03,
    "maintenance_growth":  0.03,
    "other_costs_growth":  0.03,
}

# Non-configurable (project-wide assumption).
PROPERTY_TAX_RATE = 0.025  # Essex/Bergen NJ avg

# Listings below this list price are excluded (likely land, teaser auctions, or data errors).
MIN_LIST_PRICE     = 200_000

# Realtor.com `property_type` values we treat as multi-family
MULTI_FAMILY_TYPES = {"multi_family", "duplex_triplex_quadplex"}

# Source listing statuses (from detail endpoint) that mean the property is
# no longer truly available even if the v3/list status is still "for_sale".
INACTIVE_SOURCE_STATUSES = {
    "pending", "under contract", "contingent", "sold", "closed",
    "withdrawn", "expired", "cancelled", "canceled", "off market",
}


def monthly_mortgage_payment(principal, annual_rate, term_years):
    """Standard amortized mortgage P&I payment."""
    r = annual_rate / 12
    n = term_years * 12
    if r == 0:
        return principal / n
    return principal * (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def total_roi(list_price, year1_rent, year1_mortgage, year1_components, cfg):
    """Cumulative ROI if sold at end of cfg['holding_years'].

    `year1_components` is a dict of {tax, insurance, hoa, maintenance, other}
    for year 1; each grows at its own rate. Vacancy and management fee are
    fractions of gross rent, so they grow implicitly with rent.
    """
    down_payment = list_price * cfg["down_payment_pct"]
    upfront_cash = down_payment + list_price * cfg["closing_cost_pct"]
    if upfront_cash <= 0:
        return None
    loan_balance    = list_price - down_payment
    monthly_payment = year1_mortgage / 12
    monthly_rate    = cfg["interest_rate"] / 12
    vac_rate        = cfg["vacancy_rate"]
    mgmt_rate       = cfg["management_fee_rate"]

    cumulative_cash = 0.0
    holding = int(cfg["holding_years"])
    for y in range(1, holding + 1):
        e     = y - 1
        rent  = year1_rent * (1 + cfg["rent_growth"]) ** e
        tax   = year1_components["tax"]         * (1 + cfg["tax_growth"])         ** e
        ins   = year1_components["insurance"]   * (1 + cfg["insurance_growth"])   ** e
        hoa   = year1_components["hoa"]         * (1 + cfg["hoa_growth"])         ** e
        maint = year1_components["maintenance"] * (1 + cfg["maintenance_growth"]) ** e
        other = year1_components["other"]       * (1 + cfg["other_costs_growth"]) ** e
        non_mtg = tax + ins + hoa + maint + other + rent * vac_rate + rent * mgmt_rate
        principal_paid = 0.0
        interest_paid  = 0.0
        for _ in range(12):
            interest = loan_balance * monthly_rate
            principal = monthly_payment - interest
            loan_balance -= principal
            principal_paid += principal
            interest_paid  += interest
        cumulative_cash += rent - principal_paid - interest_paid - non_mtg

    final_value = list_price * (1 + cfg["value_growth"]) ** holding
    net_sale    = final_value * (1 - cfg["sell_cost_pct"]) - loan_balance
    return (cumulative_cash + net_sale - upfront_cash) / upfront_cash


def comp_rent(conn, beds, baths, city):
    """Look up cached median rent for (city, beds, round(baths)). Falls back
    to the city=NULL row for the same (beds, baths) bucket if the town has no
    local cache entry.
    """
    if beds is None:
        return None
    baths_i = int(round(baths)) if baths else 1
    row = conn.execute(
        "SELECT median_rent FROM rent_comps WHERE city=? AND bedrooms=? AND baths=?",
        (city, beds, baths_i),
    ).fetchone()
    if row:
        return row[0]
    row = conn.execute(
        "SELECT median_rent FROM rent_comps WHERE city IS NULL AND bedrooms=? AND baths=?",
        (beds, baths_i),
    ).fetchone()
    return row[0] if row else None


def estimate_units(property_type, baths_total):
    """Fallback unit-count guess used when fetch.py hasn't populated `num_units`."""
    if property_type not in MULTI_FAMILY_TYPES:
        return 1
    if not baths_total or baths_total < 1:
        return 2
    return max(2, round(baths_total))


def _json_or_empty(s):
    if not s:
        return []
    try:
        v = json.loads(s)
        return v if isinstance(v, list) else []
    except (ValueError, TypeError):
        return []


def estimate_monthly_rent(conn, row):
    """Monthly rental income. For multi-family, comp per unit and sum.

    Uses `num_units` + `beds_per_unit_json` + `baths_per_unit_json` from the
    detail pass when available; otherwise falls back to the bath-count heuristic.
    """
    beds = row["bedrooms"]
    baths = row["baths_total"] if row["baths_total"] is not None else row["baths_full"]

    keys = row.keys() if hasattr(row, "keys") else []
    num_units = row["num_units"] if "num_units" in keys else None
    beds_per_unit = _json_or_empty(row["beds_per_unit_json"]) if "beds_per_unit_json" in keys else []
    baths_per_unit = _json_or_empty(row["baths_per_unit_json"]) if "baths_per_unit_json" in keys else []

    if num_units is None:
        num_units = estimate_units(row["property_type"], baths)

    if num_units == 1:
        return comp_rent(conn, beds, baths, row["city"])

    if beds_per_unit and baths_per_unit and len(beds_per_unit) == num_units:
        total = 0.0
        for b, ba in zip(beds_per_unit, baths_per_unit):
            r = comp_rent(conn, b, ba, row["city"])
            if r is None:
                return None
            total += r
        return total

    if beds is None:
        return None
    avg_beds = max(1, round(beds / num_units))
    avg_baths = max(1.0, baths / num_units) if baths else 1.0
    per_unit = comp_rent(conn, avg_beds, avg_baths, row["city"])
    return per_unit * num_units if per_unit is not None else None


def _dedup_key(row):
    """Identity key for the same physical property. Uses lat/lon rounded to
    3 decimals (~85m) combined with list_price + beds + baths_total to
    disambiguate adjacent buildings — this handles the common case where a
    property is listed twice with slightly different coordinates (e.g.
    "14 Crown St" vs "14-16 Crown St"). Falls back to (address_line, city,
    postal_code, list_price) when coordinates are missing.
    """
    lat, lon = row["latitude"], row["longitude"]
    if lat is not None and lon is not None:
        return ("geo", round(lat, 3), round(lon, 3),
                row["list_price"], row["bedrooms"], row["baths_total"])
    return ("addr", (row["address_line"] or "").strip().lower(),
            row["city"], row["postal_code"], row["list_price"])


def _dedup_listings(rows):
    """Collapse duplicate listings of the same physical property. When
    multiple rows share a key, keep the most recently listed one (ties →
    smallest property_id, for deterministic output).
    """
    buckets = {}
    for r in rows:
        k = _dedup_key(r)
        cur = buckets.get(k)
        if cur is None:
            buckets[k] = r
            continue
        cur_date = cur["list_date"] or ""
        new_date = r["list_date"] or ""
        if (new_date, cur["property_id"]) > (cur_date, r["property_id"]):
            buckets[k] = r
    return list(buckets.values())


def analyze(conn, cfg=None):
    """Return a list of cash-flow dicts for every for-sale property with comps."""
    cfg = {**DEFAULTS, **(cfg or {})}
    cols = {r[1] for r in conn.execute("PRAGMA table_info(properties)")}
    extras = [c for c in ("num_units", "beds_per_unit_json", "baths_per_unit_json") if c in cols]
    extras_sql = (", " + ", ".join(extras)) if extras else ""
    active_sql = " AND is_active=1" if "is_active" in cols else ""
    pending_sql = " AND COALESCE(is_pending,0)=0 AND COALESCE(is_contingent,0)=0" if "is_pending" in cols else ""
    # Drop listings whose source MLS status says they're no longer available.
    # NULL source_listing_status means we either haven't fetched detail or
    # the field wasn't exposed — keep those; SQL `status='for_sale'` already
    # filtered out the obvious inactive cases.
    if "source_listing_status" in cols:
        placeholders = ",".join("?" * len(INACTIVE_SOURCE_STATUSES))
        sls_sql = (f" AND (source_listing_status IS NULL OR "
                   f"LOWER(source_listing_status) NOT IN ({placeholders}))")
        params = tuple(s.lower() for s in INACTIVE_SOURCE_STATUSES)
    else:
        sls_sql = ""
        params = ()
    rows = conn.execute(
        f"""
        SELECT property_id, list_price, property_type, city,
               bedrooms, baths_full, baths_total, hoa_fee,
               latitude, longitude, address_line, postal_code,
               list_date{extras_sql}
        FROM properties
        WHERE status='for_sale' AND list_price IS NOT NULL AND list_price >= {MIN_LIST_PRICE}
              {active_sql}{pending_sql}{sls_sql}
        """,
        params,
    ).fetchall()
    rows = _dedup_listings(rows)

    results = []
    for r in rows:
        list_price = r["list_price"]
        monthly_rent = estimate_monthly_rent(conn, r)
        if monthly_rent is None:
            continue

        down_payment  = list_price * cfg["down_payment_pct"]
        upfront_cash  = down_payment + list_price * cfg["closing_cost_pct"]
        loan          = list_price - down_payment
        annual_rent   = monthly_rent * 12
        annual_mort   = monthly_mortgage_payment(loan, cfg["interest_rate"], cfg["loan_term_years"]) * 12
        annual_tax    = list_price * PROPERTY_TAX_RATE
        annual_ins    = list_price * cfg["insurance_rate"]
        annual_hoa    = (r["hoa_fee"] or 0) * 12
        annual_maint  = list_price * cfg["maintenance_rate"]
        annual_other  = list_price * cfg["other_costs_rate"]
        annual_vac    = annual_rent * cfg["vacancy_rate"]
        annual_mgmt   = annual_rent * cfg["management_fee_rate"]

        non_mortgage = (annual_tax + annual_ins + annual_hoa + annual_maint +
                        annual_other + annual_vac + annual_mgmt)
        expenses = annual_mort + non_mortgage
        cash_flow = annual_rent - expenses
        coc = cash_flow / upfront_cash if upfront_cash > 0 else None
        components = {
            "tax":         annual_tax,
            "insurance":   annual_ins,
            "hoa":         annual_hoa,
            "maintenance": annual_maint,
            "other":       annual_other,
        }
        troi = total_roi(list_price, annual_rent, annual_mort, components, cfg)

        results.append({
            "property_id":         r["property_id"],
            "annual_income":       round(annual_rent, 2),
            "mortgage":            round(annual_mort, 2),
            "expenses":            round(expenses, 2),
            "cash_flow":           round(cash_flow, 2),
            "cash_on_cash_return": round(coc, 4) if coc is not None else None,
            "total_roi":           round(troi, 4) if troi is not None else None,
        })
    return results


def write_results(conn, results):
    conn.executescript(
        """
        DROP TABLE IF EXISTS cashflow_analysis;
        CREATE TABLE cashflow_analysis (
            property_id          TEXT PRIMARY KEY,
            annual_income        REAL,
            mortgage             REAL,
            expenses             REAL,
            cash_flow            REAL,
            cash_on_cash_return  REAL,
            total_roi            REAL,
            FOREIGN KEY (property_id) REFERENCES properties(property_id)
        );
        CREATE INDEX idx_cashflow_cash_flow ON cashflow_analysis(cash_flow);
        CREATE INDEX idx_cashflow_coc       ON cashflow_analysis(cash_on_cash_return);
        CREATE INDEX idx_cashflow_total_roi ON cashflow_analysis(total_roi);
        """
    )
    with conn:
        conn.executemany(
            """
            INSERT INTO cashflow_analysis
                (property_id, annual_income, mortgage, expenses, cash_flow, cash_on_cash_return, total_roi)
            VALUES (:property_id, :annual_income, :mortgage, :expenses, :cash_flow, :cash_on_cash_return, :total_roi)
            """,
            results,
        )


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    results = analyze(conn)
    write_results(conn, results)
    positive = sum(1 for r in results if r["cash_flow"] > 0)
    print(f"Analyzed {len(results)} for-sale properties; {positive} have positive year-1 cash flow.")
    conn.close()


if __name__ == "__main__":
    main()
