"""Flask app — lists active for-sale properties sorted by cash-on-cash return.

10 rows per page. Each row links to realtor.com; a chevron expands a 15-year
projection inline (rent +5%/yr, non-mortgage expenses +3%/yr, value +3%/yr,
mortgage P&I fixed).
"""

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlencode

from flask import Flask, abort, jsonify, render_template, request

import analyze

# Render mounts its persistent disk at /data; local dev uses the repo copy.
DB_PATH = Path(os.environ.get("DB_PATH", Path(__file__).parent / "properties.db"))

# Fixed project-wide tax rate (not exposed in the UI).
PROPERTY_TAX_RATE = analyze.PROPERTY_TAX_RATE
PAGE_SIZE         = 10

# Defaults shown in the settings panel; user overrides come in via the
# `calc_config` cookie (values stored as percentages, e.g. 25 for 25%).
CONFIG_DEFAULTS = analyze.DEFAULTS

# Display defaults for the form (percentages multiplied by 100, term/holding
# kept as integers).
PCT_FIELDS = {
    "down_payment_pct", "interest_rate", "closing_cost_pct",
    "insurance_rate", "maintenance_rate", "other_costs_rate",
    "vacancy_rate", "management_fee_rate", "value_growth", "sell_cost_pct",
    "rent_growth", "tax_growth", "insurance_growth", "hoa_growth",
    "maintenance_growth", "other_costs_growth",
}
INT_FIELDS = {"loan_term_years", "holding_years"}


def display_defaults():
    return {
        k: round(v * 100, 4) if k in PCT_FIELDS else int(v)
        for k, v in CONFIG_DEFAULTS.items()
    }


def get_config(req):
    """Merge cookie overrides into CONFIG_DEFAULTS. Returns a fresh dict in
    decimal form (e.g. 0.25 not 25)."""
    cfg = dict(CONFIG_DEFAULTS)
    raw = req.cookies.get("calc_config")
    if not raw:
        return cfg
    try:
        user = json.loads(unquote(raw))
    except (ValueError, TypeError):
        return cfg
    for k in cfg:
        if k not in user:
            continue
        try:
            v = float(user[k])
        except (ValueError, TypeError):
            continue
        cfg[k] = int(v) if k in INT_FIELDS else v / 100 if k in PCT_FIELDS else v
    return cfg


app = Flask(__name__)


def get_conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def _fmt_baths(b):
    return f"{b:g}" if isinstance(b, (int, float)) else str(b)


def unit_breakdown(row):
    """Return a list like ["3bd/1ba", "2bd/1ba"] for multi-unit listings, or
    None if we don't have a per-unit breakdown.
    """
    if not row.get("num_units") or row["num_units"] <= 1:
        return None
    try:
        beds = json.loads(row.get("beds_per_unit_json") or "[]")
        baths = json.loads(row.get("baths_per_unit_json") or "[]")
    except (ValueError, TypeError):
        return None
    if not beds or not baths or len(beds) != row["num_units"] or len(baths) != row["num_units"]:
        return None
    return [f"{b}bd/{_fmt_baths(ba)}ba" for b, ba in zip(beds, baths)]


def irr(flows):
    """Solve for the discount rate that makes NPV of `flows` zero (IRR).
    Returns None if no rate in (-0.99, 10) brackets a sign change.
    """
    def npv(r):
        s = 0.0
        for t, cf in enumerate(flows):
            s += cf / (1 + r) ** t
        return s
    lo, hi = -0.99, 10.0
    flo, fhi = npv(lo), npv(hi)
    if flo == 0:
        return lo
    if fhi == 0:
        return hi
    if flo * fhi > 0:
        return None
    for _ in range(80):
        mid = (lo + hi) / 2
        fm = npv(mid)
        if abs(fm) < 1e-7 or (hi - lo) < 1e-8:
            return mid
        if flo * fm < 0:
            hi, fhi = mid, fm
        else:
            lo, flo = mid, fm
    return (lo + hi) / 2


def project(list_price, year1_rent, year1_mortgage, hoa_fee, cfg):
    """Return a list of per-year dicts for years 1..cfg['holding_years'].

    Each cost line grows at its own rate. Vacancy and management fee are
    percent-of-rent, so they grow implicitly with rent.
    """
    down_payment    = list_price * cfg["down_payment_pct"]
    upfront_cash    = down_payment + list_price * cfg["closing_cost_pct"]
    loan_balance    = list_price - down_payment
    monthly_payment = year1_mortgage / 12
    monthly_rate    = cfg["interest_rate"] / 12
    sell_cost       = cfg["sell_cost_pct"]
    value_growth    = cfg["value_growth"]
    holding_years   = int(cfg["holding_years"])
    vac_rate        = cfg["vacancy_rate"]
    mgmt_rate       = cfg["management_fee_rate"]

    y1_tax   = list_price * PROPERTY_TAX_RATE
    y1_ins   = list_price * cfg["insurance_rate"]
    y1_hoa   = (hoa_fee or 0) * 12
    y1_maint = list_price * cfg["maintenance_rate"]
    y1_other = list_price * cfg["other_costs_rate"]

    out = []
    cumulative_cash = 0.0
    operating_flows = []
    for y in range(1, holding_years + 1):
        e      = y - 1
        rent   = year1_rent * (1 + cfg["rent_growth"])        ** e
        tax    = y1_tax     * (1 + cfg["tax_growth"])         ** e
        ins    = y1_ins     * (1 + cfg["insurance_growth"])   ** e
        hoa    = y1_hoa     * (1 + cfg["hoa_growth"])         ** e
        maint  = y1_maint   * (1 + cfg["maintenance_growth"]) ** e
        other_ = y1_other   * (1 + cfg["other_costs_growth"]) ** e
        other  = ins + hoa + maint + other_ + rent * vac_rate + rent * mgmt_rate

        principal_paid = 0.0
        interest_paid  = 0.0
        for _ in range(12):
            interest = loan_balance * monthly_rate
            principal = monthly_payment - interest
            loan_balance -= principal
            principal_paid += principal
            interest_paid  += interest

        cash = rent - principal_paid - interest_paid - tax - other
        cumulative_cash += cash
        operating_flows.append(cash)

        value        = list_price * (1 + value_growth) ** y
        net_sale     = value * (1 - sell_cost) - loan_balance
        total_profit = cumulative_cash + net_sale - upfront_cash

        # IRR assuming sale at end of year y: outflow at t=0, operating
        # cash in years 1..y-1, and operating cash + sale proceeds in year y.
        flows = [-upfront_cash] + operating_flows[:-1] + [operating_flows[-1] + net_sale]
        year_irr = irr(flows) if upfront_cash else None

        out.append({
            "year":       y,
            "value":      value,
            "rent":       rent,
            "principal":  principal_paid,
            "interest":   interest_paid,
            "tax":        tax,
            "ins":        ins,
            "hoa_yr":     hoa,
            "maint":      maint,
            "other_costs": other_,
            "vacancy":    rent * vac_rate,
            "mgmt":       rent * mgmt_rate,
            "expenses":   other,
            "cash_flow":  cash,
            "coc":        cash / upfront_cash if upfront_cash else 0,
            "annual_roi": year_irr if year_irr is not None else 0,
            "upfront_cash": upfront_cash,
            "cum_cash":   cumulative_cash,
            "net_sale":   net_sale,
            "sell_roi":   total_profit / upfront_cash if upfront_cash else 0,
        })
    return out


def parse_filters(args):
    def _int(name):
        v = (args.get(name) or "").strip().replace(",", "")
        try:
            return int(v) if v else None
        except ValueError:
            return None
    return {
        "property_types": [t for t in args.getlist("property_type") if t],
        "min_units":      _int("min_units"),
        "min_bedrooms":   _int("min_bedrooms"),
        "min_baths":      _int("min_baths"),
        "min_price":      _int("min_price"),
        "max_price":      _int("max_price"),
        "min_sqft":       _int("min_sqft"),
        "max_sqft":       _int("max_sqft"),
        "q":              (args.get("q") or "").strip(),
        "no_rent_info":   args.get("no_rent_info") == "1",
        "hide_ghetto":    args.get("hide_ghetto", "1") != "0",
    }


def build_where(filters):
    """Compose WHERE clause + bound params from a parsed filters dict."""
    clauses = ["p.is_active=1", "p.is_pending=0", "p.is_contingent=0",
               "p.address_line IS NOT NULL", "TRIM(p.address_line) != ''"]
    params = []
    if filters["property_types"]:
        ph = ",".join("?" * len(filters["property_types"]))
        clauses.append(f"p.property_type IN ({ph})")
        params.extend(filters["property_types"])
    if filters["min_units"]:
        clauses.append("COALESCE(p.num_units, 1) >= ?")
        params.append(filters["min_units"])
    if filters["min_bedrooms"]:
        clauses.append("p.bedrooms >= ?")
        params.append(filters["min_bedrooms"])
    if filters["min_baths"]:
        clauses.append("p.baths_total >= ?")
        params.append(filters["min_baths"])
    if filters["min_price"]:
        clauses.append("p.list_price >= ?")
        params.append(filters["min_price"])
    if filters["max_price"]:
        clauses.append("p.list_price <= ?")
        params.append(filters["max_price"])
    if filters["min_sqft"]:
        clauses.append("p.area_sqft >= ?")
        params.append(filters["min_sqft"])
    if filters["max_sqft"]:
        clauses.append("p.area_sqft <= ?")
        params.append(filters["max_sqft"])
    if filters["q"]:
        like = f"%{filters['q']}%"
        clauses.append(
            "(p.address_line LIKE ? OR p.city LIKE ? OR p.postal_code LIKE ? "
            "OR p.state LIKE ? OR p.agent_name LIKE ? OR p.office_name LIKE ?)"
        )
        params.extend([like] * 6)
    if filters.get("hide_ghetto"):
        clauses.append(
            "p.postal_code NOT IN ("
            "  SELECT postal_code FROM zip_demographics"
            "  WHERE median_household_income < 50000 OR poverty_rate > 0.20)"
        )
    return " AND ".join(clauses), params


def filter_querystring(filters):
    """Build a URL querystring from filters (omitting empty/false values)."""
    parts = []
    for k, v in filters.items():
        if k == "property_types":
            for t in v:
                parts.append(("property_type", t))
        elif k in ("no_rent_info", "hide_ghetto"):
            if v:
                parts.append((k, "1"))
        elif v not in (None, "", 0):
            parts.append((k, v))
    return urlencode(parts)


SORT_COLS = {
    # key:          (sql_expr, default direction, nulls-position clause)
    "property":   ("LOWER(p.address_line)",  "ASC"),
    "list_price": ("p.list_price",           "DESC"),
    "rent":       ("c.annual_income",        "DESC"),
    "cashflow":   ("c.cash_flow",            "DESC"),
    "coc":        ("c.cash_on_cash_return",  "DESC"),
    "roi":        ("c.total_roi",            "DESC"),
}


def parse_sort(args):
    key = args.get("sort") or "coc"
    if key not in SORT_COLS:
        key = "coc"
    default_dir = SORT_COLS[key][1]
    direction = (args.get("dir") or default_dir).upper()
    if direction not in ("ASC", "DESC"):
        direction = default_dir
    return key, direction


HEADER_LABELS = [
    ("property",   "Property"),
    ("list_price", "List"),
    ("rent",       "Rent/mo"),
    ("cashflow",   "Cashflow"),
    ("coc",        "CoC"),
    ("roi",        "IRR"),
]


def build_headers(sort, filter_qs):
    """Build per-column header dicts with the URL to toggle sort for that
    column. Clicking the active column flips direction; clicking any other
    column resets to that column's default direction.
    """
    sort_key, sort_dir = sort
    base_qs = filter_qs + "&" if filter_qs else ""
    out = []
    for key, label in HEADER_LABELS:
        if key == sort_key:
            new_dir = "asc" if sort_dir == "DESC" else "desc"
            arrow = "\u2193" if sort_dir == "DESC" else "\u2191"
        else:
            new_dir = SORT_COLS[key][1].lower()
            arrow = ""
        out.append({
            "key":    key,
            "label":  label,
            "href":   f"?{base_qs}sort={key}&dir={new_dir}",
            "arrow":  arrow,
            "active": key == sort_key,
        })
    return out


def _unit_keys(p):
    """Return the (beds, baths_i) bucket keys used to compute rent for this property.
    Multi-family properties with per-unit data use each unit's beds/baths;
    single-family properties use the property-level totals.
    """
    try:
        beds_per_unit = json.loads(p.get("beds_per_unit_json") or "[]")
        baths_per_unit = json.loads(p.get("baths_per_unit_json") or "[]")
    except (ValueError, TypeError):
        beds_per_unit, baths_per_unit = [], []
    num_units = p.get("num_units") or 1
    if num_units > 1 and beds_per_unit and baths_per_unit and len(beds_per_unit) == num_units:
        return [(b, round((ba or 1.0) * 2) / 2.0) for b, ba in zip(beds_per_unit, baths_per_unit)]
    baths_r = round((p["baths_total"] or 1.0) * 2) / 2.0
    return [(p["bedrooms"] or 0, baths_r)]


def _attach_rent_comps(con, properties):
    """For each property look up nearest-to-median rental comps from
    rent_comps.comp_ids_json. Multi-family: collects comps across all unit
    buckets (deduped, sorted descending by price).
    """
    # Collect all (city, beds, baths) buckets needed across this page
    buckets = {}
    for p in properties:
        for beds, baths_i in _unit_keys(p):
            key = (p["city"], beds, baths_i)
            buckets[key] = None

    try:
        for key in list(buckets.keys()):
            city, beds, baths = key
            row = con.execute(
                "SELECT comp_ids_json FROM rent_comps WHERE city=? AND bedrooms=? AND baths=?",
                (city, beds, baths),
            ).fetchone()
            if row and row[0]:
                buckets[key] = json.loads(row[0])
    except Exception:
        for p in properties:
            p["rent_comps"] = []
        return

    # Fetch details for all referenced IDs in one query
    all_ids = {pid for ids in buckets.values() if ids for pid in ids}
    if all_ids:
        ph = ",".join("?" * len(all_ids))
        comp_rows = con.execute(
            f"SELECT property_id, address_line, city, list_price, url FROM properties WHERE property_id IN ({ph})",
            list(all_ids),
        ).fetchall()
        detail = {r["property_id"]: dict(r) for r in comp_rows}
    else:
        detail = {}

    # Check which (postal_code, beds, baths) groups have external estimates,
    # so we can label the source when no local comps exist.
    ext_sources = {}
    try:
        postal_codes = {p["postal_code"] for p in properties if p.get("postal_code")}
        if postal_codes:
            ph = ",".join("?" * len(postal_codes))
            for row in con.execute(
                f"SELECT postal_code, bedrooms, baths, source FROM external_rent_estimates WHERE postal_code IN ({ph})",
                list(postal_codes),
            ).fetchall():
                ext_sources[(row[0], row[1], row[2])] = row[3]
    except Exception:
        pass

    SOURCE_LABELS = {"rentcast": "Rentcast AVM estimate", "hud_fmr": "HUD Fair Market Rent"}

    for p in properties:
        seen = set()
        comps = []
        for beds, baths_i in _unit_keys(p):
            key = (p["city"], beds, baths_i)
            for pid in (buckets.get(key) or []):
                d = detail.get(pid)
                if d and d["city"] == p["city"] and pid not in seen:
                    seen.add(pid)
                    comps.append(d)
        comps.sort(key=lambda x: x["list_price"] or 0, reverse=True)
        p["rent_comps"] = comps
        # Attach external source label when no local comps back this property's rent
        p["rent_source"] = None
        if not comps and p.get("annual_income") is not None:
            postal_code = p.get("postal_code")
            for beds, baths_i in _unit_keys(p):
                src = ext_sources.get((postal_code, beds, baths_i))
                if src:
                    p["rent_source"] = SOURCE_LABELS.get(src, src)
                    break


def fetch_page(con, page, filters, cfg, sort):
    where, params = build_where(filters)
    offset = (page - 1) * PAGE_SIZE
    last_updated = con.execute(
        "SELECT MAX(last_seen_at) FROM properties WHERE is_active=1"
    ).fetchone()[0]

    if filters.get("no_rent_info"):
        # Properties with no city-specific rent comp (absent from cashflow_analysis)
        total = con.execute(
            f"""SELECT COUNT(*) FROM properties p
                LEFT JOIN cashflow_analysis c USING(property_id)
                WHERE {where} AND p.status='for_sale'
                  AND p.list_price >= {analyze.MIN_LIST_PRICE} AND c.property_id IS NULL""",
            params,
        ).fetchone()[0]
        rows = con.execute(
            f"""SELECT p.property_id, p.address_line, p.city, p.state, p.postal_code,
                       p.list_price, p.property_type, p.bedrooms, p.baths_total,
                       p.area_sqft, p.year_built, p.num_units, p.url, p.hoa_fee,
                       p.beds_per_unit_json, p.baths_per_unit_json
                FROM properties p
                LEFT JOIN cashflow_analysis c USING(property_id)
                WHERE {where} AND p.status='for_sale'
                  AND p.list_price >= {analyze.MIN_LIST_PRICE} AND c.property_id IS NULL
                ORDER BY p.list_price ASC, p.property_id ASC
                LIMIT ? OFFSET ?""",
            params + [PAGE_SIZE, offset],
        ).fetchall()
        properties = []
        for r in rows:
            d = dict(r)
            d.update(annual_income=None, mortgage=None, expenses=None,
                     cash_flow=None, cash_on_cash_return=None,
                     total_roi=None, projection=[], rent_comps=[])
            d["unit_breakdown"] = unit_breakdown(d)
            properties.append(d)
    else:
        sort_key, sort_dir = sort
        sort_sql = SORT_COLS[sort_key][0]
        total = con.execute(
            f"SELECT COUNT(*) FROM cashflow_analysis c JOIN properties p USING(property_id) WHERE {where}",
            params,
        ).fetchone()[0]
        rows = con.execute(
            f"""SELECT p.property_id, p.address_line, p.city, p.state, p.postal_code,
                       p.list_price, p.property_type, p.bedrooms, p.baths_total,
                       p.area_sqft, p.year_built, p.num_units, p.url, p.hoa_fee,
                       p.beds_per_unit_json, p.baths_per_unit_json,
                       c.annual_income, c.mortgage, c.expenses, c.cash_flow,
                       c.cash_on_cash_return
                FROM cashflow_analysis c JOIN properties p USING(property_id)
                WHERE {where}
                ORDER BY {sort_sql} {sort_dir}, p.property_id ASC
                LIMIT ? OFFSET ?""",
            params + [PAGE_SIZE, offset],
        ).fetchall()
        properties = []
        for r in rows:
            d = dict(r)
            d["projection"] = project(d["list_price"], d["annual_income"],
                                      d["mortgage"], d["hoa_fee"], cfg)
            d["unit_breakdown"] = unit_breakdown(d)
            if d["projection"]:
                d["total_roi"] = d["projection"][-1]["annual_roi"]
            else:
                d["total_roi"] = None
            properties.append(d)
        _attach_rent_comps(con, properties)

    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    return properties, total, pages, last_updated


@app.route("/")
def index():
    try:
        page = max(1, int(request.args.get("page", 1)))
    except ValueError:
        abort(400)
    filters = parse_filters(request.args)
    cfg = get_config(request)
    sort = parse_sort(request.args)
    con = get_conn()
    properties, total, pages, last_updated = fetch_page(con, page, filters, cfg, sort)
    if request.args.get("partial") == "1":
        html = render_template(
            "_cards.html",
            properties=properties,
            page=page,
            page_size=PAGE_SIZE,
        )
        con.close()
        return jsonify(html=html, has_more=page < pages, total=total, page=page, pages=pages)
    property_types = [
        r[0] for r in con.execute(
            "SELECT DISTINCT property_type FROM properties "
            "WHERE is_active=1 AND status='for_sale' AND property_type IS NOT NULL "
            "ORDER BY property_type"
        ).fetchall()
    ]
    no_rent_count = con.execute(
        f"""SELECT COUNT(*) FROM properties p
           LEFT JOIN cashflow_analysis c USING(property_id)
           WHERE p.is_active=1 AND p.is_pending=0 AND p.is_contingent=0
             AND p.status='for_sale'
             AND p.list_price IS NOT NULL AND p.list_price >= {analyze.MIN_LIST_PRICE}
             AND p.address_line IS NOT NULL AND TRIM(p.address_line) != ''
             AND c.property_id IS NULL"""
    ).fetchone()[0]
    ghetto_count = con.execute(
        f"""SELECT COUNT(*) FROM cashflow_analysis c JOIN properties p USING(property_id)
            WHERE p.is_active=1 AND p.is_pending=0 AND p.is_contingent=0
              AND p.address_line IS NOT NULL AND TRIM(p.address_line) != ''
              AND p.postal_code IN (
                SELECT postal_code FROM zip_demographics
                WHERE median_household_income < 50000 OR poverty_rate > 0.20)"""
    ).fetchone()[0]
    con.close()
    if page > pages and total > 0:
        abort(404)
    last_updated_display = None
    if last_updated:
        try:
            last_updated_display = datetime.fromisoformat(
                last_updated.replace("Z", "+00:00")
            ).strftime("%B %d, %Y")
        except ValueError:
            last_updated_display = last_updated[:10]
    return render_template(
        "index.html",
        properties=properties,
        page=page,
        pages=pages,
        total=total,
        page_size=PAGE_SIZE,
        projection_years=int(cfg["holding_years"]),
        last_updated=last_updated_display,
        filters=filters,
        property_types=property_types,
        filter_qs=filter_querystring(filters),
        config_defaults=display_defaults(),
        config_groups=CONFIG_GROUPS,
        headers=build_headers(sort, filter_querystring(filters)),
        sort_qs=f"sort={sort[0]}&dir={sort[1].lower()}",
        no_rent_count=no_rent_count,
        ghetto_count=ghetto_count,
    )


@app.route("/recompute", methods=["POST"])
def recompute():
    """Re-run the analyze pass with the user's current cookie config and
    overwrite cashflow_analysis in place."""
    cfg = get_config(request)
    con = get_conn()
    results = analyze.analyze(con, cfg)
    analyze.write_results(con, results)
    con.close()
    return ("", 204)


# Field metadata for rendering the settings form. (key, label, suffix, step)
# Fields rendered in the settings panel. Each entry is a group — fields in
# the same group stay side-by-side on one line and never wrap apart.
CONFIG_GROUPS = [
    [("down_payment_pct",    "Down payment",        "%",   "0.5")],
    [("interest_rate",       "Interest rate",       "%",   "0.05")],
    [("loan_term_years",     "Loan term",           "yrs", "1")],
    [("closing_cost_pct",    "Closing cost",        "%",   "0.1")],
    [("insurance_rate",      "Insurance",           "%",   "0.05"),
     ("insurance_growth",    "Insurance increase",  "%/yr","0.25")],
    [("maintenance_rate",    "Maintenance",         "%",   "0.05"),
     ("maintenance_growth",  "Maintenance increase","%/yr","0.25")],
    [("other_costs_rate",    "Other costs",         "%",   "0.05"),
     ("other_costs_growth",  "Other costs increase","%/yr","0.25")],
    [("vacancy_rate",        "Vacancy",             "%",   "0.5")],
    [("management_fee_rate", "Management fee",      "%",   "0.5")],
    [("rent_growth",         "Rent increase",       "%/yr","0.25")],
    [("tax_growth",          "Tax increase",        "%/yr","0.25")],
    [("hoa_growth",          "HOA increase",        "%/yr","0.25")],
    [("value_growth",        "Value appreciation",  "%/yr","0.25")],
    [("holding_years",       "Holding length",      "yrs", "1")],
    [("sell_cost_pct",       "Cost to sell",        "%",   "0.5")],
]


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
