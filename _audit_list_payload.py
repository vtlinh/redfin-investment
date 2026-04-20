"""Pull a wider sample and check, for multi-family rows, how often
beds_min/max, baths_min/max, sqft_min/max are populated — and whether
flags.is_pending / is_contingent are available on every list row."""
import json
import os
from collections import Counter

import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://realty-in-us.p.rapidapi.com/properties/v3/list"
HEADERS = {
    "X-RapidAPI-Key": os.environ["RAPIDAPI_KEY"],
    "X-RapidAPI-Host": "realty-in-us.p.rapidapi.com",
    "Content-Type": "application/json",
}

all_rows = []
for offset in (0, 200, 400):
    payload = {
        "limit": 200,
        "offset": offset,
        "county": "Essex",
        "state_code": "NJ",
        "status": ["for_sale"],
        "sort": {"direction": "desc", "field": "list_date"},
    }
    r = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    page = (r.json().get("data") or {}).get("home_search", {}).get("results") or []
    if not page:
        break
    all_rows.extend(page)

print(f"got {len(all_rows)} for_sale rows from Essex NJ\n")

mf_types = {"multi_family", "duplex_triplex_quadplex"}
mf_subs  = {"duplex", "triplex", "quadplex", "fourplex"}

mf_rows = [
    r for r in all_rows
    if (r.get("description") or {}).get("type") in mf_types
    or (r.get("description") or {}).get("sub_type") in mf_subs
]
print(f"{len(mf_rows)} multi-family rows\n")

# How often is each "range" field populated for MF rows?
range_fields = ["beds_min", "beds_max", "baths_min", "baths_max", "sqft_min", "sqft_max"]
pop = Counter()
for r in mf_rows:
    d = r.get("description") or {}
    for f in range_fields:
        if d.get(f) is not None:
            pop[f] += 1
print("=== range field populated count (out of {} MF rows) ===".format(len(mf_rows)))
for f in range_fields:
    print(f"  {pop[f]:4d}  description.{f}")

# Show MF rows that DO have a range populated
print("\n=== example MF rows with populated ranges ===")
shown = 0
for r in mf_rows:
    d = r.get("description") or {}
    if any(d.get(f) is not None for f in range_fields) and shown < 5:
        print(f"\nproperty_id={r['property_id']}  type={d.get('type')}  sub_type={d.get('sub_type')}")
        for f in ["beds", "beds_min", "beds_max", "baths", "baths_min", "baths_max",
                  "sqft", "sqft_min", "sqft_max"]:
            print(f"  {f:12s} = {d.get(f)}")
        print(f"  href = {r.get('href')}")
        shown += 1

# Coverage of flags fields across ALL rows
print("\n=== flags coverage across all {} rows ===".format(len(all_rows)))
flag_keys = ["is_pending", "is_contingent", "is_coming_soon", "is_new_listing",
             "is_foreclosure", "is_price_reduced", "is_new_construction"]
for k in flag_keys:
    n = sum(1 for r in all_rows if (r.get("flags") or {}).get(k) is True)
    present = sum(1 for r in all_rows if k in (r.get("flags") or {}))
    print(f"  {k:22s} present_in_payload={present:4d}  set_to_true={n:4d}")

# Check if `tags` ever appears on a list row
tags_present = sum(1 for r in all_rows if "tags" in r)
print(f"\n'tags' key present on {tags_present}/{len(all_rows)} rows")
