"""Fetch descriptions for active coops and extract monthly management fees.

Strategy:
  1. For every active for_sale coop, call the detail endpoint and pull
     description.text.
  2. Regex-scan for monthly-fee mentions ($N/mo, $N per month, $N maintenance,
     etc.) and write the largest detected number into properties.management_fee.
  3. Dump all descriptions + hits to `coop_fee_review.txt` for manual review.

Run once after fetch.py; free tier cost is ~44 detail calls.
"""
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

from fetch import DETAIL_URL, API_HOST, DB_PATH

load_dotenv()

FEE_PATTERNS = [
    # $1,234/mo | $1,234 per month | $1,234 monthly
    re.compile(r"\$\s?([\d,]{2,7})\s*(?:/|\s+per\s+|\s+a\s+)?\s*(?:mo(?:nth)?(?:ly)?)", re.I),
    # maintenance fee $1,234 | maintenance is $1,234 | maintenance: $1,234
    re.compile(r"(?:maintenance|management|monthly\s+fee|coop\s+fee|co-?op\s+fee)"
               r"[^$\n]{0,30}\$\s?([\d,]{2,7})", re.I),
    # $1,234 maintenance | $1,234 management
    re.compile(r"\$\s?([\d,]{2,7})[^$\n]{0,20}(?:maintenance|management)", re.I),
]


def extract_fee(text):
    if not text:
        return None, []
    candidates = []
    for pat in FEE_PATTERNS:
        for m in pat.finditer(text):
            raw = m.group(1).replace(",", "")
            try:
                val = float(raw)
            except ValueError:
                continue
            if 100 <= val <= 10000:
                candidates.append((val, m.group(0)))
    if not candidates:
        return None, []
    best = max(candidates, key=lambda x: x[0])
    return best[0], candidates


def main():
    api_key = os.environ.get("RAPIDAPI_KEY")
    if not api_key:
        raise SystemExit("RAPIDAPI_KEY required")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """SELECT property_id, address_line, city, hoa_fee
           FROM properties
           WHERE property_type='coop' AND is_active=1 AND status='for_sale'
           ORDER BY city, address_line"""
    ).fetchall()
    print(f"Fetching detail for {len(rows)} active coops...")

    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": API_HOST}
    review = []
    updated = 0
    for r in rows:
        pid = r["property_id"]
        try:
            resp = requests.get(DETAIL_URL, headers=headers,
                                params={"property_id": pid}, timeout=30)
            resp.raise_for_status()
            home = (resp.json().get("data") or {}).get("home") or {}
        except requests.RequestException as e:
            print(f"  {pid} {r['address_line']}: fetch failed {e}")
            continue

        desc = (home.get("description") or {}).get("text") or ""
        fee, cands = extract_fee(desc)
        review.append({
            "property_id": pid,
            "address": f"{r['address_line']}, {r['city']}",
            "hoa_fee": r["hoa_fee"],
            "detected_fee": fee,
            "matches": [c[1] for c in cands],
            "description": desc,
        })
        if fee is not None:
            con.execute(
                "UPDATE properties SET management_fee=? WHERE property_id=?",
                (fee, pid),
            )
            updated += 1
    con.commit()
    con.close()

    out = Path("coop_fee_review.txt")
    with out.open("w", encoding="utf-8") as f:
        for item in review:
            f.write("=" * 80 + "\n")
            f.write(f"ID:       {item['property_id']}\n")
            f.write(f"Address:  {item['address']}\n")
            f.write(f"hoa_fee:  {item['hoa_fee']}\n")
            f.write(f"detected: {item['detected_fee']}\n")
            if item["matches"]:
                f.write(f"matches:  {item['matches']}\n")
            f.write(f"\n{item['description']}\n\n")

    print(f"Updated management_fee on {updated} / {len(rows)} coops")
    print(f"Review file: {out}")


if __name__ == "__main__":
    main()
