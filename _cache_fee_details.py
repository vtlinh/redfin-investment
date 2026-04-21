"""One-off: cache raw detail payload into extra_info.detail for active
for-sale listings likely to have HOA/management fees, where no cached detail
exists yet and both fee columns are still NULL. Targets ~1,838 rows.

Scope: property_type in (condos, townhomes, coop, single_family, mobile,
apartment) AND hoa_fee IS NULL AND management_fee IS NULL AND cached detail
is missing. Safe to re-run — rows that already have cached detail are skipped.
"""
import json
import os
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from fetch import DETAIL_WORKERS, fetch_detail, parse_detail_payload


SCOPE_SQL = """
SELECT property_id, property_type, sub_type, bedrooms,
       baths_full, baths_total, extra_info
FROM properties
WHERE is_active=1 AND status='for_sale'
  AND (extra_info IS NULL OR json_extract(extra_info,'$.detail') IS NULL)
  AND hoa_fee IS NULL AND management_fee IS NULL
  AND property_type IN ('condos','townhomes','coop','mobile','apartment')
"""


def main():
    api_key = os.environ.get("RAPIDAPI_KEY")
    if not api_key:
        sys.exit("RAPIDAPI_KEY required")
    con = sqlite3.connect("properties.db")
    con.row_factory = sqlite3.Row
    rows = con.execute(SCOPE_SQL).fetchall()
    total = len(rows)
    print(f"Caching detail for {total} rows...")
    if not total:
        return

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    done = failed = 0

    def _fetch(row):
        row_d = dict(row)
        try:
            return row_d, fetch_detail(api_key, row_d["property_id"]), None
        except requests.RequestException as e:
            return row_d, None, e

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
        futures = [ex.submit(_fetch, r) for r in rows]
        for fut in as_completed(futures):
            row_d, detail, err = fut.result()
            if err is not None:
                failed += 1
                print(f"  fail {row_d['property_id']}: {err}")
                con.commit()
                continue
            try:
                extra = json.loads(row_d["extra_info"] or "{}") or {}
            except (ValueError, TypeError):
                extra = {}
            extra["detail"] = detail
            fields = parse_detail_payload(row_d, detail)
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
            done += 1
            if done % 50 == 0:
                con.commit()
                print(f"  progress: {done}/{total} cached ({failed} failed)")
    con.commit()
    con.close()
    print(f"Done: {done}/{total} cached, {failed} failed.")


if __name__ == "__main__":
    main()
