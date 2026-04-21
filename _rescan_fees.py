"""Re-run parse_detail_payload over every row with cached detail payload in
extra_info.detail. No API calls — pure local rescan, used after tuning the
extraction regex."""
import json
import sqlite3
from datetime import datetime, timezone

from fetch import parse_detail_payload


def main():
    con = sqlite3.connect("properties.db")
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """SELECT property_id, property_type, sub_type, bedrooms,
                  baths_full, baths_total, extra_info
           FROM properties
           WHERE extra_info IS NOT NULL
             AND json_extract(extra_info, '$.detail') IS NOT NULL"""
    ).fetchall()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    changed = 0
    for r in rows:
        row_d = dict(r)
        extra = json.loads(row_d["extra_info"])
        detail = extra["detail"]
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
                {update_hoa}
                {update_mgmt}
                detail_fetched_at     = :detail_fetched_at
            WHERE property_id = :property_id
            """,
            {**fields, "detail_fetched_at": now,
             "property_id": row_d["property_id"]},
        )
        changed += 1
        if changed % 200 == 0:
            con.commit()
    con.commit()
    print(f"Rescanned {changed} rows.")


if __name__ == "__main__":
    main()
