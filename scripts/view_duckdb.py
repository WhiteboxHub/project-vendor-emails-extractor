"""
view_duckdb.py — Pretty print all rows from the local DuckDB raw_job_listings store.

Usage:
    python scripts/view_duckdb.py
    python scripts/view_duckdb.py --limit 50
    python scripts/view_duckdb.py --candidate 730
"""
import argparse
import json
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50, help="Max rows to show")
    parser.add_argument("--candidate", type=int, default=None, help="Filter by candidate_id")
    args = parser.parse_args()

    try:
        import duckdb
    except ImportError:
        print("Run: pip install duckdb")
        return

    db_path = Path(__file__).parent.parent / "data" / "raw_job_listings.duckdb"
    if not db_path.exists():
        print(f"No DuckDB file found at: {db_path}")
        return

    conn = duckdb.connect(str(db_path))

    where = f"WHERE candidate_id = {args.candidate}" if args.candidate else ""
    rows = conn.execute(f"""
        SELECT
            id, candidate_id,
            raw_title, raw_company, raw_location, raw_zip,
            raw_contact_info, processing_status, extracted_at
        FROM raw_job_listings
        {where}
        ORDER BY id DESC
        LIMIT {args.limit}
    """).fetchall()
    conn.close()

    total = len(rows)
    print(f"\n  Total rows shown: {total}  (DB: data/raw_job_listings.duckdb)\n")
    print("=" * 90)

    for r in rows:
        id_, cand_id, title, company, location, zip_, contact_raw, status, ts = r
        contact = {}
        if contact_raw:
            try:
                contact = json.loads(contact_raw)
            except Exception:
                pass

        name  = contact.get("name")  or "—"
        email = contact.get("email") or "—"
        phone = contact.get("phone") or "—"

        print(f"  ROW {id_:<5}  Candidate: {cand_id}  |  Status: {status}  |  At: {ts}")
        print(f"    Name    : {name}")
        print(f"    Email   : {email}")
        print(f"    Phone   : {phone}")
        print(f"    Title   : {title or '—'}")
        print(f"    Company : {company or '—'}")
        print(f"    Location: {location or '—'}  ZIP: {zip_ or '—'}")
        print("-" * 90)

    print(f"\n  {total} rows | ordered by newest first | use --limit N or --candidate ID to filter\n")

if __name__ == "__main__":
    main()
