"""
generate_duckdb_log.py
======================
Reads every row from the local DuckDB raw_job_listings store and writes a
human-readable + machine-readable summary to data/duckdb_logs.json.

Run manually any time:
    python scripts/generate_duckdb_log.py

Also called automatically at the end of every extraction run by vendor_contacts.py.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DB_PATH      = _PROJECT_ROOT / "data" / "raw_job_listings.duckdb"
_LOG_PATH     = _PROJECT_ROOT / "data" / "duckdb_logs.json"


# ─────────────────────────────────────────────────────────────────────────────
# Core builder
# ─────────────────────────────────────────────────────────────────────────────

def build_duckdb_log(db_path: Optional[Path] = None) -> Dict:
    """
    Read all rows from raw_job_listings and build a structured log dict.

    Returns a dict with:
        generated_at        – ISO timestamp
        db_path             – path to the DuckDB file
        total_rows          – total contacts stored across all runs
        status_breakdown    – {status: count} (e.g. {"new": 15})
        by_candidate        – list of per-candidate summaries
        all_rows            – flat list of every row (full detail)
    """
    try:
        import duckdb
    except ImportError:
        raise ImportError("Run: pip install duckdb>=0.10.0")

    resolved = Path(db_path) if db_path else _DB_PATH
    if not resolved.exists():
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(resolved),
            "error": "DuckDB file not found — no run has completed yet.",
            "total_rows": 0,
            "status_breakdown": {},
            "by_candidate": [],
            "all_rows": [],
        }

    conn = duckdb.connect(str(resolved), read_only=True)

    try:
        # ── 1. Fetch every row ──────────────────────────────────────────────
        rows = conn.execute("""
            SELECT
                id,
                candidate_id,
                source_uid,
                raw_title,
                raw_company,
                raw_location,
                raw_zip,
                raw_contact_info,
                processing_status,
                extracted_at,
                created_at
            FROM raw_job_listings
            ORDER BY id
        """).fetchall()

        # ── 2. Status breakdown ─────────────────────────────────────────────
        status_rows = conn.execute(
            "SELECT processing_status, COUNT(*) FROM raw_job_listings GROUP BY 1"
        ).fetchall()
        status_breakdown = {s: c for s, c in status_rows}

    finally:
        conn.close()

    # ── 3. Build flat row list ─────────────────────────────────────────────
    all_rows: List[Dict] = []
    by_candidate_map: Dict[int, Dict] = {}

    for r in rows:
        (row_id, cand_id, src_uid, title, company, location,
         zip_code, contact_raw, status, extracted_at, created_at) = r

        contact_info = {}
        if contact_raw:
            try:
                contact_info = json.loads(contact_raw)
            except Exception:
                pass

        row_dict = {
            "id":            row_id,
            "candidate_id":  cand_id,
            "source_uid":    src_uid,
            "name":          contact_info.get("name"),
            "email":         contact_info.get("email"),
            "phone":         contact_info.get("phone"),
            "linkedin":      contact_info.get("linkedin"),
            "job_title":     title,
            "company":       company,
            "location":      location,
            "zip_code":      zip_code,
            "status":        status,
            "extracted_at":  str(extracted_at) if extracted_at else None,
            "created_at":    str(created_at)   if created_at   else None,
        }
        all_rows.append(row_dict)

        # Group by candidate
        cid = cand_id or 0
        if cid not in by_candidate_map:
            by_candidate_map[cid] = {
                "candidate_id":      cid,
                "positions_inserted": 0,
                "contacts": [],
            }
        by_candidate_map[cid]["positions_inserted"] += 1
        by_candidate_map[cid]["contacts"].append(row_dict)

    # Sort candidates by most contacts first
    by_candidate = sorted(
        by_candidate_map.values(),
        key=lambda x: x["positions_inserted"],
        reverse=True,
    )

    return {
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "db_path":          str(resolved),
        "total_rows":       len(all_rows),
        "status_breakdown": status_breakdown,
        "summary": {
            "total_contacts_inserted":   len(all_rows),
            "candidates_with_data":     len(by_candidate),
            "top_candidate_id":         by_candidate[0]["candidate_id"] if by_candidate else None,
            "top_candidate_count":      by_candidate[0]["positions_inserted"] if by_candidate else 0,
        },
        "by_candidate": by_candidate,
        "all_rows":     all_rows,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Write to file
# ─────────────────────────────────────────────────────────────────────────────

def _next_run_number(data_dir: Path) -> int:
    """Return the next sequential run number by scanning existing duckdb_logs_N.json files."""
    existing = list(data_dir.glob("duckdb_logs_*.json"))
    nums = []
    for p in existing:
        stem = p.stem  # e.g. "duckdb_logs_3"
        part = stem.split("_")[-1]
        if part.isdigit():
            nums.append(int(part))
    return max(nums, default=0) + 1


def write_duckdb_log(db_path=None, log_path=None) -> Path:
    """
    Generate the DuckDB log and write two files:
      1. data/duckdb_logs_<N>.json  — numbered per-run archive
      2. data/duckdb_logs.json      — always the latest run (convenience alias)

    Returns the numbered path.
    """
    log = build_duckdb_log(db_path)
    data_dir = _LOG_PATH.parent
    data_dir.mkdir(parents=True, exist_ok=True)

    # ── Numbered file ──────────────────────────────────────────────────────
    run_num     = _next_run_number(data_dir)
    log["run_number"] = run_num          # embed the run number in the log itself
    numbered_path = data_dir / f"duckdb_logs_{run_num}.json"
    numbered_path.write_text(json.dumps(log, indent=2, default=str), encoding="utf-8")

    # ── Latest alias ───────────────────────────────────────────────────────
    _LOG_PATH.write_text(json.dumps(log, indent=2, default=str), encoding="utf-8")

    logger.info(
        "DuckDB log written → run #%d  |  %s  (%d rows, %d candidates)",
        run_num, numbered_path, log["total_rows"], len(log["by_candidate"])
    )
    return numbered_path


# ─────────────────────────────────────────────────────────────────────────────
# CLI: pretty-print summary
# ─────────────────────────────────────────────────────────────────────────────

def _print_summary(log: Dict) -> None:
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    CYAN   = "\033[96m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    RESET  = "\033[0m"

    print()
    print(BOLD + CYAN + "╔══════════════════════════════════════════════════════════╗" + RESET)
    print(BOLD + CYAN + "║           DuckDB raw_job_listings — Run Log              ║" + RESET)
    print(BOLD + CYAN + "╚══════════════════════════════════════════════════════════╝" + RESET)
    print(f"  Generated : {log['generated_at']}")
    print(f"  DB file   : {log['db_path']}")

    if "error" in log:
        print(f"\n  {YELLOW}⚠  {log['error']}{RESET}\n")
        return

    s = log.get("summary", {})
    print()
    print(BOLD + f"  Total contacts in DB : {s.get('total_contacts_inserted', 0)}" + RESET)
    print(f"  Candidates with data : {s.get('candidates_with_data', 0)}")
    print(f"  Status breakdown     : {log.get('status_breakdown', {})}")
    print()

    print(BOLD + "  ── Per-Candidate Breakdown ──────────────────────────────────" + RESET)
    print(f"  {'CAND ID':>8}  {'# CONTACTS':>10}  {'NAMES / EMAILS'}")
    print("  " + "─" * 70)

    for cand in log.get("by_candidate", []):
        cid    = cand["candidate_id"]
        count  = cand["positions_inserted"]
        names  = ", ".join(
            f"{c.get('name') or '?'} <{c.get('email') or '?'}>"
            for c in cand["contacts"][:3]
        )
        if len(cand["contacts"]) > 3:
            names += f"  …+{len(cand['contacts']) - 3} more"
        colour = GREEN if count >= 2 else YELLOW
        print(f"  {colour}{cid:>8}  {count:>10}  {names}{RESET}")

    print()
    print(BOLD + "  ── Full Detail (all rows) ───────────────────────────────────" + RESET)
    for row in log.get("all_rows", []):
        print(f"  [{row['id']:>3}] cand={row['candidate_id']}  "
              f"{DIM}{row['name'] or '—':<22}{RESET}  "
              f"{row['email'] or '—':<30}  "
              f"{row['job_title'] or '—':<30}  "
              f"{row['company'] or '—':<20}  "
              f"{row['location'] or '—'}")

    print()
    print(DIM + f"  Full JSON saved to: {_LOG_PATH}" + RESET)
    print()


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.WARNING)

    numbered_path = write_duckdb_log()
    log = json.loads(numbered_path.read_text(encoding="utf-8"))
    _print_summary(log)
    run_num = log.get("run_number", "?")
    print(f"  ✓ Run #{run_num} log written:")
    print(f"      Numbered : {numbered_path}")
    print(f"      Latest   : {_LOG_PATH}\n")
    sys.exit(0)
