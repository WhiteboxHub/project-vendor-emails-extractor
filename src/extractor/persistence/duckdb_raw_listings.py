"""
duckdb_raw_listings.py
======================
Local DuckDB staging store for raw_job_listings.

Mirrors the production MySQL DDL so data can be observed locally for a few
days before committing the API integration.

Usage (called from vendor_contacts.py):
    from .duckdb_raw_listings import RawJobListingsDuckDB
    store = RawJobListingsDuckDB()
    inserted = store.insert_bulk(raw_job_listings_payload)
    store.close()
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Project root = 4 levels above this file
# src/extractor/persistence/duckdb_raw_listings.py
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB_PATH = _PROJECT_ROOT / "data" / "raw_job_listings.duckdb"

_DDL = """
CREATE SEQUENCE IF NOT EXISTS raw_job_listings_id_seq START 1;

CREATE TABLE IF NOT EXISTS raw_job_listings (
    id               BIGINT PRIMARY KEY DEFAULT nextval('raw_job_listings_id_seq'),
    candidate_id     INTEGER,
    source           VARCHAR(50)  NOT NULL DEFAULT 'email',
    source_uid       VARCHAR(255),
    extracted_at     TIMESTAMP    NOT NULL DEFAULT now(),
    extractor_version VARCHAR(50),
    raw_title        VARCHAR(500),
    raw_company      VARCHAR(255),
    raw_location     VARCHAR(255),
    raw_zip          VARCHAR(20),
    raw_description  TEXT,
    raw_contact_info TEXT,
    raw_notes        TEXT,
    raw_payload      JSON,
    processing_status VARCHAR(20)  NOT NULL DEFAULT 'new'
                         CHECK (processing_status IN ('new','parsed','mapped','discarded','error')),
    error_message    TEXT,
    processed_at     TIMESTAMP,
    created_at       TIMESTAMP    NOT NULL DEFAULT now()
);
"""


class RawJobListingsDuckDB:
    """
    Local DuckDB staging store that mirrors the raw_job_listings MySQL table.

    The DB file is created at <project_root>/data/raw_job_listings.duckdb and
    is safe to inspect at any time with the DuckDB CLI or Python REPL while
    the bot is NOT running.
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Args:
            db_path: Absolute path to the DuckDB file.
                     Defaults to <project_root>/data/raw_job_listings.duckdb.
        """
        try:
            import duckdb  # lazy import so missing package is a soft error
        except ImportError:
            raise ImportError(
                "duckdb package is required for local staging. "
                "Run: pip install duckdb>=0.10.0"
            )

        self._duckdb = duckdb
        resolved = Path(db_path) if db_path else _DEFAULT_DB_PATH
        resolved.parent.mkdir(parents=True, exist_ok=True)

        self.db_path = str(resolved)
        self.conn = duckdb.connect(self.db_path)
        self._ensure_schema()
        logger.info("DuckDB raw_job_listings store opened: %s", self.db_path)

    # ─────────────────────────────────────────────────────────────────────────
    # Schema
    # ─────────────────────────────────────────────────────────────────────────

    def _ensure_schema(self):
        """Create table and sequence if they don't exist yet."""
        statements = [s.strip() for s in _DDL.strip().split(";") if s.strip()]
        for stmt in statements:
            try:
                self.conn.execute(stmt)
            except Exception as exc:
                logger.error("DuckDB schema statement failed: %s\nStatement: %s", exc, stmt)
                raise

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def insert_bulk(self, rows: List[Dict]) -> int:
        """
        Insert a batch of raw_job_listings rows into DuckDB.

        Each row dict should match the payload built by
        VendorUtil._build_raw_job_listings_payload().

        Returns:
            Number of rows successfully inserted.
        """
        if not rows:
            logger.debug("DuckDB insert_bulk called with 0 rows — skipping")
            return 0

        inserted = 0
        now = datetime.utcnow().isoformat()

        for row in rows:
            try:
                raw_payload = row.get("raw_payload")
                if raw_payload is not None and not isinstance(raw_payload, str):
                    raw_payload = json.dumps(raw_payload, default=str)

                raw_contact_info = row.get("raw_contact_info")
                if raw_contact_info is not None and not isinstance(raw_contact_info, str):
                    raw_contact_info = json.dumps(raw_contact_info, default=str)

                self.conn.execute(
                    """
                    INSERT INTO raw_job_listings (
                        candidate_id, source, source_uid, extracted_at,
                        extractor_version, raw_title, raw_company, raw_location,
                        raw_zip, raw_description, raw_contact_info, raw_notes,
                        raw_payload, processing_status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        row.get("candidate_id"),
                        row.get("source", "email"),
                        str(row.get("source_uid", "")) or None,
                        now,
                        row.get("extractor_version"),
                        row.get("raw_title"),
                        row.get("raw_company"),
                        row.get("raw_location"),
                        row.get("raw_zip"),
                        row.get("raw_description"),
                        raw_contact_info,
                        row.get("raw_notes"),
                        raw_payload,
                        row.get("processing_status", "new"),
                        now,
                    ],
                )
                inserted += 1
            except Exception as exc:
                logger.warning(
                    "DuckDB: failed to insert row (candidate_id=%s): %s",
                    row.get("candidate_id"),
                    exc,
                )

        logger.info("DuckDB: inserted %d / %d raw_job_listings rows", inserted, len(rows))
        return inserted

    def close(self):
        """Close the DuckDB connection."""
        try:
            self.conn.close()
            logger.debug("DuckDB connection closed")
        except Exception as exc:
            logger.warning("DuckDB close error: %s", exc)

    # ─────────────────────────────────────────────────────────────────────────
    # Convenience — quick stats for logging
    # ─────────────────────────────────────────────────────────────────────────

    def get_stats(self) -> Dict:
        """Return row counts grouped by processing_status."""
        try:
            rows = self.conn.execute(
                "SELECT processing_status, COUNT(*) AS cnt "
                "FROM raw_job_listings GROUP BY processing_status"
            ).fetchall()
            return {status: cnt for status, cnt in rows}
        except Exception as exc:
            logger.warning("DuckDB stats query failed: %s", exc)
            return {}
