"""SQLite WAL persistence for timing-node outbox."""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS reads (
            id TEXT PRIMARY KEY,
            epc TEXT NOT NULL,
            read_at TEXT NOT NULL,
            captured_at_mono REAL NOT NULL,
            raw_json TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outbox (
            id TEXT PRIMARY KEY,
            epc TEXT NOT NULL,
            read_at TEXT NOT NULL,
            captured_at_mono REAL NOT NULL,
            timing_event_id TEXT,
            checkpoint_id TEXT,
            assignment_pending INTEGER NOT NULL DEFAULT 1,
            clock_untrusted INTEGER NOT NULL DEFAULT 0,
            raw_json TEXT,
            status TEXT NOT NULL DEFAULT 'queued',
            dead_letter_reason TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            sent_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status);
        CREATE INDEX IF NOT EXISTS idx_outbox_assignment_pending ON outbox(assignment_pending);
        """
    )


def utc_now_iso_ms() -> str:
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{dt.microsecond // 1000:03d}Z"


def insert_tag_read(
    conn: sqlite3.Connection,
    *,
    epc: str,
    read_at: str,
    captured_at_mono: float,
    timing_event_id: Optional[str],
    checkpoint_id: Optional[str],
    assignment_pending: bool,
    clock_untrusted: bool,
    raw: Optional[dict[str, Any]],
    log: logging.Logger,
) -> str:
    oid = str(uuid.uuid4())
    rid = str(uuid.uuid4())
    created = utc_now_iso_ms()
    raw_json = json.dumps(raw) if raw is not None else None

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            """
            INSERT INTO reads (id, epc, read_at, captured_at_mono, raw_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (rid, epc, read_at, captured_at_mono, raw_json, created),
        )
        conn.execute(
            """
            INSERT INTO outbox (
                id, epc, read_at, captured_at_mono,
                timing_event_id, checkpoint_id, assignment_pending, clock_untrusted,
                raw_json, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)
            """,
            (
                oid,
                epc,
                read_at,
                captured_at_mono,
                timing_event_id,
                checkpoint_id,
                1 if assignment_pending else 0,
                1 if clock_untrusted else 0,
                raw_json,
                created,
            ),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        log.exception("SQLite insert failed")
        raise
    return oid


def get_queued_for_sync(
    conn: sqlite3.Connection,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Return up to `limit` outbox rows that are ready to sync to the backend."""
    cur = conn.execute(
        """
        SELECT id, epc, read_at, timing_event_id, checkpoint_id, raw_json
        FROM outbox
        WHERE status = 'queued' AND assignment_pending = 0
        ORDER BY read_at ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def mark_read_sent(conn: sqlite3.Connection, row_id: str) -> None:
    """Mark an outbox row as successfully synced."""
    conn.execute(
        "UPDATE outbox SET status = 'sent', sent_at = ? WHERE id = ?",
        (utc_now_iso_ms(), row_id),
    )


def mark_read_dead(conn: sqlite3.Connection, row_id: str, reason: str) -> None:
    """Move an outbox row to dead-letter state (non-retryable failure)."""
    conn.execute(
        "UPDATE outbox SET status = 'dead', dead_letter_reason = ? WHERE id = ?",
        (reason[:500], row_id),
    )


def increment_retry(conn: sqlite3.Connection, row_id: str) -> int:
    """Increment retry_count; return the new count."""
    conn.execute(
        "UPDATE outbox SET retry_count = retry_count + 1 WHERE id = ?",
        (row_id,),
    )
    cur = conn.execute("SELECT retry_count FROM outbox WHERE id = ?", (row_id,))
    row = cur.fetchone()
    return row["retry_count"] if row else 0


def backfill_assignment(
    conn: sqlite3.Connection,
    *,
    timing_event_id: str,
    checkpoint_id: str,
    log: logging.Logger,
) -> int:
    """Attach event/checkpoint to rows that were captured before assignment arrived."""
    cur = conn.execute(
        """
        UPDATE outbox
        SET timing_event_id = ?, checkpoint_id = ?, assignment_pending = 0
        WHERE assignment_pending = 1 AND status = 'queued'
        """,
        (timing_event_id, checkpoint_id),
    )
    n = cur.rowcount if cur.rowcount is not None else 0
    if n:
        log.info("Backfilled %s queued row(s) with assignment %s / %s", n, timing_event_id, checkpoint_id)
    return n
