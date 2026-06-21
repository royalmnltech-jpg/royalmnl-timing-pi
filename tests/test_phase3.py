"""Phase 3 race-day tests: retention / purge.

Verifies:
  - Old sent rows purged, fresh sent and queued rows untouched
  - Stale dead rows purged, fresh dead and queued rows untouched
  - Dead-letter cap enforced: oldest deleted, newest kept
  - Old raw reads purged, fresh reads untouched
  - get_outbox_depth / get_oldest_queued_read_at helpers
  - Maintenance tick fires inside sync_loop and cleans up old sent rows
  - Outbox row count stays bounded across multiple race "rounds" (no accumulation)

Stdlib only — no pytest.
Run: python tests/test_phase3.py
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import tempfile
import threading
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import NodeConfig  # noqa: E402
from db import (  # noqa: E402
    cap_dead_letter_rows,
    connect,
    get_oldest_queued_read_at,
    get_outbox_depth,
    init_schema,
    mark_read_dead,
    mark_read_sent,
    purge_old_reads,
    purge_sent_rows,
    purge_stale_dead_rows,
    utc_now_iso_ms,
)
from node_state import NodeState  # noqa: E402
from sync_loop import run_sync_loop  # noqa: E402
from tests.fake_backend import FakeBackend, MODE_OK  # noqa: E402

_PASS = 0
_FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global _PASS, _FAIL
    if cond:
        _PASS += 1
        print(f"  PASS  {name}")
    else:
        _FAIL += 1
        print(f"  FAIL  {name}  {detail}")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    init_schema(conn)
    return conn


def _insert_outbox(
    conn: sqlite3.Connection,
    *,
    status: str,
    sent_at: str | None = None,
    created_at: str | None = None,
) -> str:
    oid = str(uuid.uuid4())
    now = utc_now_iso_ms()
    conn.execute(
        """INSERT INTO outbox
           (id, epc, read_at, captured_at_mono, timing_event_id, checkpoint_id,
            assignment_pending, clock_untrusted, raw_json, status, sent_at,
            dead_letter_reason, created_at)
           VALUES (?, ?, ?, 0.0, 'te_test', 'finish', 0, 0, NULL, ?, ?, NULL, ?)""",
        (oid, "AABBCCDDEEFF001122334455", now, status, sent_at, created_at or now),
    )
    return oid


def _insert_read(conn: sqlite3.Connection, *, created_at: str | None = None) -> str:
    rid = str(uuid.uuid4())
    now = utc_now_iso_ms()
    conn.execute(
        """INSERT INTO reads (id, epc, read_at, captured_at_mono, raw_json, created_at)
           VALUES (?, ?, ?, 0.0, NULL, ?)""",
        (rid, "AABBCCDDEEFF001122334455", now, created_at or now),
    )
    return rid


def _old_ts() -> str:
    """ISO timestamp 2 days ago — older than any TTL used in tests."""
    from datetime import datetime, timedelta, timezone
    dt = datetime.now(timezone.utc) - timedelta(days=2)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"


def _count(conn: sqlite3.Connection, table: str, where: str = "") -> int:
    sql = f"SELECT COUNT(*) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    return conn.execute(sql).fetchone()[0]


def _row_count_file(db_path: str, table: str, status: str | None = None) -> int:
    c = sqlite3.connect(db_path)
    try:
        if status:
            return c.execute(
                f"SELECT COUNT(*) FROM {table} WHERE status = ?", (status,)
            ).fetchone()[0]
        return c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        c.close()


# ---------------------------------------------------------------------------
# 3A — purge_sent_rows
# ---------------------------------------------------------------------------
def test_purge_sent_rows() -> None:
    print("test_purge_sent_rows (3A — old sent deleted, fresh + queued kept)")
    conn = _fresh_db()

    old_ts = _old_ts()
    # 5 old sent (sent_at = 2 days ago)
    for _ in range(5):
        _insert_outbox(conn, status="sent", sent_at=old_ts, created_at=old_ts)
    # 3 fresh sent (sent_at = now)
    now_ts = utc_now_iso_ms()
    for _ in range(3):
        _insert_outbox(conn, status="sent", sent_at=now_ts)
    # 4 queued
    for _ in range(4):
        _insert_outbox(conn, status="queued")

    # Purge with 1-hour TTL — 2-day-old rows should go, fresh should stay
    deleted = purge_sent_rows(conn, max_age_sec=3600)

    check("5 old sent rows deleted", deleted == 5, f"deleted={deleted}")
    check(
        "3 fresh sent rows remain",
        _count(conn, "outbox", "status='sent'") == 3,
        str(_count(conn, "outbox", "status='sent'")),
    )
    check(
        "4 queued rows untouched",
        _count(conn, "outbox", "status='queued'") == 4,
    )


# ---------------------------------------------------------------------------
# 3B — purge_stale_dead_rows
# ---------------------------------------------------------------------------
def test_purge_stale_dead_rows() -> None:
    print("test_purge_stale_dead_rows (3B — old dead deleted, fresh + queued kept)")
    conn = _fresh_db()

    old_ts = _old_ts()
    for _ in range(6):
        _insert_outbox(conn, status="dead", created_at=old_ts)
    for _ in range(2):
        _insert_outbox(conn, status="dead")
    for _ in range(3):
        _insert_outbox(conn, status="queued")

    deleted = purge_stale_dead_rows(conn, max_age_sec=3600)

    check("6 stale dead rows deleted", deleted == 6, f"deleted={deleted}")
    check(
        "2 fresh dead rows remain",
        _count(conn, "outbox", "status='dead'") == 2,
    )
    check(
        "3 queued rows untouched",
        _count(conn, "outbox", "status='queued'") == 3,
    )


# ---------------------------------------------------------------------------
# 3C — cap_dead_letter_rows
# ---------------------------------------------------------------------------
def test_dead_letter_cap() -> None:
    print("test_dead_letter_cap (3C — oldest deleted, cap enforced)")
    conn = _fresh_db()

    cap = 100
    # Insert cap+50 dead rows; older rows have lexicographically earlier created_at.
    from datetime import datetime, timedelta, timezone
    base_dt = datetime.now(timezone.utc) - timedelta(hours=1)
    for i in range(cap + 50):
        dt = base_dt + timedelta(seconds=i)
        ts = dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"
        oid = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO outbox
               (id, epc, read_at, captured_at_mono, timing_event_id, checkpoint_id,
                assignment_pending, clock_untrusted, raw_json, status, created_at)
               VALUES (?, ?, ?, 0.0, 'te_test', 'finish', 0, 0, NULL, 'dead', ?)""",
            (oid, "AABBCCDDEEFF001122334455", ts, ts),
        )
    # 4 queued rows (must be untouched)
    for _ in range(4):
        _insert_outbox(conn, status="queued")

    deleted = cap_dead_letter_rows(conn, cap)

    check("50 excess dead rows deleted", deleted == 50, f"deleted={deleted}")
    check(
        f"exactly {cap} dead rows remain",
        _count(conn, "outbox", "status='dead'") == cap,
        str(_count(conn, "outbox", "status='dead'")),
    )
    check(
        "4 queued rows untouched",
        _count(conn, "outbox", "status='queued'") == 4,
    )

    # Verify the OLDEST rows were removed (newest kept).
    # The oldest row had created_at = base_dt + 0s; the newest = base_dt + 149s.
    # After capping at 100, rows base_dt+0 through base_dt+49 should be gone.
    oldest_remaining = conn.execute(
        "SELECT MIN(created_at) FROM outbox WHERE status = 'dead'"
    ).fetchone()[0]
    oldest_expected = (base_dt + timedelta(seconds=50)).strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"
    check(
        "oldest remaining dead row is index 50 (not 0)",
        oldest_remaining == oldest_expected,
        f"got={oldest_remaining} want={oldest_expected}",
    )


# ---------------------------------------------------------------------------
# 3D — purge_old_reads
# ---------------------------------------------------------------------------
def test_purge_old_reads() -> None:
    print("test_purge_old_reads (3D — old raw reads deleted, fresh kept)")
    conn = _fresh_db()

    old_ts = _old_ts()
    for _ in range(7):
        _insert_read(conn, created_at=old_ts)
    for _ in range(3):
        _insert_read(conn)

    deleted = purge_old_reads(conn, max_age_sec=3600)

    check("7 old reads deleted", deleted == 7, f"deleted={deleted}")
    check(
        "3 fresh reads remain",
        _count(conn, "reads") == 3,
        str(_count(conn, "reads")),
    )


# ---------------------------------------------------------------------------
# 3E — telemetry helpers
# ---------------------------------------------------------------------------
def test_depth_helpers() -> None:
    print("test_depth_helpers (3E — get_outbox_depth + get_oldest_queued_read_at)")
    conn = _fresh_db()

    check("depth is 0 on empty outbox", get_outbox_depth(conn) == 0)
    check("oldest is None on empty outbox", get_oldest_queued_read_at(conn) is None)

    # Insert queued rows with known read_at values (oldest first by design)
    from datetime import datetime, timedelta, timezone
    base_dt = datetime.now(timezone.utc) - timedelta(minutes=5)
    ids = []
    for i in range(5):
        dt = base_dt + timedelta(seconds=i * 10)
        read_at = dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"
        oid = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO outbox
               (id, epc, read_at, captured_at_mono, timing_event_id, checkpoint_id,
                assignment_pending, clock_untrusted, raw_json, status, created_at)
               VALUES (?, ?, ?, 0.0, 'te_test', 'finish', 0, 0, NULL, 'queued', ?)""",
            (oid, "AABBCCDDEEFF001122334455", read_at, utc_now_iso_ms()),
        )
        ids.append((oid, read_at))
    # 2 sent rows (must not count toward depth)
    for _ in range(2):
        _insert_outbox(conn, status="sent", sent_at=utc_now_iso_ms())
    # 1 assignment_pending row (must not count toward depth)
    conn.execute(
        """INSERT INTO outbox
           (id, epc, read_at, captured_at_mono, timing_event_id, checkpoint_id,
            assignment_pending, clock_untrusted, raw_json, status, created_at)
           VALUES (?, ?, ?, 0.0, NULL, NULL, 1, 0, NULL, 'queued', ?)""",
        (str(uuid.uuid4()), "AABBCCDDEEFF001122334455", utc_now_iso_ms(), utc_now_iso_ms()),
    )

    depth = get_outbox_depth(conn)
    oldest = get_oldest_queued_read_at(conn)
    expected_oldest = ids[0][1]  # earliest read_at inserted

    check("depth counts only ready queued rows (5)", depth == 5, f"depth={depth}")
    check(
        "oldest queued read_at matches earliest row",
        oldest == expected_oldest,
        f"got={oldest} want={expected_oldest}",
    )


# ---------------------------------------------------------------------------
# 3F — maintenance tick fires in sync_loop
# ---------------------------------------------------------------------------
def _cfg_purge(base_url: str, purge_interval: float, sent_ttl: float) -> NodeConfig:
    return NodeConfig(
        timing_node_id="node-p3",
        timing_api_base_url=base_url,
        timing_api_key="test-key",
        reader_ip="127.0.0.1",
        reader_port=0,
        dedupe_window_sec=20.0,
        assignment_poll_sec=1.0,
        assignment_poll_stable_sec=5.0,
        timing_db_path=":memory:",
        lock_file_path="",
        log_level="INFO",
        sync_batch_size=200,
        sync_interval_sec=0.05,
        sync_max_retries=3,
        sync_sent_ttl_sec=sent_ttl,
        sync_dead_ttl_sec=86400.0,
        sync_dead_cap=10000,
        sync_purge_interval_sec=purge_interval,
        sync_reads_ttl_sec=604800.0,
        reader_stall_sec=120.0,
    )


def test_maintenance_tick_in_loop() -> None:
    """Maintenance tick runs inside sync_loop and purges old sent rows."""
    print("test_maintenance_tick_in_loop (3F — maintenance fires, old sent rows purged)")
    log = logging.getLogger("t3f")
    backend = FakeBackend().start()
    backend.set_mode(MODE_OK)

    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")

    # Pre-seed 10 old sent rows directly in the DB (past the sent_ttl we'll use)
    seed_conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
    seed_conn.row_factory = sqlite3.Row
    init_schema(seed_conn)
    old_ts = _old_ts()
    for _ in range(10):
        oid = str(uuid.uuid4())
        seed_conn.execute(
            """INSERT INTO outbox
               (id, epc, read_at, captured_at_mono, timing_event_id, checkpoint_id,
                assignment_pending, clock_untrusted, raw_json, status, sent_at, created_at)
               VALUES (?, ?, ?, 0.0, 'te_test', 'finish', 0, 0, NULL, 'sent', ?, ?)""",
            (oid, "AABBCCDDEEFF001122334455", old_ts, old_ts, old_ts),
        )
    # 5 fresh queued rows (these should sync normally)
    for i in range(5):
        oid = str(uuid.uuid4())
        now = utc_now_iso_ms()
        seed_conn.execute(
            """INSERT INTO outbox
               (id, epc, read_at, captured_at_mono, timing_event_id, checkpoint_id,
                assignment_pending, clock_untrusted, raw_json, status, created_at)
               VALUES (?, ?, ?, 0.0, 'te_test', 'finish', 0, 0, NULL, 'queued', ?)""",
            (oid, f"EPC{i:024X}"[:24], now, now),
        )
    seed_conn.close()

    state = NodeState()
    conn = connect(db_path)
    holder = {"conn": conn}
    lock = threading.Lock()
    # Short purge interval (0.05s) + very short sent_ttl (0.01s = 10ms)
    cfg = _cfg_purge(backend.base_url, purge_interval=0.05, sent_ttl=0.01)
    t = threading.Thread(
        target=run_sync_loop, args=(state, cfg, holder, lock, log), daemon=True
    )
    t.start()
    try:
        # Wait for queued rows to sync AND maintenance to run several times
        time.sleep(0.5)
        total = _row_count_file(db_path, "outbox")
        check(
            "old pre-seeded sent rows purged by maintenance tick",
            _row_count_file(db_path, "outbox", "sent") == 0,
            f"remaining sent={_row_count_file(db_path, 'outbox', 'sent')}",
        )
        check(
            "fresh queued rows synced (not dead)",
            _row_count_file(db_path, "outbox", "dead") == 0,
            f"dead={_row_count_file(db_path, 'outbox', 'dead')}",
        )
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


# ---------------------------------------------------------------------------
# 3G — bounded outbox: row count doesn't accumulate across race rounds
# ---------------------------------------------------------------------------
def test_outbox_bounded_across_rounds() -> None:
    """Two race rounds: sent rows purged after each; total stays near 0 between rounds."""
    print("test_outbox_bounded_across_rounds (3G — no row accumulation across rounds)")
    log = logging.getLogger("t3g")
    backend = FakeBackend().start()
    backend.set_mode(MODE_OK)

    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")

    state = NodeState()
    conn = connect(db_path)
    init_schema(conn)
    holder = {"conn": conn}
    lock = threading.Lock()
    # Short purge interval + very short sent_ttl to simulate time passing quickly
    cfg = _cfg_purge(backend.base_url, purge_interval=0.05, sent_ttl=0.05)
    t = threading.Thread(
        target=run_sync_loop, args=(state, cfg, holder, lock, log), daemon=True
    )
    t.start()
    try:
        def _seed_round(n: int) -> None:
            with lock:
                c = holder["conn"]
                if c is None:
                    return
                for i in range(n):
                    oid = str(uuid.uuid4())
                    now = utc_now_iso_ms()
                    c.execute(
                        """INSERT INTO outbox
                           (id, epc, read_at, captured_at_mono, timing_event_id, checkpoint_id,
                            assignment_pending, clock_untrusted, raw_json, status, created_at)
                           VALUES (?, ?, ?, 0.0, 'te_test', 'finish', 0, 0, NULL, 'queued', ?)""",
                        (oid, f"EPC{i:024X}"[:24], now, now),
                    )

        def _wait_outbox_below(threshold: int, timeout: float = 5.0) -> bool:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if _row_count_file(db_path, "outbox") < threshold:
                    return True
                time.sleep(0.05)
            return False

        # Round 1: seed 50 rows
        _seed_round(50)
        r1_drained = _wait_outbox_below(5, timeout=5.0)
        check(
            "round 1: outbox drains to near-zero after sync + purge",
            r1_drained,
            f"remaining={_row_count_file(db_path, 'outbox')}",
        )

        # Round 2: seed another 50 rows
        _seed_round(50)
        r2_drained = _wait_outbox_below(5, timeout=5.0)
        check(
            "round 2: outbox drains to near-zero (no accumulation from round 1)",
            r2_drained,
            f"remaining={_row_count_file(db_path, 'outbox')}",
        )
        check(
            "total backend reads = 100 (none lost across rounds)",
            len(backend.received_reads) == 100,
            f"received={len(backend.received_reads)}",
        )
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


def main() -> int:
    logging.basicConfig(level=logging.CRITICAL)
    test_purge_sent_rows()
    test_purge_stale_dead_rows()
    test_dead_letter_cap()
    test_purge_old_reads()
    test_depth_helpers()
    test_maintenance_tick_in_loop()
    test_outbox_bounded_across_rounds()
    print(f"\n{_PASS} passed, {_FAIL} failed")
    return 1 if _FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
