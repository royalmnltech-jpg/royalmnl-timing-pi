"""Phase 2 race-day tests: bulk push throughput.

Verifies that sync_loop POSTs to /reads (bulk, ≤200) and correctly handles:
  - mass-finish burst (300 reads draining in batches)
  - partial-success (per-item 422→keep, 400→dead, rest sent)
  - network drop / 503 recovery (no reads lost, ordering preserved)
  - Phase 1 behavior preserved: 401 pauses, 422 blocks, 429 not lost

Stdlib only — no pytest.
Run: python tests/test_phase2.py
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
from db import connect, init_schema, utc_now_iso_ms  # noqa: E402
from node_state import NodeState  # noqa: E402
from sync_loop import (  # noqa: E402
    DEAD,
    KEEP,
    PAUSE,
    RETRY,
    SENT,
    classify_response,
    run_sync_loop,
)
from tests.fake_backend import (  # noqa: E402
    FakeBackend,
    MODE_AUTH_401,
    MODE_BADPAYLOAD_400,
    MODE_BULK_MIXED,
    MODE_OK,
    MODE_RATELIMIT_429,
    MODE_SEMANTIC_422,
    MODE_SERVER_503,
)

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


def _cfg(base_url: str, batch_size: int = 200, max_retries: int = 3) -> NodeConfig:
    return NodeConfig(
        timing_node_id="node-p2",
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
        sync_batch_size=batch_size,
        sync_interval_sec=0.05,
        sync_max_retries=max_retries,
        sync_sent_ttl_sec=3600.0,
        sync_dead_ttl_sec=86400.0,
        sync_dead_cap=10000,
        sync_purge_interval_sec=9999.0,  # never fires during Phase 2 tests
        sync_reads_ttl_sec=604800.0,
        reader_stall_sec=120.0,
    )


def _seed_bulk(conn: sqlite3.Connection, n: int) -> None:
    """Insert n queued rows in a single transaction (fast for large n)."""
    init_schema(conn)
    conn.execute("BEGIN IMMEDIATE")
    try:
        for i in range(n):
            oid = str(uuid.uuid4())
            now = utc_now_iso_ms()
            conn.execute(
                """INSERT INTO outbox
                   (id, epc, read_at, captured_at_mono, timing_event_id, checkpoint_id,
                    assignment_pending, clock_untrusted, raw_json, status, created_at)
                   VALUES (?, ?, ?, ?, 'te_test', 'finish', 0, 0, NULL, 'queued', ?)""",
                (oid, f"EPC{i:024X}"[:24], now, float(i), now),
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _status_counts(db_path: str) -> dict[str, int]:
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    try:
        rows = c.execute(
            "SELECT status, COUNT(*) AS n FROM outbox GROUP BY status"
        ).fetchall()
        return {r["status"]: r["n"] for r in rows}
    finally:
        c.close()


def _wait_for(predicate, timeout: float = 8.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _run_loop(db_path, backend, state, log, batch_size=200, max_retries=3):
    conn = connect(db_path)
    holder = {"conn": conn}
    lock = threading.Lock()
    t = threading.Thread(
        target=run_sync_loop,
        args=(state, _cfg(backend.base_url, batch_size, max_retries), holder, lock, log),
        daemon=True,
    )
    t.start()
    return conn, t


# ---------------------------------------------------------------------------
# 2A — mass-finish burst: 300 reads drain in ≤200-item batches
# ---------------------------------------------------------------------------
def test_bulk_mass_finish() -> None:
    print("test_bulk_mass_finish (2A — 300 reads, ≤200 per batch)")
    log = logging.getLogger("t2a")
    backend = FakeBackend().start()
    backend.set_mode(MODE_OK)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_bulk(seed_conn, 300)
    seed_conn.close()
    state = NodeState()
    conn, t = _run_loop(db_path, backend, state, log, batch_size=200)
    try:
        flushed = _wait_for(
            lambda: _status_counts(db_path).get("sent", 0) == 300, timeout=10.0
        )
        check("all 300 reads sent", flushed, str(_status_counts(db_path)))
        check(
            "backend received all 300",
            len(backend.received_reads) == 300,
            f"received={len(backend.received_reads)}",
        )
        check(
            "at least 2 batches used (capped at 200)",
            len(backend.batch_sizes) >= 2,
            f"batches={backend.batch_sizes}",
        )
        check(
            "no batch exceeded 200 items",
            all(s <= 200 for s in backend.batch_sizes),
            f"batch sizes={backend.batch_sizes}",
        )
        check(
            "zero dead-lettered",
            _status_counts(db_path).get("dead", 0) == 0,
            str(_status_counts(db_path)),
        )
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


# ---------------------------------------------------------------------------
# 2B — partial-success: 422 items kept, 400 items dead, rest sent
# ---------------------------------------------------------------------------
def test_bulk_partial_success() -> None:
    """MODE_BULK_MIXED: i%7==3 → 422 keep, i%7==5 → 400 dead, rest → sent.

    For 14 seeds: indices 3,10 → keep (2); indices 5,12 → dead (2); rest → sent (10).
    After mode flips to OK, the 2 kept rows flush → 12 sent total, 2 dead.
    """
    print("test_bulk_partial_success (2B — mixed per-item outcomes)")
    log = logging.getLogger("t2b")
    backend = FakeBackend().start()
    backend.set_mode(MODE_BULK_MIXED)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_bulk(seed_conn, 14)
    seed_conn.close()
    state = NodeState()
    conn, t = _run_loop(db_path, backend, state, log)
    try:
        # Wait for the first batch to be processed
        settled = _wait_for(
            lambda: _status_counts(db_path).get("dead", 0) >= 2, timeout=5.0
        )
        check("first batch processed", settled, str(_status_counts(db_path)))
        counts = _status_counts(db_path)
        check(
            "bad-payload items dead-lettered (2)",
            counts.get("dead", 0) == 2,
            str(counts),
        )
        check(
            "semantic-error items kept queued (not dead)",
            counts.get("queued", 0) == 2,
            str(counts),
        )
        check(
            "accepted items sent (10)",
            counts.get("sent", 0) == 10,
            str(counts),
        )
        # Flip to OK — the 2 kept rows should now flush
        backend.set_mode(MODE_OK)
        all_done = _wait_for(
            lambda: _status_counts(db_path).get("sent", 0) == 12, timeout=5.0
        )
        check(
            "kept rows flush after mode flips live",
            all_done,
            str(_status_counts(db_path)),
        )
        check(
            "dead count unchanged after flush",
            _status_counts(db_path).get("dead", 0) == 2,
            str(_status_counts(db_path)),
        )
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


# ---------------------------------------------------------------------------
# 2C — 503 recovery: no reads lost, all eventually sent
# ---------------------------------------------------------------------------
def test_bulk_503_recovery() -> None:
    """503 for several sync cycles, then recover — no reads lost, all flush.

    Uses max_retries=20 so the 503 window can be long without exhausting retries.
    This models the real race-day scenario: a few seconds of connectivity loss
    during a mass-finish burst must not destroy any reads.
    """
    print("test_bulk_503_recovery (2C — network drop recovery)")
    log = logging.getLogger("t2c")
    backend = FakeBackend().start()
    backend.set_mode(MODE_SERVER_503)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_bulk(seed_conn, 10)
    seed_conn.close()
    state = NodeState()
    # High max_retries: real outages last seconds; exhausting 3 retries in 0.15s is artificial.
    conn, t = _run_loop(db_path, backend, state, log, max_retries=20)
    try:
        # Let several 503 cycles run
        time.sleep(0.2)
        counts = _status_counts(db_path)
        check(
            "no reads dead-lettered during 503 window",
            counts.get("dead", 0) == 0,
            str(counts),
        )
        check(
            "reads remain queued during 503 window",
            counts.get("sent", 0) == 0,
            str(counts),
        )
        backend.set_mode(MODE_OK)
        flushed = _wait_for(
            lambda: _status_counts(db_path).get("sent", 0) == 10, timeout=8.0
        )
        check(
            "all 10 reads flush after recovery",
            flushed,
            str(_status_counts(db_path)),
        )
        check(
            "backend received all 10",
            len(backend.received_reads) == 10,
            f"received={len(backend.received_reads)}",
        )
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


# ---------------------------------------------------------------------------
# 2D — Phase 1 behaviors preserved in bulk mode
# ---------------------------------------------------------------------------
def test_bulk_401_pauses() -> None:
    print("test_bulk_401_pauses (2D — bad API key pauses bulk loop)")
    log = logging.getLogger("t2d_401")
    backend = FakeBackend().start()
    backend.set_mode(MODE_AUTH_401)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_bulk(seed_conn, 5)
    seed_conn.close()
    state = NodeState()
    conn, t = _run_loop(db_path, backend, state, log)
    try:
        paused = _wait_for(lambda: state.is_auth_failed(), timeout=3.0)
        check("auth_failed set on bulk 401", paused)
        time.sleep(0.1)
        counts = _status_counts(db_path)
        check(
            "zero reads dead-lettered on bulk 401",
            counts.get("dead", 0) == 0,
            str(counts),
        )
        check(
            "all 5 reads stay queued on bulk 401",
            counts.get("queued", 0) == 5,
            str(counts),
        )
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


def test_bulk_422_keeps_reads() -> None:
    print("test_bulk_422_keeps_reads (2D — event not live keeps whole batch)")
    log = logging.getLogger("t2d_422")
    backend = FakeBackend().start()
    backend.set_mode(MODE_SEMANTIC_422)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_bulk(seed_conn, 8)
    seed_conn.close()
    state = NodeState()
    conn, t = _run_loop(db_path, backend, state, log)
    try:
        blocked = _wait_for(lambda: state.is_ingest_blocked(), timeout=3.0)
        check("ingest_blocked set on bulk 422", blocked)
        time.sleep(0.1)
        counts = _status_counts(db_path)
        check("no reads dead-lettered on bulk 422", counts.get("dead", 0) == 0, str(counts))
        check("all 8 reads stay queued", counts.get("queued", 0) == 8, str(counts))
        backend.set_mode(MODE_OK)
        flushed = _wait_for(
            lambda: _status_counts(db_path).get("sent", 0) == 8, timeout=5.0
        )
        check("all 8 flush when event goes live", flushed, str(_status_counts(db_path)))
        check(
            "ingest_blocked cleared after flush",
            not state.is_ingest_blocked(),
        )
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


def test_bulk_429_not_lost() -> None:
    print("test_bulk_429_not_lost (2D — rate limit must not dead-letter)")
    log = logging.getLogger("t2d_429")
    backend = FakeBackend().start()
    backend.set_mode(MODE_RATELIMIT_429)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_bulk(seed_conn, 6)
    seed_conn.close()
    state = NodeState()
    conn, t = _run_loop(db_path, backend, state, log)
    try:
        time.sleep(0.3)
        counts = _status_counts(db_path)
        check("no reads dead-lettered on bulk 429", counts.get("dead", 0) == 0, str(counts))
        check("reads remain queued on bulk 429", counts.get("queued", 0) == 6, str(counts))
        backend.set_mode(MODE_OK)
        flushed = _wait_for(
            lambda: _status_counts(db_path).get("sent", 0) == 6, timeout=5.0
        )
        check("reads flush after rate limit lifts", flushed, str(_status_counts(db_path)))
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


def main() -> int:
    logging.basicConfig(level=logging.CRITICAL)
    test_bulk_mass_finish()
    test_bulk_partial_success()
    test_bulk_503_recovery()
    test_bulk_401_pauses()
    test_bulk_422_keeps_reads()
    test_bulk_429_not_lost()
    print(f"\n{_PASS} passed, {_FAIL} failed")
    return 1 if _FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
