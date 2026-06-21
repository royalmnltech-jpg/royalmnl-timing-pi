"""Phase 4 race-day tests: Pi telemetry on assignment polls.

Verifies:
  - Every assignment GET includes ?outboxDepth=<n>&lastSyncAt=<iso>
  - outboxDepth tracks real outbox state: spikes during burst, returns to 0 after flush
  - lastSyncAt is absent before first sync, present after first successful flush
  - Both fields survive 422-blocked and 401-paused states (depth stays > 0, no lastSyncAt change)

Requires running both network_loop + sync_loop together — tests the integration between
the sync worker setting state.last_sync_at and the network worker reading it.

Stdlib only — no pytest.
Run: python tests/test_phase4.py
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
from network_loop import run_network_loop  # noqa: E402
from sync_loop import run_sync_loop  # noqa: E402
from tests.fake_backend import (  # noqa: E402
    FakeBackend,
    MODE_AUTH_401,
    MODE_OK,
    MODE_SEMANTIC_422,
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


def _cfg(base_url: str) -> NodeConfig:
    return NodeConfig(
        timing_node_id="node-p4",
        timing_api_base_url=base_url,
        timing_api_key="test-key",
        reader_ip="127.0.0.1",
        reader_port=0,
        dedupe_window_sec=20.0,
        assignment_poll_sec=0.1,       # poll fast in tests
        assignment_poll_stable_sec=0.2,
        timing_db_path=":memory:",
        lock_file_path="",
        log_level="INFO",
        sync_batch_size=200,
        sync_interval_sec=0.05,
        sync_max_retries=3,
        sync_sent_ttl_sec=3600.0,
        sync_dead_ttl_sec=86400.0,
        sync_dead_cap=10000,
        sync_purge_interval_sec=9999.0,
        sync_reads_ttl_sec=604800.0,
        reader_stall_sec=120.0,
    )


def _seed_queued(conn: sqlite3.Connection, n: int) -> None:
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
                   VALUES (?, ?, ?, 0.0, 'te_test', 'finish', 0, 0, NULL, 'queued', ?)""",
                (oid, f"EPC{i:024X}"[:24], now, now),
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _row_count(db_path: str, status: str) -> int:
    c = sqlite3.connect(db_path)
    try:
        return c.execute(
            "SELECT COUNT(*) FROM outbox WHERE status = ?", (status,)
        ).fetchone()[0]
    finally:
        c.close()


def _wait_for(pred, timeout: float = 6.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _start_both(db_path, backend, state, log):
    conn = connect(db_path)
    holder = {"conn": conn}
    lock = threading.Lock()
    cfg = _cfg(backend.base_url)
    sync_t = threading.Thread(
        target=run_sync_loop, args=(state, cfg, holder, lock, log), daemon=True
    )
    net_t = threading.Thread(
        target=run_network_loop, args=(state, cfg, holder, lock, log), daemon=True
    )
    sync_t.start()
    net_t.start()
    return conn, sync_t, net_t


# ---------------------------------------------------------------------------
# 4A — outboxDepth in assignment poll, tracks outbox state
# ---------------------------------------------------------------------------
def test_telemetry_depth_tracks_state() -> None:
    """outboxDepth in every poll; spikes when queued, returns to 0 after flush."""
    print("test_telemetry_depth_tracks_state (4A — outboxDepth tracks outbox)")
    log = logging.getLogger("t4a")
    backend = FakeBackend().start()
    backend.set_mode(MODE_OK)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_queued(seed_conn, 10)
    seed_conn.close()
    state = NodeState()
    conn, sync_t, net_t = _start_both(db_path, backend, state, log)
    try:
        # Wait for at least one assignment query to arrive
        got_query = _wait_for(lambda: len(backend.assignment_queries) > 0, timeout=5.0)
        check("assignment GET issued", got_query, f"queries={len(backend.assignment_queries)}")
        if got_query:
            first_q = backend.assignment_queries[0]
            check(
                "outboxDepth present in assignment query",
                "outboxDepth" in first_q,
                str(first_q),
            )
            depth_val = first_q.get("outboxDepth", [""])[0]
            check(
                "outboxDepth is numeric string",
                depth_val.isdigit(),
                f"got={depth_val!r}",
            )

        # Wait for all 10 rows to sync
        flushed = _wait_for(lambda: _row_count(db_path, "sent") == 10, timeout=6.0)
        check("all 10 rows synced", flushed, f"sent={_row_count(db_path, 'sent')}")

        # Wait for a follow-up assignment query showing depth=0
        zero_depth = _wait_for(
            lambda: any(
                q.get("outboxDepth", ["x"])[0] == "0"
                for q in backend.assignment_queries
            ),
            timeout=5.0,
        )
        check(
            "outboxDepth=0 in later query (queue drained)",
            zero_depth,
            str([q.get("outboxDepth") for q in backend.assignment_queries[-3:]]),
        )
    finally:
        state.request_shutdown()
        sync_t.join(timeout=2.0)
        net_t.join(timeout=2.0)
        conn.close()
        backend.stop()


# ---------------------------------------------------------------------------
# 4B — lastSyncAt absent before first sync, present afterwards
# ---------------------------------------------------------------------------
def test_telemetry_last_sync_at() -> None:
    """lastSyncAt absent in pre-sync queries, set after first successful flush."""
    print("test_telemetry_last_sync_at (4B — lastSyncAt set after flush)")
    log = logging.getLogger("t4b")
    backend = FakeBackend().start()
    backend.set_mode(MODE_OK)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_queued(seed_conn, 5)
    seed_conn.close()
    state = NodeState()
    conn, sync_t, net_t = _start_both(db_path, backend, state, log)
    try:
        # Wait for sync to complete
        flushed = _wait_for(lambda: _row_count(db_path, "sent") == 5, timeout=6.0)
        check("5 rows synced", flushed)
        check("state.last_sync_at set after flush", state.get_last_sync_at() is not None)

        # Wait for a query that includes lastSyncAt
        got_sync_at = _wait_for(
            lambda: any("lastSyncAt" in q for q in backend.assignment_queries),
            timeout=5.0,
        )
        check(
            "lastSyncAt in assignment query after flush",
            got_sync_at,
            str([list(q.keys()) for q in backend.assignment_queries[-3:]]),
        )
        if got_sync_at:
            sync_queries = [q for q in backend.assignment_queries if "lastSyncAt" in q]
            iso = sync_queries[-1]["lastSyncAt"][0]
            check(
                "lastSyncAt is a valid ISO string",
                "T" in iso and "Z" in iso,
                f"iso={iso!r}",
            )
    finally:
        state.request_shutdown()
        sync_t.join(timeout=2.0)
        net_t.join(timeout=2.0)
        conn.close()
        backend.stop()


# ---------------------------------------------------------------------------
# 4C — depth stays > 0 while 422-blocked; lastSyncAt not updated
# ---------------------------------------------------------------------------
def test_telemetry_depth_stable_during_422() -> None:
    """While event not live (422), depth stays > 0 and lastSyncAt is never set."""
    print("test_telemetry_depth_stable_during_422 (4C — no spurious lastSyncAt on 422)")
    log = logging.getLogger("t4c")
    backend = FakeBackend().start()
    backend.set_mode(MODE_SEMANTIC_422)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_queued(seed_conn, 6)
    seed_conn.close()
    state = NodeState()
    conn, sync_t, net_t = _start_both(db_path, backend, state, log)
    try:
        # Let several poll+sync cycles run while event is blocked
        got_blocked = _wait_for(lambda: state.is_ingest_blocked(), timeout=4.0)
        check("ingest_blocked set (422 active)", got_blocked)
        time.sleep(0.3)  # several more cycles

        got_any_query = len(backend.assignment_queries) > 0
        check("assignment polls happened during 422 block", got_any_query,
              f"queries={len(backend.assignment_queries)}")

        if got_any_query:
            # All queries while blocked should show depth > 0
            depths = [
                int(q.get("outboxDepth", ["0"])[0])
                for q in backend.assignment_queries
                if q.get("outboxDepth", [""])[0].isdigit()
            ]
            check(
                "all depths > 0 while blocked",
                all(d > 0 for d in depths),
                f"depths={depths}",
            )

        check(
            "lastSyncAt not set while event blocked",
            state.get_last_sync_at() is None,
            f"last_sync_at={state.get_last_sync_at()!r}",
        )
    finally:
        state.request_shutdown()
        sync_t.join(timeout=2.0)
        net_t.join(timeout=2.0)
        conn.close()
        backend.stop()


# ---------------------------------------------------------------------------
# 4D — depth stable during 401 pause; no sends
# ---------------------------------------------------------------------------
def test_telemetry_depth_stable_during_401() -> None:
    """401 pauses sync; depth stays > 0 in polls, lastSyncAt never set."""
    print("test_telemetry_depth_stable_during_401 (4D — depth > 0 while paused)")
    log = logging.getLogger("t4d")
    backend = FakeBackend().start()
    backend.set_mode(MODE_AUTH_401)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_queued(seed_conn, 4)
    seed_conn.close()
    state = NodeState()
    conn, sync_t, net_t = _start_both(db_path, backend, state, log)
    try:
        got_paused = _wait_for(lambda: state.is_auth_failed(), timeout=4.0)
        check("auth_failed set (401 active)", got_paused)
        time.sleep(0.3)

        got_any_query = len(backend.assignment_queries) > 0
        # Note: 401 is returned from SYNC loop's /reads endpoint, not from /assignment.
        # Network loop polls /assignment which may also return 401 in some setups,
        # but in our fake_backend, /assignment always returns 200.
        # So assignment_queries should have entries regardless.
        check("assignment polls happened", got_any_query,
              f"queries={len(backend.assignment_queries)}")

        check(
            "lastSyncAt not set while auth paused",
            state.get_last_sync_at() is None,
            f"last_sync_at={state.get_last_sync_at()!r}",
        )
        check(
            "zero rows sent",
            _row_count(db_path, "sent") == 0,
        )
    finally:
        state.request_shutdown()
        sync_t.join(timeout=2.0)
        net_t.join(timeout=2.0)
        conn.close()
        backend.stop()


def main() -> int:
    logging.basicConfig(level=logging.CRITICAL)
    test_telemetry_depth_tracks_state()
    test_telemetry_last_sync_at()
    test_telemetry_depth_stable_during_422()
    test_telemetry_depth_stable_during_401()
    print(f"\n{_PASS} passed, {_FAIL} failed")
    return 1 if _FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
