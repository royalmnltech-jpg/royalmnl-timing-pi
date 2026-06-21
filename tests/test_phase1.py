"""Phase 1 race-day tests: sync error handling (1A) + reader power (1B).

Stdlib only (no pytest) — matches the Pi's no-third-party-deps constraint.
Run:  python tests/test_phase1.py
"""

from __future__ import annotations

import logging
import os
import socket
import sqlite3
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import NodeConfig  # noqa: E402
from db import connect, init_schema, insert_tag_read, utc_now_iso_ms  # noqa: E402  (init_schema used in _seed_queued)
from node_state import NodeState  # noqa: E402
from reader_protocol import run_configuration_and_health  # noqa: E402
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
    MODE_OK,
    MODE_RATELIMIT_429,
    MODE_SEMANTIC_422,
)
from tests.fake_reader import FakeReader  # noqa: E402

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
        timing_node_id="node-test",
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
        sync_sent_ttl_sec=3600.0,
        sync_dead_ttl_sec=86400.0,
        sync_dead_cap=10000,
        sync_purge_interval_sec=9999.0,  # never fires during Phase 1 tests
        sync_reads_ttl_sec=604800.0,
        reader_stall_sec=120.0,
    )


def _seed_queued(conn: sqlite3.Connection, n: int, log: logging.Logger) -> None:
    init_schema(conn)
    for i in range(n):
        insert_tag_read(
            conn,
            epc=f"EPC{i:024X}"[:24],
            read_at=utc_now_iso_ms(),
            captured_at_mono=time.monotonic(),
            timing_event_id="te_test",
            checkpoint_id="finish",
            assignment_pending=False,
            clock_untrusted=False,
            raw={"epc": f"EPC{i}"},
            log=log,
        )


def _status_counts(db_path: str) -> dict[str, int]:
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    try:
        rows = c.execute("SELECT status, COUNT(*) AS n FROM outbox GROUP BY status").fetchall()
        return {r["status"]: r["n"] for r in rows}
    finally:
        c.close()


def _wait_for(predicate, timeout: float = 5.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# 1A — pure classification unit tests
# ---------------------------------------------------------------------------
def test_classify() -> None:
    print("test_classify (1A response classification)")
    check("200 -> SENT", classify_response(200, {"ok": True})[0] == SENT)
    check("201 -> SENT", classify_response(201, {})[0] == SENT)
    check("409 -> SENT", classify_response(409, "")[0] == SENT)
    auth = classify_response(401, '{"ok":false,"error":{"code":"INVALID_API_KEY"}}')
    check("401 -> PAUSE", auth[0] == PAUSE, str(auth))
    sem = classify_response(422, '{"ok":false,"error":{"code":"SEMANTICALLY_INVALID_TIMING_READ"}}')
    check("422 -> KEEP", sem[0] == KEEP, str(sem))
    check("429 -> KEEP (was data loss before)", classify_response(429, "")[0] == KEEP)
    check("400 -> DEAD", classify_response(400, '{"ok":false,"error":{"code":"INVALID_PAYLOAD"}}')[0] == DEAD)
    check("404 -> DEAD", classify_response(404, "")[0] == DEAD)
    check("500 -> RETRY", classify_response(500, "")[0] == RETRY)
    check("503 -> RETRY", classify_response(503, "")[0] == RETRY)


# ---------------------------------------------------------------------------
# 1A — integration: real sync_loop over HTTP against the fake backend
# ---------------------------------------------------------------------------
def _run_loop(db_path, backend, state, log):
    conn = connect(db_path)
    holder = {"conn": conn}
    lock = threading.Lock()
    t = threading.Thread(
        target=run_sync_loop, args=(state, _cfg(backend.base_url), holder, lock, log), daemon=True
    )
    t.start()
    return conn, t


def test_422_then_live() -> None:
    """Operator forgot to set event live at gun: reads must survive, then flush."""
    print("test_422_then_live (1A — event not ingestible)")
    log = logging.getLogger("t422")
    backend = FakeBackend().start()
    backend.set_mode(MODE_SEMANTIC_422)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_queued(seed_conn, 10, log)
    seed_conn.close()
    state = NodeState()
    conn, t = _run_loop(db_path, backend, state, log)
    try:
        # While 422, nothing should be dead-lettered and nothing sent.
        stayed = _wait_for(lambda: state.is_ingest_blocked(), timeout=3.0)
        check("ingest_blocked flag set on 422", stayed)
        time.sleep(0.5)
        counts = _status_counts(db_path)
        check("no reads dead-lettered while not live", counts.get("dead", 0) == 0, str(counts))
        check("no reads sent while not live", counts.get("sent", 0) == 0, str(counts))
        check("all 10 reads still queued", counts.get("queued", 0) == 10, str(counts))
        # Operator flips event live.
        backend.set_mode(MODE_OK)
        flushed = _wait_for(lambda: _status_counts(db_path).get("sent", 0) == 10, timeout=5.0)
        check("all 10 reads flush once live", flushed, str(_status_counts(db_path)))
        check("ingest_blocked cleared after flush", not state.is_ingest_blocked())
        check("backend received all 10", len(backend.received_reads) == 10, str(len(backend.received_reads)))
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


def test_401_pauses() -> None:
    print("test_401_pauses (1A — bad API key)")
    log = logging.getLogger("t401")
    backend = FakeBackend().start()
    backend.set_mode(MODE_AUTH_401)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_queued(seed_conn, 5, log)
    seed_conn.close()
    state = NodeState()
    conn, t = _run_loop(db_path, backend, state, log)
    try:
        paused = _wait_for(lambda: state.is_auth_failed(), timeout=3.0)
        check("auth_failed set on 401", paused)
        time.sleep(0.5)
        counts = _status_counts(db_path)
        check("zero reads dead-lettered on 401", counts.get("dead", 0) == 0, str(counts))
        check("all 5 reads still queued on 401", counts.get("queued", 0) == 5, str(counts))
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


def test_400_deadletters() -> None:
    print("test_400_deadletters (1A — genuine bad payload control case)")
    log = logging.getLogger("t400")
    backend = FakeBackend().start()
    backend.set_mode(MODE_BADPAYLOAD_400)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_queued(seed_conn, 3, log)
    seed_conn.close()
    state = NodeState()
    conn, t = _run_loop(db_path, backend, state, log)
    try:
        done = _wait_for(lambda: _status_counts(db_path).get("dead", 0) == 3, timeout=4.0)
        check("bad-payload reads dead-lettered", done, str(_status_counts(db_path)))
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


def test_429_not_lost() -> None:
    print("test_429_not_lost (1A — rate limit must not dead-letter)")
    log = logging.getLogger("t429")
    backend = FakeBackend().start()
    backend.set_mode(MODE_RATELIMIT_429)
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "outbox.db")
    seed_conn = connect(db_path)
    _seed_queued(seed_conn, 4, log)
    seed_conn.close()
    state = NodeState()
    conn, t = _run_loop(db_path, backend, state, log)
    try:
        time.sleep(1.0)
        counts = _status_counts(db_path)
        check("no reads dead-lettered on 429", counts.get("dead", 0) == 0, str(counts))
        check("reads remain queued on 429", counts.get("queued", 0) == 4, str(counts))
        backend.set_mode(MODE_OK)
        flushed = _wait_for(lambda: _status_counts(db_path).get("sent", 0) == 4, timeout=5.0)
        check("reads flush after rate limit lifts", flushed, str(_status_counts(db_path)))
    finally:
        state.request_shutdown()
        t.join(timeout=2.0)
        conn.close()
        backend.stop()


# ---------------------------------------------------------------------------
# 1B — reader power
# ---------------------------------------------------------------------------
def test_reader_power() -> None:
    print("test_reader_power (1B — 26 dBm)")
    records: list[str] = []

    class _Cap(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    log = logging.getLogger("t1b")
    log.setLevel(logging.INFO)
    log.addHandler(_Cap())

    reader = FakeReader(power_dbm=0x1A).start()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2.5)
    try:
        sock.connect(("127.0.0.1", reader.port))
        run_configuration_and_health(sock, log)
        time.sleep(0.2)
        check("reader received set-power 0x1A (26 dBm)", 0x1A in reader.set_power_values,
              str(reader.set_power_values))
        check("health echo shows 26 dBm", any("26 dBm" in m for m in records),
              " | ".join(m for m in records if "dBm" in m))
    finally:
        sock.close()
        reader.stop()


def main() -> int:
    logging.basicConfig(level=logging.CRITICAL)  # keep test output clean
    test_classify()
    test_422_then_live()
    test_401_pauses()
    test_400_deadletters()
    test_429_not_lost()
    test_reader_power()
    print(f"\n{_PASS} passed, {_FAIL} failed")
    return 1 if _FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
