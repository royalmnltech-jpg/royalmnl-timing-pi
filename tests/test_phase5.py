"""Phase 5 race-day tests: reader-thread hardening.

Verifies:
  5A — Stall watchdog: reader stays TCP-connected but emits no tags → stalled flag set,
       reader transitions out of CAPTURING.
  5B — Dedup eviction: EPC re-read after dedup window expires → accepted (eviction works,
       map doesn't grow unbounded).
  5C — Re-key on assignment backfill: EPC read while pending, assignment delivered, EPC
       re-read within dedup window → only 1 outbox row (no double-capture).
  5D — Stall clears on recovery: after stall, fake reader switches to trickle → reader
       reconnects, stalled flag clears, reads resume.

Stdlib only — no pytest.
Run: python tests/test_phase5.py

NOTE: Tests monkey-patch `reader_loop.INV_ROUND_TIMEOUT_SEC` to shorten stall-round wait
      times. Original value is restored in each test's finally block.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import reader_loop as _rl  # noqa: E402  — module-level for monkey-patching
from config import NodeConfig  # noqa: E402
from db import connect, init_schema  # noqa: E402
from node_state import NodeState, ReaderState  # noqa: E402
from reader_loop import run_reader_loop  # noqa: E402
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


def _cfg(
    reader_port: int,
    *,
    dedupe_window_sec: float = 20.0,
    reader_stall_sec: float = 120.0,
) -> NodeConfig:
    return NodeConfig(
        timing_node_id="node-p5",
        timing_api_base_url="http://127.0.0.1:1",  # unused by reader loop
        timing_api_key="test-key",
        reader_ip="127.0.0.1",
        reader_port=reader_port,
        dedupe_window_sec=dedupe_window_sec,
        assignment_poll_sec=0.1,
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
        reader_stall_sec=reader_stall_sec,
    )


def _fresh_db() -> tuple[str, sqlite3.Connection]:
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "outbox.db")
    conn = connect(path)
    init_schema(conn)
    return path, conn


def _outbox_count(db_path: str) -> int:
    c = sqlite3.connect(db_path)
    try:
        return c.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
    finally:
        c.close()


def _wait_for(pred, timeout: float = 8.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _start_reader(cfg: NodeConfig, state: NodeState, conn: sqlite3.Connection) -> threading.Thread:
    holder = {"conn": conn}
    lock = threading.Lock()
    t = threading.Thread(
        target=run_reader_loop, args=(state, cfg, holder, lock, logging.getLogger("t5")),
        daemon=True,
    )
    t.start()
    return t


# ---------------------------------------------------------------------------
# 5A — Stall watchdog triggers when no tags flow
# ---------------------------------------------------------------------------
def test_stall_watchdog_triggers() -> None:
    """Stall mode: reader stays connected but silent → stalled flag + leaves CAPTURING."""
    print("test_stall_watchdog_triggers (5A — stall watchdog)")
    orig_timeout = _rl.INV_ROUND_TIMEOUT_SEC
    _rl.INV_ROUND_TIMEOUT_SEC = 0.2  # each stalled round times out in 0.2s
    reader = FakeReader(mode="stall").start()
    db_path, conn = _fresh_db()
    state = NodeState()
    state.set_assignment(
        timing_event_id="te1", checkpoint_id="finish", version=1,
        checkpoint_valid=True, assignment_pending=False,
    )
    # stall triggers after >2 rounds * 0.2s = >0.4s → use 0.5s
    cfg = _cfg(reader.port, reader_stall_sec=0.5)
    t = _start_reader(cfg, state, conn)
    try:
        stalled = _wait_for(lambda: state.is_reader_stalled(), timeout=6.0)
        check("reader_stalled flag set", stalled)
        left_capturing = _wait_for(
            lambda: state.get_reader_state() != ReaderState.CAPTURING,
            timeout=3.0,
        )
        check("reader left CAPTURING after stall", left_capturing,
              f"state={state.get_reader_state()}")
        check("zero outbox rows (stall mode never emits tags)", _outbox_count(db_path) == 0)
    finally:
        state.request_shutdown()
        t.join(timeout=3.0)
        conn.close()
        reader.stop()
        _rl.INV_ROUND_TIMEOUT_SEC = orig_timeout


# ---------------------------------------------------------------------------
# 5B — Dedup eviction allows re-read after window expiry
# ---------------------------------------------------------------------------
def test_dedup_eviction_allows_reread() -> None:
    """Same EPC re-read after dedup window expires → 2nd outbox row (eviction worked)."""
    print("test_dedup_eviction_allows_reread (5B — dedup eviction)")
    orig_timeout = _rl.INV_ROUND_TIMEOUT_SEC
    orig_pause = _rl.POLL_PAUSE_SEC
    _rl.INV_ROUND_TIMEOUT_SEC = 0.1
    _rl.POLL_PAUSE_SEC = 0.01
    epc = "E2000017221101441890C0A1"
    reader = FakeReader(epcs=[epc], mode="trickle").start()
    db_path, conn = _fresh_db()
    state = NodeState()
    state.set_assignment(
        timing_event_id="te1", checkpoint_id="finish", version=1,
        checkpoint_valid=True, assignment_pending=False,
    )
    # short dedup window: 0.15s; each round ~0.11s, so window expires after ~1-2 rounds
    cfg = _cfg(reader.port, dedupe_window_sec=0.15, reader_stall_sec=999.0)
    t = _start_reader(cfg, state, conn)
    try:
        # At least 2 rows means the same EPC was accepted a second time after window expired
        got_reread = _wait_for(lambda: _outbox_count(db_path) >= 2, timeout=6.0)
        check("EPC re-read accepted after dedup window expired", got_reread,
              f"rows={_outbox_count(db_path)}")
    finally:
        state.request_shutdown()
        t.join(timeout=3.0)
        conn.close()
        reader.stop()
        _rl.INV_ROUND_TIMEOUT_SEC = orig_timeout
        _rl.POLL_PAUSE_SEC = orig_pause


# ---------------------------------------------------------------------------
# 5C — Dedup re-key prevents double-capture across pending→assigned boundary
# ---------------------------------------------------------------------------
def test_pending_assignment_rekey_no_double_capture() -> None:
    """EPC seen while pending, assignment arrives, same EPC re-read within window → 1 row only."""
    print("test_pending_assignment_rekey_no_double_capture (5C — dedup re-key)")
    orig_pause = _rl.POLL_PAUSE_SEC
    _rl.POLL_PAUSE_SEC = 0.01
    epc = "E2000017221101441890C0A1"
    reader = FakeReader(epcs=[epc], mode="trickle").start()
    db_path, conn = _fresh_db()
    state = NodeState()
    # Start with NO assignment — EPC will be captured as assignment_pending=1
    state.set_assignment(
        timing_event_id=None, checkpoint_id=None, version=None,
        checkpoint_valid=False, assignment_pending=True,
    )
    # Large dedup window (10s) — assignment arrives well within it
    cfg = _cfg(reader.port, dedupe_window_sec=10.0, reader_stall_sec=999.0)
    t = _start_reader(cfg, state, conn)
    try:
        # Wait for first pending read
        got_pending = _wait_for(lambda: _outbox_count(db_path) >= 1, timeout=5.0)
        check("first read captured while assignment pending", got_pending,
              f"rows={_outbox_count(db_path)}")

        # Deliver assignment → triggers re-key in reader_loop at next round start
        state.set_assignment(
            timing_event_id="te1", checkpoint_id="finish", version=1,
            checkpoint_valid=True, assignment_pending=False,
        )

        # Let several more rounds run — same EPC re-read, but window still active
        time.sleep(0.25)

        row_count = _outbox_count(db_path)
        check(
            "same EPC not double-captured after re-key (still within window)",
            row_count == 1,
            f"rows={row_count}",
        )
    finally:
        state.request_shutdown()
        t.join(timeout=3.0)
        conn.close()
        reader.stop()
        _rl.POLL_PAUSE_SEC = orig_pause


# ---------------------------------------------------------------------------
# 5D — Stall flag clears after reconnect with tags flowing again
# ---------------------------------------------------------------------------
def test_stall_clears_after_recovery() -> None:
    """After stall triggers, fake reader switches to trickle → stalled flag clears, reads resume."""
    print("test_stall_clears_after_recovery (5D — stall recovery)")
    orig_timeout = _rl.INV_ROUND_TIMEOUT_SEC
    _rl.INV_ROUND_TIMEOUT_SEC = 0.2
    reader = FakeReader(mode="stall").start()
    db_path, conn = _fresh_db()
    state = NodeState()
    state.set_assignment(
        timing_event_id="te1", checkpoint_id="finish", version=1,
        checkpoint_valid=True, assignment_pending=False,
    )
    cfg = _cfg(reader.port, reader_stall_sec=0.5)
    t = _start_reader(cfg, state, conn)
    try:
        # Phase 1: stall fires
        stalled = _wait_for(lambda: state.is_reader_stalled(), timeout=6.0)
        check("stalled flag set (stall mode active)", stalled)

        # Phase 2: flip to trickle before reconnect completes (2s sleep in reader_loop)
        reader.set_mode("trickle")

        # Phase 3: after reconnect, stalled flag should clear
        cleared = _wait_for(lambda: not state.is_reader_stalled(), timeout=10.0)
        check("stalled flag cleared after reconnect with tags", cleared)

        # Phase 4: reads should be flowing again
        got_row = _wait_for(lambda: _outbox_count(db_path) >= 1, timeout=5.0)
        check("at least one read captured after stall recovery", got_row,
              f"rows={_outbox_count(db_path)}")
        check("reader back in CAPTURING", state.get_reader_state() == ReaderState.CAPTURING,
              f"state={state.get_reader_state()}")
    finally:
        state.request_shutdown()
        t.join(timeout=3.0)
        conn.close()
        reader.stop()
        _rl.INV_ROUND_TIMEOUT_SEC = orig_timeout


def main() -> int:
    logging.basicConfig(level=logging.CRITICAL)
    test_stall_watchdog_triggers()
    test_dedup_eviction_allows_reread()
    test_pending_assignment_rekey_no_double_capture()
    test_stall_clears_after_recovery()
    print(f"\n{_PASS} passed, {_FAIL} failed")
    return 1 if _FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
