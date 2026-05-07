#!/usr/bin/env python3
"""
RoyalMNL timing node entrypoint: parallel reader + network loops, SQLite WAL, boot preflight.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time

from clock_check import is_clock_trusted
from config import load_config, load_env_overlays, validate_required
from db import connect, init_schema
from network_loop import run_network_loop
from node_state import NodeState
from reader_loop import run_reader_loop

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore


def _acquire_lock(lock_path: str, log: logging.Logger):
    """Best-effort single-instance lock (Linux/Pi)."""
    if fcntl is None:
        log.warning("fcntl unavailable — single-instance lock skipped")
        return None
    try:
        lock_f = open(lock_path, "w")
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_f.write(str(time.time()))
        lock_f.flush()
        return lock_f
    except BlockingIOError:
        log.error("Another timing-node instance holds %s", lock_path)
        sys.exit(1)
    except OSError as e:
        log.warning("Could not acquire lock %s: %s — continuing", lock_path, e)
        return None


def main() -> None:
    load_env_overlays()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    root_log = logging.getLogger("timing-node")

    while True:
        cfg = load_config()
        err = validate_required(cfg)
        if err is None:
            break
        root_log.error("Config invalid: %s — retry in 10s", err)
        time.sleep(10)
        load_env_overlays()

    log = logging.getLogger("timing-node")
    log.setLevel(getattr(logging, cfg.log_level.upper(), logging.INFO))

    lock_f = _acquire_lock(cfg.lock_file_path, log)

    conn = connect(cfg.timing_db_path)
    init_schema(conn)

    state = NodeState()
    state.set_clock_trusted(is_clock_trusted(log))

    conn_holder: dict = {"conn": conn}
    db_lock = threading.Lock()

    def handle_signal(_sig, _frame) -> None:
        log.warning("Shutdown requested")
        state.request_shutdown()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    t_reader = threading.Thread(
        target=run_reader_loop,
        args=(state, cfg, conn_holder, db_lock, log),
        name="reader",
        daemon=True,
    )
    t_network = threading.Thread(
        target=run_network_loop,
        args=(state, cfg, conn_holder, db_lock, log),
        name="network",
        daemon=True,
    )

    t_reader.start()
    t_network.start()

    try:
        while not state.is_shutdown_requested():
            time.sleep(0.5)
    finally:
        state.request_shutdown()
        t_reader.join(timeout=15.0)
        t_network.join(timeout=15.0)
        try:
            conn.close()
        except Exception:
            log.exception("SQLite close")
        if lock_f:
            try:
                lock_f.close()
            except OSError:
                pass


if __name__ == "__main__":
    main()
