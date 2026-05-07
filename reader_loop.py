"""Reader worker: TCP connect, configure UHF reader, inventory loop -> SQLite."""

from __future__ import annotations

import logging
import socket
import threading
import time

from config import NodeConfig
from db import insert_tag_read, utc_now_iso_ms
from node_state import NodeState, ReaderState
from reader_protocol import (
    A0Framer,
    INV_ROUND_TIMEOUT_SEC,
    POLL_PAUSE_SEC,
    drain_inventory_round,
    run_configuration_and_health,
    select_inventory_mode,
)


def run_reader_loop(
    state: NodeState,
    cfg: NodeConfig,
    conn_holder: dict,
    db_lock: threading.Lock,
    log: logging.Logger,
) -> None:
    dedupe_last: dict[tuple[str, str, str], float] = {}

    while not state.is_shutdown_requested():
        state.set_reader_state(ReaderState.CONNECTING)
        sock: socket.socket | None = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.5)
            log.info("Connecting reader %s:%s", cfg.reader_ip, cfg.reader_port)
            sock.connect((cfg.reader_ip, cfg.reader_port))
            state.set_reader_state(ReaderState.CONFIGURING)
            run_configuration_and_health(sock, log)
            cmd, inv_bytes, label = select_inventory_mode(sock, log)
            log.info("Reader capture mode: %s", label)
            state.set_reader_state(ReaderState.CAPTURING)
            framer = A0Framer()

            while not state.is_shutdown_requested():
                sock.sendall(inv_bytes)
                deadline = time.monotonic() + INV_ROUND_TIMEOUT_SEC

                def on_tag(payload: dict) -> None:
                    raw_epc = payload.get("epc", "")
                    epc = raw_epc.strip().upper()
                    if not epc:
                        return

                    te_id, cp_id, _, checkpoint_valid, _ = state.get_assignment_snapshot()
                    dedupe_key_te = te_id or "__pending__"
                    dedupe_key_cp = cp_id or "__pending__"
                    key = (dedupe_key_te, dedupe_key_cp, epc)
                    now_m = time.monotonic()
                    last = dedupe_last.get(key)
                    if last is not None and (now_m - last) < cfg.dedupe_window_sec:
                        return
                    dedupe_last[key] = now_m

                    read_at = utc_now_iso_ms()
                    clock_ok = state.get_clock_trusted()

                    raw_payload = {
                        **payload,
                        "clock_untrusted": not clock_ok,
                        "timing_node_id": cfg.timing_node_id,
                    }

                    assignment_pending = not (
                        checkpoint_valid and te_id and cp_id
                    )
                    timing_event_id = te_id if te_id else None
                    checkpoint_id = cp_id if cp_id else None

                    try:
                        with db_lock:
                            conn = conn_holder.get("conn")
                            if not conn:
                                return
                            insert_tag_read(
                                conn,
                                epc=epc,
                                read_at=read_at,
                                captured_at_mono=now_m,
                                timing_event_id=timing_event_id,
                                checkpoint_id=checkpoint_id,
                                assignment_pending=assignment_pending,
                                clock_untrusted=not clock_ok,
                                raw=raw_payload,
                                log=log,
                            )
                    except Exception:
                        log.exception("Failed to persist tag read")

                _, finished = drain_inventory_round(sock, framer, deadline, cmd, on_tag)
                if not finished:
                    log.debug("Inventory round incomplete (timeout)")
                time.sleep(POLL_PAUSE_SEC)

        except OSError as e:
            log.warning("Reader connection error: %s", e)
            state.set_reader_state(ReaderState.RECONNECTING)
            time.sleep(2.0)
        finally:
            if sock:
                try:
                    sock.close()
                except OSError:
                    pass
            state.set_reader_state(ReaderState.DISCONNECTED)
