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
    probe_reader_alive,
    run_configuration_and_health,
    select_inventory_mode,
)


def _rekey_pending_dedup(
    dedupe_last: dict,
    te_id: str,
    cp_id: str,
    log: logging.Logger,
) -> None:
    """Move __pending__ dedup entries to real assignment keys on backfill.

    Carries the dedup window over the pending→assigned boundary so an EPC read while
    pending and then re-read within the window after assignment arrives is not double-captured.
    """
    pending = [k for k in dedupe_last if k[0] == "__pending__" or k[1] == "__pending__"]
    for k in pending:
        new_key = (te_id, cp_id, k[2])
        old_ts = dedupe_last.pop(k)
        existing_ts = dedupe_last.get(new_key)
        if existing_ts is None or old_ts > existing_ts:
            dedupe_last[new_key] = old_ts
    if pending:
        log.debug("Dedup re-key: %d pending → (%s, %s)", len(pending), te_id, cp_id)


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
            state.set_reader_stalled(False)
            framer = A0Framer()
            # last_tag_mono[0]: monotonic time of most recent tag seen (any tag, including deduped).
            # Initialised to now so a fresh connection doesn't immediately trigger the stall watchdog.
            last_tag_mono: list[float] = [time.monotonic()]
            prev_cp_valid = False

            while not state.is_shutdown_requested():
                round_start = time.monotonic()

                # Assignment transition: pending → assigned.
                # Re-key dedup entries so the dedup window carries over the boundary —
                # prevents double-capture of an EPC first seen while pending.
                # Reset stall clock so the watchdog starts fresh from assignment time,
                # not from an arbitrary pre-assignment moment.
                te_id, cp_id, _, cp_valid, _ = state.get_assignment_snapshot()
                if cp_valid and not prev_cp_valid and te_id and cp_id:
                    _rekey_pending_dedup(dedupe_last, te_id, cp_id, log)
                    last_tag_mono[0] = round_start
                prev_cp_valid = cp_valid

                # Evict dedup entries older than the window (bounds map size over long races).
                if cfg.dedupe_window_sec > 0:
                    stale = [
                        k for k, ts in dedupe_last.items()
                        if round_start - ts > cfg.dedupe_window_sec
                    ]
                    for k in stale:
                        del dedupe_last[k]

                # Liveness watchdog: TCP-connected but no tags for cfg.reader_stall_sec.
                # Only active when a valid assignment exists — pre-race idle (no assignment)
                # produces no tags by design and must not trigger spurious reconnects.
                # Probe before reconnecting: a silent checkpoint (no runners) looks identical
                # to a hung reader. Send Get Temperature and wait up to 2s for a reply.
                # Reconnect only if the reader fails to respond.
                if cp_valid and round_start - last_tag_mono[0] > cfg.reader_stall_sec:
                    log.warning(
                        "No tags for %.0fs — probing reader before reconnect",
                        cfg.reader_stall_sec,
                    )
                    if probe_reader_alive(sock, log):
                        log.info("Reader probe OK — alive, resetting stall clock")
                        last_tag_mono[0] = time.monotonic()
                    else:
                        log.warning("Reader probe failed — reader hung, reconnecting")
                        state.set_reader_stalled(True)
                        break

                sock.sendall(inv_bytes)
                deadline = time.monotonic() + INV_ROUND_TIMEOUT_SEC

                def on_tag(payload: dict) -> None:
                    raw_epc = payload.get("epc", "")
                    epc = raw_epc.strip().upper()
                    if not epc:
                        return

                    now_m = time.monotonic()
                    # Update stall clock on every seen tag (even deduped ones —
                    # the reader is alive and emitting; dedup is a logical filter).
                    last_tag_mono[0] = now_m

                    te_id, cp_id, _, checkpoint_valid, _ = state.get_assignment_snapshot()
                    dedupe_key_te = te_id or "__pending__"
                    dedupe_key_cp = cp_id or "__pending__"
                    key = (dedupe_key_te, dedupe_key_cp, epc)
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
