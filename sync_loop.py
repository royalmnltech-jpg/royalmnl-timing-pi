"""HTTP sync worker: flush SQLite outbox rows to the timing backend API."""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from config import NodeConfig
from db import get_queued_for_sync, increment_retry, mark_read_dead, mark_read_sent
from node_state import NodeState


def _post_json(
    url: str,
    api_key: str,
    body: dict[str, Any],
    timeout: float = 10.0,
) -> tuple[int, Any]:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-api-key": api_key,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_body = resp.read().decode("utf-8", errors="replace")
            return resp.status, json.loads(resp_body) if resp_body else {}
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        return e.code, resp_body


def _build_payload(row: dict[str, Any], timing_node_id: str) -> dict[str, Any]:
    raw = json.loads(row["raw_json"]) if row.get("raw_json") else None
    payload: dict[str, Any] = {
        "timingEventId": row["timing_event_id"],
        "checkpointId": row["checkpoint_id"],
        "epc": row["epc"],
        "readAt": row["read_at"],
        "readerId": timing_node_id,
    }
    if raw is not None:
        payload["raw"] = raw
    return payload


def run_sync_loop(
    state: NodeState,
    cfg: NodeConfig,
    conn_holder: dict,
    db_lock: threading.Lock,
    log: logging.Logger,
) -> None:
    """
    Continuously flush queued outbox rows to the backend.

    - Reads are sent in batches ordered by read_at (oldest first).
    - 200/201: mark sent.
    - 409 (duplicate): mark sent — backend already has it.
    - Other 4xx: dead-letter immediately (bad payload, not retryable).
    - 5xx / network error: increment retry_count; dead-letter after sync_max_retries.
    - Sleeps sync_interval_sec between cycles.
    """
    base = cfg.timing_api_base_url.rstrip("/")
    read_url = f"{base}/api/v1/timing/read"

    while not state.is_shutdown_requested():
        with db_lock:
            conn = conn_holder.get("conn")
            if conn is None:
                time.sleep(cfg.sync_interval_sec)
                continue
            rows = get_queued_for_sync(conn, limit=cfg.sync_batch_size)

        for row in rows:
            if state.is_shutdown_requested():
                break

            row_id: str = row["id"]
            payload = _build_payload(row, cfg.timing_node_id)

            try:
                status, _ = _post_json(read_url, cfg.timing_api_key, payload)
            except Exception as exc:
                log.warning("Sync POST failed (network): %s — row %s", exc, row_id)
                with db_lock:
                    conn = conn_holder.get("conn")
                    if conn:
                        new_count = increment_retry(conn, row_id)
                        if new_count >= cfg.sync_max_retries:
                            mark_read_dead(conn, row_id, f"network error after {new_count} retries: {exc}")
                            log.warning("Dead-lettered row %s after %s retries", row_id, new_count)
                continue

            with db_lock:
                conn = conn_holder.get("conn")
                if conn is None:
                    break

                if status in (200, 201):
                    mark_read_sent(conn, row_id)
                    log.debug("Synced row %s (HTTP %s)", row_id, status)
                elif status == 409:
                    mark_read_sent(conn, row_id)
                    log.debug("Row %s already on backend (409), marked sent", row_id)
                elif 400 <= status < 500:
                    reason = f"HTTP {status} (non-retryable)"
                    mark_read_dead(conn, row_id, reason)
                    log.warning("Dead-lettered row %s: %s", row_id, reason)
                else:
                    new_count = increment_retry(conn, row_id)
                    if new_count >= cfg.sync_max_retries:
                        mark_read_dead(conn, row_id, f"HTTP {status} after {new_count} retries")
                        log.warning("Dead-lettered row %s after %s retries (HTTP %s)", row_id, new_count, status)
                    else:
                        log.warning("Sync HTTP %s for row %s, retry %s/%s", status, row_id, new_count, cfg.sync_max_retries)

        time.sleep(cfg.sync_interval_sec)
