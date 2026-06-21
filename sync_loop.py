"""HTTP sync worker: flush SQLite outbox rows to the timing backend API (bulk)."""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Optional

from config import NodeConfig
from db import (
    cap_dead_letter_rows,
    get_queued_for_sync,
    increment_retry,
    mark_read_dead,
    mark_read_sent,
    mark_reads_sent_bulk,
    purge_old_reads,
    purge_sent_rows,
    purge_stale_dead_rows,
    utc_now_iso_ms,
)
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


def _extract_error_code(body: Any) -> Optional[str]:
    """Pull the machine `error.code` out of a backend error envelope.

    Body is a dict on a JSON success response, or a raw string on HTTPError.
    Envelope shape: {"ok": false, "error": {"code", "message", "details"}, ...}.
    """
    parsed: Any = body
    if isinstance(body, str):
        if not body:
            return None
        try:
            parsed = json.loads(body)
        except (ValueError, TypeError):
            return None
    if isinstance(parsed, dict):
        err = parsed.get("error")
        if isinstance(err, dict):
            code = err.get("code")
            return code if isinstance(code, str) else None
    return None


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


# Outcome of classifying one HTTP response. Caller acts on this so the same
# logic is unit-testable without a live HTTP call or SQLite handle.
SENT = "sent"            # backend accepted (or duplicate) — mark sent
KEEP = "keep"            # leave queued, retry next cycle (no retry-count bump)
RETRY = "retry"          # transient server/network failure — bump retry, dead-letter after max
DEAD = "dead"            # non-retryable bad payload — dead-letter now
PAUSE = "pause"          # bad API key — pause the whole sync loop, leave queued


def classify_response(status: int, body: Any) -> tuple[str, str]:
    """Map an HTTP (status, body) to an (action, reason) pair.

    - 200/201            -> SENT (covers duplicates, which return 200 isDuplicate=true)
    - 401 INVALID_API_KEY -> PAUSE (never dead-letter; key is wrong, operator must fix)
    - 422 semantic        -> KEEP (commonly EVENT_NOT_INGESTIBLE; flushes once event is live)
    - 429 rate limited    -> KEEP (back off and retry; must not be lost)
    - other 4xx           -> DEAD (genuine bad payload / not found — not retryable)
    - 5xx                 -> RETRY (transient; dead-letter only after max retries)
    """
    if status in (200, 201):
        return SENT, f"HTTP {status}"
    if status == 409:
        # Defensive: duplicates actually return 200 isDuplicate=true, but treat an
        # explicit 409 as idempotent-already-stored.
        return SENT, "HTTP 409 duplicate"
    if status == 401:
        return PAUSE, "HTTP 401 INVALID_API_KEY"
    if status == 422:
        code = _extract_error_code(body) or "SEMANTICALLY_INVALID_TIMING_READ"
        return KEEP, f"HTTP 422 {code}"
    if status == 429:
        return KEEP, "HTTP 429 rate limited"
    if 400 <= status < 500:
        code = _extract_error_code(body) or f"HTTP {status}"
        return DEAD, f"HTTP {status} {code} (non-retryable)"
    return RETRY, f"HTTP {status}"


def _classify_item(item: dict[str, Any]) -> tuple[str, str]:
    """Classify one per-item result from a bulk /reads partial-success response.

    ok:true                           -> SENT
    ok:false SEMANTICALLY_INVALID_*   -> KEEP (event not live; re-queued for next cycle)
    ok:false INVALID_API_KEY          -> PAUSE (shouldn't appear per-item, but handle it)
    ok:false anything else            -> DEAD (bad payload — non-retryable)
    """
    if item.get("ok"):
        return SENT, "bulk item accepted"
    err = item.get("error")
    code = (err.get("code", "") if isinstance(err, dict) else "") or ""
    if code == "SEMANTICALLY_INVALID_TIMING_READ":
        return KEEP, f"bulk item 422 {code}"
    if code == "INVALID_API_KEY":
        return PAUSE, f"bulk item 401 {code}"
    return DEAD, f"bulk item DEAD {code or 'unknown'}"


def _run_maintenance(
    cfg: NodeConfig,
    conn_holder: dict,
    db_lock: threading.Lock,
    log: logging.Logger,
) -> None:
    """Periodic purge: remove old sent/dead rows, cap dead-letter, trim reads table.

    Runs every cfg.sync_purge_interval_sec inside the sync loop. Bounded by db_lock
    so it shares the same connection serialization as the sync path — no 4th thread.
    """
    with db_lock:
        conn = conn_holder.get("conn")
        if conn is None:
            return
        sent_del = purge_sent_rows(conn, cfg.sync_sent_ttl_sec)
        dead_del = purge_stale_dead_rows(conn, cfg.sync_dead_ttl_sec)
        dead_capped = cap_dead_letter_rows(conn, cfg.sync_dead_cap)
        reads_del = purge_old_reads(conn, cfg.sync_reads_ttl_sec)
    total = sent_del + dead_del + dead_capped + reads_del
    if total:
        log.info(
            "Maintenance: purged %s sent, %s stale dead, %s over-cap dead, %s reads",
            sent_del, dead_del, dead_capped, reads_del,
        )


def run_sync_loop(
    state: NodeState,
    cfg: NodeConfig,
    conn_holder: dict,
    db_lock: threading.Lock,
    log: logging.Logger,
) -> None:
    """Flush queued outbox rows to the backend via POST /api/v1/timing/reads (bulk, ≤200).

    Top-level response handling (whole batch):
      - PAUSE (401)     -> set auth_failed; all rows stay queued
      - KEEP (422/429)  -> leave all queued, retry next cycle
      - DEAD (4xx)      -> dead-letter all rows immediately (malformed request)
      - RETRY (5xx/net) -> bump retry_count on each row; dead-letter after sync_max_retries
      - SENT (200/201)  -> parse per-item results (see _classify_item)

    Per-item results (inside a 200 response):
      - ok:true                         -> mark sent
      - ok:false SEMANTICALLY_INVALID   -> keep queued (event not live)
      - ok:false other                  -> dead-letter (bad payload per this read)
    """
    base = cfg.timing_api_base_url.rstrip("/")
    reads_url = f"{base}/api/v1/timing/reads"

    blocked_alerted = False
    last_purge_at = 0.0  # trigger on first iteration

    while not state.is_shutdown_requested():
        now_mono = time.monotonic()
        if now_mono - last_purge_at >= cfg.sync_purge_interval_sec:
            _run_maintenance(cfg, conn_holder, db_lock, log)
            last_purge_at = now_mono

        if state.is_auth_failed():
            time.sleep(cfg.sync_interval_sec)
            continue

        with db_lock:
            conn = conn_holder.get("conn")
            if conn is None:
                time.sleep(cfg.sync_interval_sec)
                continue
            rows = get_queued_for_sync(conn, limit=cfg.sync_batch_size)

        if not rows:
            time.sleep(cfg.sync_interval_sec)
            continue

        payload = {"reads": [_build_payload(row, cfg.timing_node_id) for row in rows]}

        try:
            status, body = _post_json(reads_url, cfg.timing_api_key, payload)
        except Exception as exc:
            log.warning(
                "Bulk sync POST failed (network): %s — bumping retry on %s rows", exc, len(rows)
            )
            with db_lock:
                conn = conn_holder.get("conn")
                if conn:
                    for row in rows:
                        new_count = increment_retry(conn, row["id"])
                        if new_count >= cfg.sync_max_retries:
                            mark_read_dead(
                                conn, row["id"],
                                f"network error after {new_count} retries: {exc}",
                            )
                            log.warning(
                                "Dead-lettered row %s after %s retries", row["id"], new_count
                            )
            time.sleep(cfg.sync_interval_sec)
            continue

        top_action, top_reason = classify_response(status, body)

        if top_action == PAUSE:
            state.set_auth_failed(True)
            log.error(
                "SYNC PAUSED — backend rejected API key (%s). Reads remain queued; "
                "fix TIMING_API_KEY and restart the node.",
                top_reason,
            )
            time.sleep(cfg.sync_interval_sec)
            continue

        if top_action == KEEP:
            if status == 422:
                state.set_ingest_blocked(True)
                if not blocked_alerted:
                    log.error(
                        "INGEST BLOCKED — backend not accepting reads (%s). Reads are safe "
                        "and queued; they will flush once the event is set live.",
                        top_reason,
                    )
                    blocked_alerted = True
            else:
                log.warning("Bulk batch kept queued (%s)", top_reason)
            time.sleep(cfg.sync_interval_sec)
            continue

        if top_action == DEAD:
            # Top-level 4xx means the request itself was rejected — dead-letter all rows.
            with db_lock:
                conn = conn_holder.get("conn")
                if conn:
                    for row in rows:
                        mark_read_dead(conn, row["id"], top_reason)
                        log.warning("Dead-lettered row %s: %s", row["id"], top_reason)
            log.warning(
                "Bulk sync dead-lettered all %s rows (%s)", len(rows), top_reason
            )
            time.sleep(cfg.sync_interval_sec)
            continue

        if top_action == RETRY:
            with db_lock:
                conn = conn_holder.get("conn")
                if conn:
                    for row in rows:
                        new_count = increment_retry(conn, row["id"])
                        if new_count >= cfg.sync_max_retries:
                            mark_read_dead(
                                conn, row["id"],
                                f"{top_reason} after {new_count} retries",
                            )
                            log.warning(
                                "Dead-lettered row %s after %s retries",
                                row["id"], new_count,
                            )
            log.warning("Bulk sync %s for %s rows", top_reason, len(rows))
            time.sleep(cfg.sync_interval_sec)
            continue

        # top_action == SENT: top-level 200/201 — parse per-item results
        items: list[Any] = []
        if isinstance(body, dict):
            data = body.get("data", {})
            if isinstance(data, dict):
                raw_items = data.get("items")
                if isinstance(raw_items, list):
                    items = raw_items

        if not items:
            # Server accepted the whole batch without per-item detail — mark all sent.
            with db_lock:
                conn = conn_holder.get("conn")
                if conn:
                    mark_reads_sent_bulk(conn, [r["id"] for r in rows])
            state.set_last_sync_at(utc_now_iso_ms())
            if blocked_alerted or state.is_ingest_blocked():
                blocked_alerted = False
                state.set_ingest_blocked(False)
            log.info("Bulk sync: %s rows sent (no per-item data)", len(rows))
            time.sleep(cfg.sync_interval_sec)
            continue

        sent_ids: list[str] = []
        dead_ids_reasons: list[tuple[str, str]] = []
        handled: set[int] = set()
        pause_triggered = False

        for item in items:
            if not isinstance(item, dict):
                continue
            idx = item.get("index")
            if not isinstance(idx, int) or idx < 0 or idx >= len(rows):
                continue
            handled.add(idx)
            row_id = rows[idx]["id"]
            item_action, item_reason = _classify_item(item)

            if item_action == SENT:
                sent_ids.append(row_id)
            elif item_action == PAUSE:
                state.set_auth_failed(True)
                log.error(
                    "SYNC PAUSED via per-item 401. Fix TIMING_API_KEY and restart the node."
                )
                pause_triggered = True
                break
            elif item_action == KEEP:
                state.set_ingest_blocked(True)
                if not blocked_alerted:
                    log.error(
                        "INGEST BLOCKED (per-item) — %s. Row %s kept queued; "
                        "flush when event goes live.",
                        item_reason, row_id,
                    )
                    blocked_alerted = True
            else:  # DEAD
                dead_ids_reasons.append((row_id, item_reason))

        if not pause_triggered:
            # Rows the server omitted from items → treat as sent (server accepted, omitted entry).
            for i, row in enumerate(rows):
                if i not in handled:
                    sent_ids.append(row["id"])

        with db_lock:
            conn = conn_holder.get("conn")
            if conn:
                if sent_ids:
                    mark_reads_sent_bulk(conn, sent_ids)
                for rid, reason in dead_ids_reasons:
                    mark_read_dead(conn, rid, reason)
                    log.warning("Dead-lettered row %s: %s", rid, reason)

        if sent_ids:
            state.set_last_sync_at(utc_now_iso_ms())
        if sent_ids and (blocked_alerted or state.is_ingest_blocked()):
            blocked_alerted = False
            state.set_ingest_blocked(False)

        kept = len(rows) - len(sent_ids) - len(dead_ids_reasons)
        log.info(
            "Bulk sync: %s sent, %s kept, %s dead-lettered (of %s)",
            len(sent_ids), kept, len(dead_ids_reasons), len(rows),
        )

        time.sleep(cfg.sync_interval_sec)
