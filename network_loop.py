"""Network worker: health probe + timing-node assignment polling."""

from __future__ import annotations

import json
import logging
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional

from config import NodeConfig
from db import backfill_assignment
from node_state import NetworkState, NodeState


def _jitter(base: float, spread: float = 0.2) -> float:
    lo = base * (1.0 - spread)
    hi = base * (1.0 + spread)
    return random.uniform(lo, hi)


def _get_json(url: str, api_key: str, timeout: float = 15.0) -> tuple[int, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "x-api-key": api_key,
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return resp.status, json.loads(body)


def run_network_loop(
    state: NodeState,
    cfg: NodeConfig,
    conn_holder: dict,
    db_lock: threading.Lock,
    log: logging.Logger,
) -> None:
    """
    Poll /health and assignment endpoint; update shared state and backfill SQLite.
    conn_holder['conn'] is the sqlite3 connection.
    """
    base = cfg.timing_api_base_url.rstrip("/")
    node_enc = urllib.parse.quote(cfg.timing_node_id, safe="")
    assign_url = f"{base}/api/v1/timing/nodes/{node_enc}/assignment"
    health_url = f"{base}/health"

    backoff = 2.0
    prev_event: Optional[str] = None
    prev_cp: Optional[str] = None
    prev_ver: Optional[int] = None
    prev_net_state: Optional[NetworkState] = None

    while not state.is_shutdown_requested():
        state.set_network_state(NetworkState.PROBING)
        log.debug("Probing backend: %s", health_url)
        try:
            hreq = urllib.request.Request(health_url, method="GET")
            with urllib.request.urlopen(hreq, timeout=10.0) as hresp:
                if hresp.status != 200:
                    raise RuntimeError(f"health HTTP {hresp.status}")
            state.touch_health_ok(time.monotonic())

            status, payload = _get_json(assign_url, cfg.timing_api_key)
            if status != 200:
                raise RuntimeError(f"assignment HTTP {status}")

            data = payload.get("data") if isinstance(payload, dict) else None

            if isinstance(data, dict) and data.get("shutdownRequested"):
                log.warning("OS poweroff requested by server — draining and shutting down")
                state.request_os_poweroff()
                break

            assignment = data.get("assignment") if isinstance(data, dict) else None

            if assignment is None:
                state.set_assignment(
                    timing_event_id=None,
                    checkpoint_id=None,
                    version=None,
                    checkpoint_valid=False,
                    assignment_pending=True,
                )
                new_net_state = NetworkState.DEGRADED
                state.set_network_state(new_net_state)
                if prev_net_state != new_net_state:
                    log.info("Backend reachable — no assignment for node %s", cfg.timing_node_id)
                else:
                    log.warning("No assignment for node %s", cfg.timing_node_id)
                prev_net_state = new_net_state
            else:
                te = assignment.get("timingEventId")
                cp = assignment.get("checkpointId")
                ver = assignment.get("assignmentVersion")
                if te and cp:
                    boundary = (
                        prev_event is not None
                        and (prev_event != te or prev_cp != cp or prev_ver != ver)
                    )
                    if boundary:
                        log.warning(
                            "Assignment boundary: old=(%s,%s,v=%s) new=(%s,%s,v=%s)",
                            prev_event,
                            prev_cp,
                            prev_ver,
                            te,
                            cp,
                            ver,
                        )
                    prev_event, prev_cp, prev_ver = te, cp, ver

                    state.set_assignment(
                        timing_event_id=te,
                        checkpoint_id=cp,
                        version=int(ver) if ver is not None else None,
                        checkpoint_valid=True,
                        assignment_pending=False,
                    )
                    new_net_state = NetworkState.ONLINE
                    state.set_network_state(new_net_state)
                    if prev_net_state != new_net_state:
                        log.info("Backend ONLINE — assigned event=%s checkpoint=%s v=%s", te, cp, ver)
                    prev_net_state = new_net_state

                    with db_lock:
                        conn = conn_holder.get("conn")
                        if conn:
                            backfill_assignment(conn, timing_event_id=te, checkpoint_id=cp, log=log)
                else:
                    state.set_network_state(NetworkState.DEGRADED)
                    prev_net_state = NetworkState.DEGRADED

            backoff = 2.0
            state.touch_assignment_check(time.monotonic())

        except Exception as e:
            new_net_state = NetworkState.OFFLINE
            if prev_net_state != new_net_state:
                log.warning("Backend OFFLINE: %s", e)
            else:
                log.debug("Backend still offline: %s", e)
            prev_net_state = new_net_state
            state.set_network_state(NetworkState.OFFLINE)
            time.sleep(_jitter(min(backoff, 60.0)))
            backoff = min(backoff * 2.0, 60.0)
            continue

        # Stable poll interval when online + valid checkpoint; else faster poll
        snap = state.get_assignment_snapshot()
        _, _, _, checkpoint_valid, _ = snap
        interval = (
            cfg.assignment_poll_stable_sec if checkpoint_valid else cfg.assignment_poll_sec
        )
        time.sleep(_jitter(interval))
