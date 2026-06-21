"""Scriptable fake timing backend for race-day emulation tests.

Mimics the timing server's HTTP surface (`/health`, node assignment, `/read`, `/reads`)
using only the stdlib. Tests flip `read_mode` / `assignment` at runtime to reproduce
race-day conditions: event not live (422), bad key (401), rate limiting (429), server
blips (503), duplicates, and partial-success batches.

Error envelopes mirror the real server: {"ok": false, "error": {code, message, details}}.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs

# Response modes for POST /read and /reads
MODE_OK = "ok"                 # 200 accepted
MODE_DUPLICATE = "duplicate"   # 200 isDuplicate=true
MODE_SEMANTIC_422 = "semantic422"   # 422 SEMANTICALLY_INVALID_TIMING_READ (e.g. event not live)
MODE_AUTH_401 = "auth401"      # 401 INVALID_API_KEY
MODE_BADPAYLOAD_400 = "badpayload400"  # 400 INVALID_PAYLOAD
MODE_RATELIMIT_429 = "ratelimit429"    # 429 RATE_LIMITED
MODE_SERVER_503 = "server503"  # 503 transient
# Bulk-only: each item resolved by index parity for partial-success testing
MODE_BULK_MIXED = "bulkmixed"


def _error_envelope(code: str, message: str) -> dict[str, Any]:
    return {
        "ok": False,
        "error": {"code": code, "message": message, "details": []},
        "meta": {"requestId": "fake", "timestamp": "1970-01-01T00:00:00Z"},
    }


def _success_read(is_duplicate: bool = False, resolved: bool = True) -> dict[str, Any]:
    return {
        "ok": True,
        "data": {
            "ingest": {
                "status": "accepted",
                "timingReadId": "tr_fake",
                "idempotency": {"isDuplicate": is_duplicate, "dedupeKey": "k"},
            },
            "resolution": {
                "status": "resolved" if resolved else "unresolved",
                "participantId": "1" if resolved else None,
                "reasonCode": None if resolved else "EPC_NOT_MAPPED",
            },
            "ranking": {"reason": "queued"},
        },
        "meta": {"requestId": "fake", "timestamp": "1970-01-01T00:00:00Z"},
    }


class FakeBackend:
    """Controllable in-process backend. Flip attributes from the test thread."""

    def __init__(self) -> None:
        self.read_mode: str = MODE_OK
        # assignment payload returned by the node assignment endpoint; None = unassigned
        self.assignment: Optional[dict[str, Any]] = {
            "timingEventId": "te_test",
            "checkpointId": "finish",
            "assignmentVersion": 1,
        }
        self.shutdown_requested: bool = False
        # Captured for assertions
        self.received_reads: list[dict[str, Any]] = []
        self.batch_sizes: list[int] = []
        self.assignment_queries: list[dict[str, list[str]]] = []
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    # --- lifecycle ---
    def start(self) -> "FakeBackend":
        backend = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: Any) -> None:  # silence stdlib logging
                pass

            def _send(self, status: int, body: dict[str, Any]) -> None:
                data = json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/health":
                    self._send(200, {"ok": True, "data": {"db": "ok"}, "meta": {}})
                    return
                if parsed.path.endswith("/assignment"):
                    with backend._lock:
                        backend.assignment_queries.append(parse_qs(parsed.query))
                        assignment = backend.assignment
                        shutdown = backend.shutdown_requested
                    self._send(
                        200,
                        {
                            "ok": True,
                            "data": {"assignment": assignment, "shutdownRequested": shutdown},
                            "meta": {},
                        },
                    )
                    return
                self._send(404, _error_envelope("NOT_FOUND", "not found"))

            def do_POST(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b""
                try:
                    payload = json.loads(raw) if raw else {}
                except (ValueError, TypeError):
                    payload = {}

                if parsed.path.endswith("/timing/read"):
                    self._handle_single(payload)
                    return
                if parsed.path.endswith("/timing/reads"):
                    self._handle_bulk(payload)
                    return
                self._send(404, _error_envelope("NOT_FOUND", "not found"))

            def _handle_single(self, payload: dict[str, Any]) -> None:
                with backend._lock:
                    mode = backend.read_mode
                if mode in (MODE_OK,):
                    with backend._lock:
                        backend.received_reads.append(payload)
                    self._send(200, _success_read())
                elif mode == MODE_DUPLICATE:
                    self._send(200, _success_read(is_duplicate=True))
                elif mode == MODE_SEMANTIC_422:
                    self._send(422, _error_envelope(
                        "SEMANTICALLY_INVALID_TIMING_READ",
                        "Timing event is not in a status that accepts reads.",
                    ))
                elif mode == MODE_AUTH_401:
                    self._send(401, _error_envelope("INVALID_API_KEY", "bad key"))
                elif mode == MODE_BADPAYLOAD_400:
                    self._send(400, _error_envelope("INVALID_PAYLOAD", "bad payload"))
                elif mode == MODE_RATELIMIT_429:
                    self._send(429, _error_envelope("RATE_LIMITED", "slow down"))
                elif mode == MODE_SERVER_503:
                    self._send(503, _error_envelope("INTERNAL_SERVER_ERROR", "blip"))
                else:
                    self._send(200, _success_read())

            def _handle_bulk(self, payload: dict[str, Any]) -> None:
                reads = payload.get("reads", []) if isinstance(payload, dict) else []
                with backend._lock:
                    mode = backend.read_mode
                # Top-level error responses (whole batch rejected before per-item processing)
                if mode == MODE_AUTH_401:
                    self._send(401, _error_envelope("INVALID_API_KEY", "bad key"))
                    return
                if mode == MODE_SERVER_503:
                    self._send(503, _error_envelope("INTERNAL_SERVER_ERROR", "blip"))
                    return
                if mode == MODE_SEMANTIC_422:
                    self._send(422, _error_envelope(
                        "SEMANTICALLY_INVALID_TIMING_READ",
                        "Timing event is not in a status that accepts reads.",
                    ))
                    return
                if mode == MODE_RATELIMIT_429:
                    self._send(429, _error_envelope("RATE_LIMITED", "slow down"))
                    return
                if mode == MODE_BADPAYLOAD_400:
                    self._send(400, _error_envelope("INVALID_PAYLOAD", "bad payload"))
                    return
                # Per-item responses (200 with partial-success items array)
                items: list[dict[str, Any]] = []
                accepted = 0
                semantic = 0
                new_reads: list[dict[str, Any]] = []
                for i, _read in enumerate(reads):
                    if mode == MODE_BULK_MIXED and i % 7 == 3:
                        items.append({"index": i, "ok": False, "error": {
                            "code": "SEMANTICALLY_INVALID_TIMING_READ",
                            "message": "not live", "details": []}})
                        semantic += 1
                    elif mode == MODE_BULK_MIXED and i % 7 == 5:
                        items.append({"index": i, "ok": False, "error": {
                            "code": "INVALID_PAYLOAD", "message": "bad", "details": []}})
                    else:
                        items.append({"index": i, "ok": True, "ingest": {
                            "status": "accepted", "timingReadId": f"tr_{i}",
                            "idempotency": {"isDuplicate": False, "dedupeKey": "k"}}})
                        accepted += 1
                        new_reads.append(_read)
                with backend._lock:
                    backend.received_reads.extend(new_reads)
                    backend.batch_sizes.append(len(reads))
                self._send(200, {
                    "ok": True,
                    "data": {
                        "summary": {"total": len(reads), "accepted": accepted,
                                    "duplicates": 0, "unresolved": 0, "debounced": 0,
                                    "semanticErrors": semantic},
                        "items": items,
                    },
                    "meta": {},
                })

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()

    @property
    def base_url(self) -> str:
        assert self._server is not None, "server not started"
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def set_mode(self, mode: str) -> None:
        with self._lock:
            self.read_mode = mode
