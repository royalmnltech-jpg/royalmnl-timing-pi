"""Shared timing-node state for reader and network worker threads."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ReaderState(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONFIGURING = "configuring"
    CAPTURING = "capturing"
    RECONNECTING = "reconnecting"


class NetworkState(str, Enum):
    OFFLINE = "offline"
    PROBING = "probing"
    ONLINE = "online"
    DEGRADED = "degraded"


@dataclass
class NodeState:
    """Thread-safe shared state for the timing node process."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    reader_state: ReaderState = ReaderState.DISCONNECTED
    network_state: NetworkState = NetworkState.OFFLINE
    clock_trusted: bool = True
    checkpoint_valid: bool = False
    assignment_pending: bool = True
    assigned_timing_event_id: Optional[str] = None
    assigned_checkpoint_id: Optional[str] = None
    assignment_version: Optional[int] = None
    last_assignment_check_mono: float = 0.0
    last_health_ok_mono: float = 0.0
    shutdown_requested: bool = False
    os_poweroff_requested: bool = False
    # Set when the backend rejects our API key (401). Sync pauses while true so we
    # neither hammer the server nor dead-letter reads; cleared only by restart (key is
    # fixed at boot). Reads stay queued and flush once the key is corrected.
    auth_failed: bool = False
    # Set when the backend reports the event is not ingestible (422). Reads stay queued
    # and flush once an operator flips the event live; surfaced for operator visibility.
    ingest_blocked: bool = False
    # UTC ISO timestamp of the most recent successful outbox flush. None until first sync.
    # Network loop attaches this to every assignment poll for server-side telemetry.
    last_sync_at: Optional[str] = None
    # Set when the reader is TCP-connected but emitting no tags beyond the stall window.
    # Forces a reconnect; surfaced to telemetry so operators see a dark-but-connected reader.
    reader_stalled: bool = False

    def set_reader_state(self, state: ReaderState) -> None:
        with self.lock:
            self.reader_state = state

    def get_reader_state(self) -> ReaderState:
        with self.lock:
            return self.reader_state

    def set_network_state(self, state: NetworkState) -> None:
        with self.lock:
            self.network_state = state

    def get_network_state(self) -> NetworkState:
        with self.lock:
            return self.network_state

    def set_assignment(
        self,
        *,
        timing_event_id: Optional[str],
        checkpoint_id: Optional[str],
        version: Optional[int],
        checkpoint_valid: bool,
        assignment_pending: bool,
    ) -> None:
        with self.lock:
            self.assigned_timing_event_id = timing_event_id
            self.assigned_checkpoint_id = checkpoint_id
            self.assignment_version = version
            self.checkpoint_valid = checkpoint_valid
            self.assignment_pending = assignment_pending

    def get_assignment_snapshot(self) -> tuple[
        Optional[str],
        Optional[str],
        Optional[int],
        bool,
        bool,
    ]:
        with self.lock:
            return (
                self.assigned_timing_event_id,
                self.assigned_checkpoint_id,
                self.assignment_version,
                self.checkpoint_valid,
                self.assignment_pending,
            )

    def set_clock_trusted(self, trusted: bool) -> None:
        with self.lock:
            self.clock_trusted = trusted

    def get_clock_trusted(self) -> bool:
        with self.lock:
            return self.clock_trusted

    def request_shutdown(self) -> None:
        with self.lock:
            self.shutdown_requested = True

    def is_shutdown_requested(self) -> bool:
        with self.lock:
            return self.shutdown_requested

    def set_auth_failed(self, failed: bool) -> None:
        with self.lock:
            self.auth_failed = failed

    def is_auth_failed(self) -> bool:
        with self.lock:
            return self.auth_failed

    def set_ingest_blocked(self, blocked: bool) -> None:
        with self.lock:
            self.ingest_blocked = blocked

    def is_ingest_blocked(self) -> bool:
        with self.lock:
            return self.ingest_blocked

    def request_os_poweroff(self) -> None:
        with self.lock:
            self.shutdown_requested = True
            self.os_poweroff_requested = True

    def is_os_poweroff_requested(self) -> bool:
        with self.lock:
            return self.os_poweroff_requested

    def set_last_sync_at(self, iso: Optional[str]) -> None:
        with self.lock:
            self.last_sync_at = iso

    def get_last_sync_at(self) -> Optional[str]:
        with self.lock:
            return self.last_sync_at

    def set_reader_stalled(self, stalled: bool) -> None:
        with self.lock:
            self.reader_stalled = stalled

    def is_reader_stalled(self) -> bool:
        with self.lock:
            return self.reader_stalled

    def touch_assignment_check(self, mono: float) -> None:
        with self.lock:
            self.last_assignment_check_mono = mono

    def touch_health_ok(self, mono: float) -> None:
        with self.lock:
            self.last_health_ok_mono = mono
