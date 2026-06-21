"""Fake A0 UHF RFID reader (TCP) for race-day emulation tests.

Speaks enough of the A0 protocol that the real `reader_loop` / `reader_protocol` code
connects, configures, runs health checks, and ingests tag frames against it — no hardware.

Reuses `reader_protocol.build_command` + `A0Framer` so every frame is byte-valid.

Modes (set on the instance, read per inventory round):
  - "trickle": emit one EPC per round, cycling through `epcs`
  - "burst":   emit all `epcs` in a single round (mass-finish)
  - "dup":     emit `epcs[0]` repeatedly (duplicate storm)
  - "stall":   emit nothing and no round-done frame (silent reader — for the P5 watchdog)

`power_dbm` is echoed back on the "Get Output Power" health check so tests can assert 1B.
"""

from __future__ import annotations

import os
import socket
import sys
import threading
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reader_protocol import A0Framer, build_command  # noqa: E402

# Command bytes we recognize from the client
CMD_SET_POWER = 0x66
CMD_SET_BEEPER = 0x7A
CMD_SET_RF_PROFILE = 0x69
CMD_GET_POWER = 0x77
CMD_GET_ANTENNA = 0x75
CMD_GET_ANT_DETECT = 0x63
CMD_GET_FIRMWARE = 0x72
CMD_GET_TEMP = 0x7B
INVENTORY_CMDS = (0x89, 0x8A, 0x8B)


def _ack(cmd: int) -> bytes:
    """Generic success reply (byte[4] == 0x10 is what setup_sequence checks)."""
    return build_command(cmd, [0x10])


def _tag_frame(cmd: int, epc_hex: str, ant: int = 0, rssi: int = 0x55) -> bytes:
    """A valid inventory tag frame: data = freq_ant, pc(2), epc bytes, rssi."""
    epc_bytes = list(bytes.fromhex(epc_hex))
    freq_ant = ant & 0x03
    data = [freq_ant, 0x30, 0x00] + epc_bytes + [rssi & 0x7F]
    return build_command(cmd, data)


def _round_done(cmd: int, total: int) -> bytes:
    """Round-complete frame: ln must be 0x0A (data len 7) for the 0x89/0x8B 'done' path."""
    data = [0x00, 0x00, 0x00, 0x00, (total >> 16) & 0xFF, (total >> 8) & 0xFF, total & 0xFF]
    return build_command(cmd, data)


class FakeReader:
    def __init__(self, epcs: Optional[list[str]] = None, mode: str = "trickle",
                 power_dbm: int = 0x1A) -> None:
        self.epcs = epcs or ["E2000017221101441890C0A1"]
        self.mode = mode
        self.power_dbm = power_dbm
        # Captured for assertions
        self.set_power_values: list[int] = []
        self._trickle_idx = 0
        self._server: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.port = 0

    def start(self) -> "FakeReader":
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(1)
        self.port = self._server.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        return self

    def set_mode(self, mode: str) -> None:
        self.mode = mode

    def stop(self) -> None:
        self._stop.set()
        if self._server is not None:
            try:
                self._server.close()
            except OSError:
                pass

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                self._server.settimeout(0.5)
                conn, _ = self._server.accept()
            except (socket.timeout, OSError):
                continue
            try:
                self._handle(conn)
            except OSError:
                pass
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

    def _handle(self, conn: socket.socket) -> None:
        framer = A0Framer()
        conn.settimeout(0.2)
        while not self._stop.is_set():
            try:
                chunk = conn.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                return
            if chunk == b"":
                return
            framer.feed(chunk)
            while True:
                pkt = framer.pop_packet()
                if pkt is None:
                    break
                if len(pkt) < 4:
                    continue
                self._dispatch(conn, pkt)

    def _dispatch(self, conn: socket.socket, pkt: bytes) -> None:
        cmd = pkt[3]
        if cmd == CMD_SET_POWER:
            # data starts at index 4 (before checksum)
            if len(pkt) >= 6:
                self.set_power_values.append(pkt[4])
            conn.sendall(_ack(cmd))
        elif cmd in (CMD_SET_BEEPER, CMD_SET_RF_PROFILE):
            conn.sendall(_ack(cmd))
        elif cmd == CMD_GET_POWER:
            conn.sendall(build_command(CMD_GET_POWER, [self.power_dbm]))
        elif cmd == CMD_GET_ANTENNA:
            conn.sendall(build_command(CMD_GET_ANTENNA, [0x00]))
        elif cmd == CMD_GET_ANT_DETECT:
            conn.sendall(build_command(CMD_GET_ANT_DETECT, [0x00]))
        elif cmd == CMD_GET_FIRMWARE:
            conn.sendall(build_command(CMD_GET_FIRMWARE, [0x03, 0x08]))
        elif cmd == CMD_GET_TEMP:
            conn.sendall(build_command(CMD_GET_TEMP, [0x00, 0x1B]))
        elif cmd in INVENTORY_CMDS:
            self._emit_inventory(conn, cmd)
        # unknown commands are ignored (reader would just not reply)

    def _emit_inventory(self, conn: socket.socket, cmd: int) -> None:
        if self.mode == "stall":
            return  # silent: no tags, no done frame -> client round times out
        if self.mode == "burst":
            emit = list(self.epcs)
        elif self.mode == "dup":
            emit = [self.epcs[0]] * max(1, len(self.epcs))
        else:  # trickle
            emit = [self.epcs[self._trickle_idx % len(self.epcs)]]
            self._trickle_idx += 1
        for epc in emit:
            conn.sendall(_tag_frame(cmd, epc))
        conn.sendall(_round_done(cmd, len(emit)))
