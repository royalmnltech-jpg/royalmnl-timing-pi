"""A0 RFID reader protocol helpers (shared with test_reader)."""

from __future__ import annotations

import os
import socket
import time
from typing import Callable, Optional

READER_ADDRESS = 0x01

INV_POLL_MODE = os.environ.get("INV_POLL_MODE", "8b").strip().lower()
INV_SESSION_S1 = 0x01
INV_TARGET_A = 0x00
INV_REPEAT = int(os.environ.get("INV_REPEAT", "255"), 0) & 0xFF

FAST_SWITCH_ENABLED = os.environ.get("FAST_SWITCH_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
)
FAST_SWITCH_ANT_COUNT = os.environ.get("FAST_SWITCH_ANT_COUNT", "auto").strip().lower()

INV_ANT_PRIMARY = 0x00
INV_STAY_PRIMARY = 0x0A
INV_ANT_UNUSED = 0x04
INV_STAY_UNUSED = 0x00
INV_INTERVAL = 0x00
INV_REPEAT_FAST = 0x01

INV_ROUND_TIMEOUT_SEC = float(os.environ.get("INV_ROUND_TIMEOUT_SEC", "15"))
POLL_PAUSE_SEC = float(os.environ.get("POLL_PAUSE_SEC", "0.02"))
DEBUG_FRAMES = os.environ.get("DEBUG_FRAMES", "").strip() in ("1", "true", "yes")


def build_command(cmd: int, data: Optional[list[int]] = None) -> bytes:
    if data is None:
        data = []
    length = len(data) + 3
    sum_bytes = 0xA0 + length + READER_ADDRESS + cmd + sum(data)
    checksum = (~sum_bytes + 1) & 0xFF
    return bytes([0xA0, length, READER_ADDRESS, cmd] + data + [checksum])


CMD_SESSION_INVENTORY = build_command(0x8B, [INV_SESSION_S1, INV_TARGET_A, INV_REPEAT])
CMD_REALTIME_INVENTORY = build_command(0x89, [INV_REPEAT])
CMD_FAST_SWITCH_INVENTORY_4 = build_command(
    0x8A,
    [
        0x00,
        INV_STAY_PRIMARY,  # A = antenna 0
        0x01,
        INV_STAY_PRIMARY,  # B = antenna 1
        0x02,
        INV_STAY_PRIMARY,  # C = antenna 2
        0x03,
        INV_STAY_PRIMARY,  # D = antenna 3
        INV_INTERVAL,
        INV_REPEAT_FAST,
    ],
)
# V3.8 extended 8-ant 0x8A format: A..H + Interval + Reserve0(5 bytes) + Session + Target
# + Reserve1..3 + Phase + Repeat
CMD_FAST_SWITCH_INVENTORY_8 = build_command(
    0x8A,
    [
        # A..H antenna IDs / stay rounds
        0x00,
        INV_STAY_PRIMARY,  # A
        0x01,
        INV_STAY_PRIMARY,  # B
        0x02,
        INV_STAY_PRIMARY,  # C
        0x03,
        INV_STAY_PRIMARY,  # D
        0x04,
        INV_STAY_PRIMARY,  # E
        0x05,
        INV_STAY_PRIMARY,  # F
        0x06,
        INV_STAY_PRIMARY,  # G
        0x07,
        INV_STAY_PRIMARY,  # H
        INV_INTERVAL,
        # reserve0 (5 bytes)
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        # session / target
        INV_SESSION_S1,
        INV_TARGET_A,
        # reserve1..3
        0x00,
        0x00,
        0x00,
        # phase disabled, repeat
        0x00,
        INV_REPEAT_FAST,
    ],
)
_PROBE_STAY = 0x01  # 1 round per antenna; only used during mode selection probe
_CMD_PROBE_8 = build_command(
    0x8A,
    [
        0x00,
        _PROBE_STAY,
        0x01,
        _PROBE_STAY,
        0x02,
        _PROBE_STAY,
        0x03,
        _PROBE_STAY,
        0x04,
        _PROBE_STAY,
        0x05,
        _PROBE_STAY,
        0x06,
        _PROBE_STAY,
        0x07,
        _PROBE_STAY,
        INV_INTERVAL,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,  # reserve0 (5 bytes)
        INV_SESSION_S1,
        INV_TARGET_A,
        0x00,
        0x00,
        0x00,  # reserve1..3
        0x00,
        INV_REPEAT_FAST,  # phase disabled, repeat
    ],
)
_CMD_PROBE_4 = build_command(
    0x8A,
    [
        0x00,
        _PROBE_STAY,  # A = antenna 0
        0x01,
        _PROBE_STAY,  # B = antenna 1
        0x02,
        _PROBE_STAY,  # C = antenna 2
        0x03,
        _PROBE_STAY,  # D = antenna 3
        INV_INTERVAL,
        INV_REPEAT_FAST,
    ],
)
# claude --resume 6dd818a5-380f-4c5a-bb00-8229991c579f
setup_sequence = {
    # 26 dBm finish-line power. 10 dBm (0x0A) was a bench value with sub-meter range.
    # If the reader caps lower, the "Get Output Power" health reply below shows the clamp.
    "Set Temp Output Power (26dBm)": (build_command(0x66, [0x1A]), 0.5),
    "Set Beeper Mode": (build_command(0x7A, [0x00]), 0.10),
    "Set RF Link Profile": (build_command(0x69, [0xD1]), 1),
}

HEALTH_CHECKS = {
    "Get Output Power": build_command(0x77, []),
    "Get Work Antenna": build_command(0x75, []),
    "Get Ant connection detector": build_command(0x63, []),
    "Get Firmware Version": build_command(0x72, []),
    "Get Temperature": build_command(0x7B, []),
}


def parse_health_reply(name: str, reply: bytes) -> str:
    hex_list = reply.hex("-").upper().split("-")
    if name == "Get Work Antenna":
        antenna_id = int(hex_list[4], 16)
        return f"Antenna {antenna_id + 1} (ID=0x{antenna_id:02X})"
    if name == "Get Temperature":
        return f"{int(hex_list[5], 16)}°C"
    if name == "Get Firmware Version":
        return f"v{int(hex_list[4], 16)}.{int(hex_list[5], 16)}"
    if name == "Get Output Power":
        if reply[1] == 0x04:
            return f"{reply[4]} dBm (all antennas same)"
        if reply[1] == 0x07:
            return (
                f"Ant1={reply[4]}dBm Ant2={reply[5]}dBm "
                f"Ant3={reply[6]}dBm Ant4={reply[7]}dBm"
            )
    if name == "Get Ant connection detector" and reply[1] == 0x04 and len(reply) > 4:
        return (
            "off" if reply[4] == 0x00 else f"sensitivity={reply[4]} (0x{reply[4]:02X})"
        )
    return reply.hex(" ").upper()


def flush_tcp_input(sock: socket.socket, max_total_sec: float = 0.25) -> None:
    sock.settimeout(0.02)
    deadline = time.monotonic() + max_total_sec
    while time.monotonic() < deadline:
        try:
            if not sock.recv(8192):
                return
        except socket.timeout:
            return


def _physical_antenna_id(freq_ant: int, rssi_byte: int) -> int:
    base = (freq_ant & 0x03) + 1
    if rssi_byte & 0x80:
        return base + 4
    return base


def parse_inventory_reply(pkt: bytes, cmd: int):
    if len(pkt) < 6 or pkt[0] != 0xA0 or pkt[3] != cmd:
        return ("misc", pkt.hex(" ").upper())
    ln = pkt[1]
    if ln == 0x04 and len(pkt) >= 6:
        return ("error", f"0x{pkt[4]:02X}")
    if cmd == 0x8A and ln == 0x05 and len(pkt) >= 7:
        return ("ant_fault", {"ant_id": pkt[4], "code": f"0x{pkt[5]:02X}"})
    if cmd == 0x8A and ln == 0x0A and len(pkt) >= 12:
        total = int.from_bytes(pkt[4:7], "big")
        duration_ms = int.from_bytes(pkt[7:11], "big")
        return ("done", {"total": total, "duration_ms": duration_ms})
    if cmd in (0x89, 0x8B) and ln == 0x0A and len(pkt) >= 12:
        ant_id = int.from_bytes(pkt[4:6], "big")
        read_rate = int.from_bytes(pkt[6:8], "big")
        total = int.from_bytes(pkt[8:11], "big")
        return ("done", {"total": total, "read_rate": read_rate, "hw_ant": ant_id})
    if cmd in (0x89, 0x8B) and ln == 0x0B and len(pkt) >= 13:
        ant_id = int.from_bytes(pkt[4:6], "big")
        read_rate = int.from_bytes(pkt[6:8], "big")
        total = int.from_bytes(pkt[8:12], "big")
        return ("done", {"total": total, "read_rate": read_rate, "hw_ant": ant_id})
    epc_len = ln - 7
    if epc_len < 0 or len(pkt) < 2 + ln:
        return ("misc", pkt.hex(" ").upper())
    freq_ant = pkt[4]
    pc = pkt[5:7]
    epc = pkt[7 : 7 + epc_len]
    rssi_b = pkt[7 + epc_len]
    ant = _physical_antenna_id(freq_ant, rssi_b)
    rssi_val = rssi_b & 0x7F
    return (
        "tag",
        {
            "epc": epc.hex().upper(),
            "pc": pc.hex().upper(),
            "ant": ant,
            "rssi": rssi_val,
        },
    )


class A0Framer:
    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, chunk: Optional[bytes]) -> None:
        if chunk:
            self._buf.extend(chunk)

    def pop_packet(self):
        while True:
            if len(self._buf) < 2:
                return None
            if self._buf[0] != 0xA0:
                try:
                    i = self._buf.index(0xA0)
                    del self._buf[:i]
                except ValueError:
                    self._buf.clear()
                continue
            plen = self._buf[1]
            if plen > 250 or plen < 3:
                del self._buf[0]
                continue
            need = 2 + plen
            if len(self._buf) < need:
                return None
            pkt = bytes(self._buf[:need])
            del self._buf[:need]
            return pkt


def drain_inventory_round(
    sock: socket.socket,
    framer: A0Framer,
    overall_deadline: float,
    cmd: int,
    on_tag: Callable[[dict], None],
) -> tuple[int, bool]:
    tags_printed = 0
    sock.settimeout(0.05)
    while time.monotonic() < overall_deadline:
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            chunk = None
        if chunk == b"":
            break
        if chunk:
            framer.feed(chunk)
        while True:
            pkt = framer.pop_packet()
            if pkt is None:
                break
            kind, payload = parse_inventory_reply(pkt, cmd)
            if DEBUG_FRAMES:
                print(f"  [dbg {kind}] {pkt.hex(' ').upper()}")
            if kind == "tag":
                on_tag(payload)
                tags_printed += 1
            elif kind == "done":
                return tags_printed, True
            elif kind == "error":
                return tags_printed, True
            elif kind == "ant_fault":
                pass  # mid-cycle; reader sends "done" after all antennas complete
    return tags_printed, False


def run_configuration_and_health(client: socket.socket, log) -> None:
    log.info("Reader configuration starting")
    for name, (cmd_bytes, wait) in setup_sequence.items():
        client.sendall(cmd_bytes)
        time.sleep(wait)
        try:
            reply = client.recv(1024)
            ok = len(reply) > 4 and reply[4] == 0x10
            log.debug("%s: %s", name, "OK" if ok else "reply")
        except socket.timeout:
            log.warning("Timeout: %s", name)

    for name, cmd_bytes in HEALTH_CHECKS.items():
        client.sendall(cmd_bytes)
        time.sleep(0.1)
        try:
            reply = client.recv(1024)
            log.info("%s: %s", name, parse_health_reply(name, reply))
        except OSError:
            log.warning("%s: read failed", name)

    flush_tcp_input(client)
    log.info("Reader configuration complete")


def inventory_mode() -> tuple[int, bytes, str]:
    if FAST_SWITCH_ENABLED:
        if FAST_SWITCH_ANT_COUNT == "4":
            return 0x8A, CMD_FAST_SWITCH_INVENTORY_4, "0x8A-4ant"
        if FAST_SWITCH_ANT_COUNT == "8":
            return 0x8A, CMD_FAST_SWITCH_INVENTORY_8, "0x8A-8ant"
        # auto mode handled by select_inventory_mode()
        return 0x8A, CMD_FAST_SWITCH_INVENTORY_4, "0x8A-4ant"
    if INV_POLL_MODE in ("89", "realtime", "rt"):
        return 0x89, CMD_REALTIME_INVENTORY, "0x89"
    return 0x8B, CMD_SESSION_INVENTORY, "0x8B"


def _probe_mode(
    sock: socket.socket, cmd: int, inv_bytes: bytes, timeout_sec: float = 0.8
):
    """Return (supported, ant_fault_port_if_any)."""
    framer = A0Framer()
    deadline = time.monotonic() + timeout_sec
    sock.sendall(inv_bytes)
    sock.settimeout(0.05)
    ant_fault_port = None
    while time.monotonic() < deadline:
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            chunk = None
        if chunk == b"":
            break
        if chunk:
            framer.feed(chunk)
        while True:
            pkt = framer.pop_packet()
            if pkt is None:
                break
            kind, payload = parse_inventory_reply(pkt, cmd)
            if kind == "tag" or kind == "done":
                return True, ant_fault_port
            if kind == "ant_fault":
                ant_fault_port = payload.get("ant_id")
                # packet format recognized; may still be unsuitable for this antenna count
                return True, ant_fault_port
            if kind == "error":
                return False, ant_fault_port
    return False, ant_fault_port


def select_inventory_mode(sock: socket.socket, log) -> tuple[int, bytes, str]:
    """
    Runtime selection:
    - FAST_SWITCH_ANT_COUNT=8: force 8-ant 0x8A
    - FAST_SWITCH_ANT_COUNT=4: force 4-ant 0x8A
    - FAST_SWITCH_ANT_COUNT=auto: try 8-ant then 4-ant, then fallback to 0x8B/0x89
    """
    if not FAST_SWITCH_ENABLED:
        mode = inventory_mode()
        log.info("Inventory mode selected: %s", mode[2])
        return mode

    if FAST_SWITCH_ANT_COUNT in ("8", "4"):
        mode = (
            (0x8A, CMD_FAST_SWITCH_INVENTORY_8, "0x8A-8ant")
            if FAST_SWITCH_ANT_COUNT == "8"
            else (0x8A, CMD_FAST_SWITCH_INVENTORY_4, "0x8A-4ant")
        )
        log.info("Inventory mode selected: %s (forced)", mode[2])
        return mode

    # auto: probe 8 first
    ok8, fault8 = _probe_mode(sock, 0x8A, _CMD_PROBE_8)
    if ok8 and (fault8 is None or int(fault8) <= 3):
        log.info("Inventory mode selected: 0x8A-8ant (auto)")
        flush_tcp_input(sock)
        return 0x8A, CMD_FAST_SWITCH_INVENTORY_8, "0x8A-8ant"
    if ok8 and fault8 is not None and int(fault8) > 3:
        log.warning("8-ant probe fault on port %s; falling back to 4-ant mode", fault8)

    flush_tcp_input(sock)  # drain lingering 8-ant data before 4-ant probe
    ok4, _ = _probe_mode(sock, 0x8A, _CMD_PROBE_4)
    if ok4:
        log.info("Inventory mode selected: 0x8A-4ant (auto)")
        flush_tcp_input(sock)
        return 0x8A, CMD_FAST_SWITCH_INVENTORY_4, "0x8A-4ant"

    # legacy fallback
    if INV_POLL_MODE in ("89", "realtime", "rt"):
        log.warning("Fast-switch unavailable; fallback to 0x89")
        flush_tcp_input(sock)
        return 0x89, CMD_REALTIME_INVENTORY, "0x89"
    log.warning("Fast-switch unavailable; fallback to 0x8B")
    flush_tcp_input(sock)
    return 0x8B, CMD_SESSION_INVENTORY, "0x8B"
