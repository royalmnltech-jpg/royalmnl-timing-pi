import os
import socket
import sys
import time
from datetime import datetime, timezone

READER_IP = "192.168.1.200"
READER_PORT = 4000
READER_ADDRESS = 0x01

# --- Inventory polling (ISO18000-6C EPC) ---
# Protocol recommends 0x8B session inventory (S1 + Target A) for normal polling (V3.x §2.2.1.3).
# Set INV_POLL_MODE=89 to use cmd_real_time_inventory only.
INV_POLL_MODE = os.environ.get("INV_POLL_MODE", "8b").strip().lower()
INV_SESSION_S1 = 0x01
INV_TARGET_A = 0x00
# 0xFF = shortest round when few tags (per doc); lower = shorter wall-clock for debugging.
INV_REPEAT = int(os.environ.get("INV_REPEAT", "255"), 0) & 0xFF

# Optional: 0x8A fast switch (multi-ant).
USE_FAST_SWITCH_8A = False

# 0x62: 0x00 = disable antenna connection detection (else reader returns 0x22 on inventory).
DISABLE_ANT_CONNECTION_DETECT = True
INV_ANT_PRIMARY = 0x00
INV_STAY_PRIMARY = 0x0A
INV_ANT_UNUSED = 0x04
INV_STAY_UNUSED = 0x00
INV_INTERVAL = 0x00
INV_REPEAT_FAST = 0x01

INV_ROUND_TIMEOUT_SEC = 15.0
POLL_PAUSE_SEC = 0.02
DEBUG_FRAMES = os.environ.get("DEBUG_FRAMES", "").strip() in ("1", "true", "yes")


def build_command(cmd, data=None):
    if data is None:
        data = []
    length = len(data) + 3
    sum_bytes = 0xA0 + length + READER_ADDRESS + cmd + sum(data)
    checksum = (~sum_bytes + 1) & 0xFF
    return bytes([0xA0, length, READER_ADDRESS, cmd] + data + [checksum])


CMD_SESSION_INVENTORY = build_command(
    0x8B,
    [INV_SESSION_S1, INV_TARGET_A, INV_REPEAT],
)

CMD_REALTIME_INVENTORY = build_command(0x89, [INV_REPEAT])

CMD_FAST_SWITCH_INVENTORY = build_command(
    0x8A,
    [
        INV_ANT_PRIMARY,
        INV_STAY_PRIMARY,
        INV_ANT_UNUSED,
        INV_STAY_UNUSED,
        INV_ANT_UNUSED,
        INV_STAY_UNUSED,
        INV_ANT_UNUSED,
        INV_STAY_UNUSED,
        INV_INTERVAL,
        INV_REPEAT_FAST,
    ],
)

CMD_DISABLE_ANT_CONNECTION_DETECT = build_command(0x62, [0x00])
CMD_GET_WORK_ANTENNA = build_command(0x75, [])

setup_sequence = {
    # **(
    #     {
    #         "Disable antenna connection detector (0x62=off)": (
    #             CMD_DISABLE_ANT_CONNECTION_DETECT,
    #             0.15,
    #         )
    #     }
    #     if DISABLE_ANT_CONNECTION_DETECT
    #     else {}
    # ),
    
    "Set Temp Output Power (10dBm)": (
        build_command(0x66, [0x0A]),
        0.5,
    ),
    "Set Beeper Quiet": (build_command(0x7A, [0x02]), 0.10),
    "Set RF Link Profile": (build_command(0x69, [0xD1]), 1),
}

HEALTH_CHECKS = {
    "Get Output Power": build_command(0x77, []),
    "Get Work Antenna": build_command(0x75, []),
    "Get Ant connection detector": build_command(0x63, []),
    "Get Firmware Version": build_command(0x72, []),
    "Get Temperature": build_command(0x7B, []),
}


def parse_work_antenna_id(reply):
    """Return AntennaID byte from Get Work Antenna (0x75) reply, or None."""
    if len(reply) < 6 or reply[0] != 0xA0 or reply[3] != 0x75:
        return None
    return reply[4]


def query_and_reapply_work_antenna(client, wait=0.1):
    """0x75 get current work antenna, then 0x74 set to that same ID."""
    client.sendall(CMD_GET_WORK_ANTENNA)
    time.sleep(wait)
    try:
        reply = client.recv(1024)
    except OSError as e:
        print(f"  [TIMEOUT] Get Work Antenna (0x75): {e}")
        return
    ant_id = parse_work_antenna_id(reply)
    if ant_id is None:
        print(f"  [REPLY RCVD] Get Work Antenna — bad frame: {reply.hex(' ').upper()}")
        return
    human = ant_id + 1
    print(
        f"  [QUERY] Get Work Antenna (0x75): ID=0x{ant_id:02X} "
        f"(Antenna {human})"
    )
    set_cmd = build_command(0x74, [ant_id])
    client.sendall(set_cmd)
    time.sleep(wait)
    try:
        sreply = client.recv(1024)
        ok = len(sreply) > 4 and sreply[4] == 0x10
        status = "SUCCESS" if ok else "REPLY RCVD"
        print(
            f"  [{status}] Set Work Antenna (0x74) → ID=0x{ant_id:02X} "
            f"(Antenna {human})"
        )
    except OSError:
        print(f"  [TIMEOUT] Set Work Antenna (0x74) ID=0x{ant_id:02X}")


def parse_health_reply(name, reply):
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


def flush_tcp_input(sock, max_total_sec=0.25):
    """Drop any leftover bytes so the inventory framer starts aligned."""
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


def parse_inventory_reply(pkt, cmd):
    """Parse 0x89 / 0x8B / 0x8A: EPC row, round done, error, or (8A only) antenna fault."""
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
        # AntID(2) + ReadRate(2) + TotalRead(3) + Check — frame 2+0x0A
        ant_id = int.from_bytes(pkt[4:6], "big")
        read_rate = int.from_bytes(pkt[6:8], "big")
        total = int.from_bytes(pkt[8:11], "big")
        return ("done", {"total": total, "read_rate": read_rate, "hw_ant": ant_id})
    if cmd in (0x89, 0x8B) and ln == 0x0B and len(pkt) >= 13:
        # Some FW uses TotalRead as 32-bit
        ant_id = int.from_bytes(pkt[4:6], "big")
        read_rate = int.from_bytes(pkt[6:8], "big")
        total = int.from_bytes(pkt[8:12], "big")
        return ("done", {"total": total, "read_rate": read_rate, "hw_ant": ant_id})
    # Tag row: Len = 7 + len(EPC); same for 0x89 / 0x8A / 0x8B (phase off).
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
    """Len = bytes from Address through Check; frame size = 2 + Len."""

    def __init__(self):
        self._buf = bytearray()

    def feed(self, chunk):
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


def drain_inventory_round(sock, framer, overall_deadline, cmd, verbose_done=False):
    """Process inventory replies until completion, error, or deadline."""
    tags_printed = 0
    sock.settimeout(0.05)
    label = f"0x{cmd:02X}"
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
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                print(
                    f"  [{ts}] EPC={payload['epc']} "
                    f"ant={payload['ant']} rssi={payload['rssi']} pc={payload['pc']}"
                )
                tags_printed += 1
            elif kind == "done":
                if cmd == 0x8A:
                    print(
                        f"  [round {label}] total={payload['total']} "
                        f"duration_ms={payload['duration_ms']} epc_rows={tags_printed}"
                    )
                else:
                    print(
                        f"  [round {label}] total={payload['total']} "
                        f"rate={payload['read_rate']} ant_raw=0x{payload['hw_ant']:04X} "
                        f"epc_rows={tags_printed}"
                    )
                return tags_printed, True
            elif kind == "error":
                if payload == "0x22":
                    print(
                        f"  [{label} error] {payload} antenna-missing "
                        "(enable DISABLE_ANT_CONNECTION_DETECT / 0x62, or fix coax)"
                    )
                else:
                    print(f"  [{label} error] {payload}")
                return tags_printed, True
            elif kind == "ant_fault":
                print(
                    f"  [{label} antenna] port={payload['ant_id']} code={payload['code']} "
                    "(set 0x62 sensitivity 0x00 or fix antenna)"
                )
                return tags_printed, True
            elif kind == "misc" and len(pkt) > 3 and pkt[3] == cmd:
                print(f"  [{label} ?] {payload}")
    return tags_printed, False


def run_configuration_and_health(client):
    print("--- STEP 1: CONFIGURATION ---")
    query_and_reapply_work_antenna(client)
    for name, (cmd_bytes, wait) in setup_sequence.items():
        client.sendall(cmd_bytes)
        time.sleep(wait)
        try:
            reply = client.recv(1024)
            status = "SUCCESS" if len(reply) > 4 and reply[4] == 0x10 else "REPLY RCVD"
            print(f"  [{status}] {name}")
        except socket.timeout:
            print(f"  [TIMEOUT] {name}")

    print("\n--- STEP 2: HEALTH STATUS ---")
    for name, cmd_bytes in HEALTH_CHECKS.items():
        client.sendall(cmd_bytes)
        time.sleep(0.1)
        try:
            reply = client.recv(1024)
            print(f"  {name}: {parse_health_reply(name, reply)}")
        except OSError:
            print(f"  {name}: Failed to read")

    print("\nInitialization complete. Hardware is ready.\n")
    flush_tcp_input(client)


def run_poll_loop(client):
    if USE_FAST_SWITCH_8A:
        cmd, inv_bytes = 0x8A, CMD_FAST_SWITCH_INVENTORY
        title = "fast switch inventory (0x8A)"
    elif INV_POLL_MODE in ("89", "realtime", "rt"):
        cmd, inv_bytes = 0x89, CMD_REALTIME_INVENTORY
        title = "real-time inventory (0x89)"
    else:
        cmd, inv_bytes = 0x8B, CMD_SESSION_INVENTORY
        title = "session inventory (0x8B S1/A)"
    print(f"--- STEP 3: CONTINUOUS {title} — Ctrl+C to stop ---")
    print(f"  TX: {inv_bytes.hex(' ').upper()}")
    print(
        "  Tip: env DEBUG_FRAMES=1 for unknown frames; INV_POLL_MODE=89 for 0x89 only.\n"
    )
    framer = A0Framer()
    try:
        while True:
            client.sendall(inv_bytes)
            deadline = time.monotonic() + INV_ROUND_TIMEOUT_SEC
            _, finished = drain_inventory_round(
                client, framer, deadline, cmd, verbose_done=False
            )
            if not finished:
                print(
                    f"  [warn] round timeout (no completion). "
                    f"Try DEBUG_FRAMES=1 or match RF profile/region to tag."
                )
            time.sleep(POLL_PAUSE_SEC)
    except KeyboardInterrupt:
        print("\nPoll stopped.")


def main():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.settimeout(2.5)
    try:
        print(f"Connecting to {READER_IP}...")
        client.connect((READER_IP, READER_PORT))
        print("Connection established.\n")

        run_configuration_and_health(client)
        run_poll_loop(client)

    except Exception as e:
        print(f"Connection Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
