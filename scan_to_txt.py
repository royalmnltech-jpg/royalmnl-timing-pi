#!/usr/bin/env python3
"""Standalone RFID scan-to-file diagnostic: connect reader, capture unique EPCs to a txt file.

No backend, no SQLite, no sync — reader configuration only, reused from main.py/reader_loop.py.
"""

from __future__ import annotations

import logging
import os
import socket
import time
from pathlib import Path

from reader_protocol import (
    A0Framer,
    INV_ROUND_TIMEOUT_SEC,
    POLL_PAUSE_SEC,
    drain_inventory_round,
    run_configuration_and_health,
    select_inventory_mode,
)

READER_IP = os.environ.get("READER_IP", "192.168.1.200").strip()
READER_PORT = int(os.environ.get("READER_PORT", "4000"))
OUTPUT_PATH = Path(os.environ.get("SCAN_OUTPUT_PATH", "scanned_tags.txt")).expanduser()


def load_existing_tags(path: Path, log: logging.Logger) -> set[str]:
    if not path.is_file():
        return set()
    tags = {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}
    log.info("Loaded %d existing unique tag(s) from %s", len(tags), path)
    return tags


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    log = logging.getLogger("scan-to-txt")

    seen: set[str] = load_existing_tags(OUTPUT_PATH, log)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2.5)
    log.info("Connecting reader %s:%s", READER_IP, READER_PORT)
    sock.connect((READER_IP, READER_PORT))

    run_configuration_and_health(sock, log)
    cmd, inv_bytes, label = select_inventory_mode(sock, log)
    log.info("Reader capture mode: %s", label)

    framer = A0Framer()

    def on_tag(payload: dict) -> None:
        raw_epc = payload.get("epc", "")
        epc = raw_epc.strip().upper()
        if not epc or epc in seen:
            return
        seen.add(epc)
        with OUTPUT_PATH.open("a", encoding="utf-8") as f:
            f.write(epc + "\n")
            f.flush()
            os.fsync(f.fileno())
        log.info("New unique tag captured: %s (total=%d)", epc, len(seen))

    log.info("Scanning — writing unique tags to %s. Press Ctrl+C to stop.", OUTPUT_PATH)
    try:
        while True:
            sock.sendall(inv_bytes)
            deadline = time.monotonic() + INV_ROUND_TIMEOUT_SEC
            _, finished = drain_inventory_round(sock, framer, deadline, cmd, on_tag)
            if not finished:
                log.debug("Inventory round incomplete (timeout)")
            time.sleep(POLL_PAUSE_SEC)
    except KeyboardInterrupt:
        log.warning("Stopped by user")
    except OSError as e:
        log.error("Reader connection error: %s", e)
    finally:
        try:
            sock.close()
        except OSError:
            pass
        log.info("Session ended. %d unique tag(s) total in %s", len(seen), OUTPUT_PATH)


if __name__ == "__main__":
    main()
