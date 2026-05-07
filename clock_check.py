"""Clock trust heuristics for Raspberry Pi (no RTC battery)."""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from datetime import datetime, timezone


def _ntp_synced_via_timedatectl() -> bool | None:
    """Return True if systemd-timesyncd reports sync, False if not, None if unknown."""
    if not shutil.which("timedatectl"):
        return None
    try:
        out = subprocess.run(
            ["timedatectl", "show", "-p", "NTPSynchronized", "--value"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        line = (out.stdout or "").strip().lower()
        if line == "yes":
            return True
        if line == "no":
            return False
    except OSError:
        return None
    return None


_NTP_LINE = re.compile(r"^\s*ntp synchronized:\s*(yes|no)\s*$", re.I)


def _ntp_synced_via_timedatectl_status() -> bool | None:
    """Fallback: parse `timedatectl status` for NTP synchronized."""
    if not shutil.which("timedatectl"):
        return None
    try:
        out = subprocess.run(
            ["timedatectl", "status"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        for raw in (out.stdout or "").splitlines():
            m = _NTP_LINE.match(raw)
            if m:
                return m.group(1).lower() == "yes"
    except OSError:
        return None
    return None


def is_clock_trusted(log: logging.Logger) -> bool:
    """
    Trusted if wall clock is clearly past epoch drift AND (when available) NTP reports synced.

    Policy: never block capture; this only sets flags for raw payload and ops visibility.
    """
    now = datetime.now(timezone.utc)
    year_ok = now.year >= 2021

    ntp = _ntp_synced_via_timedatectl()
    if ntp is None:
        ntp = _ntp_synced_via_timedatectl_status()

    if not year_ok:
        log.warning(
            "Clock trust: system year=%s — treating as UNTRUSTED (possible 1970 / no NTP)",
            now.year,
        )
        return False

    if ntp is False:
        log.warning("Clock trust: NTP not synchronized (timedatectl) — UNTRUSTED")
        return False

    if ntp is True:
        log.info("Clock trust: OK (year OK + NTP synchronized)")
        return True

    log.info("Clock trust: year OK; NTP status unknown — TRUSTED by year heuristic")
    return True
