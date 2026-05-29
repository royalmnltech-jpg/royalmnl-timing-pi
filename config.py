"""Load timing-node configuration from environment and optional env file."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


DEFAULT_ENV_PATH = Path("/etc/royalmnl-timing-node.env")
# Repo-local .env for local development (takes priority over system env file)
DEV_ENV_PATH = Path(__file__).parent / ".env"


def _parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except PermissionError:
        print(f"[config] WARNING: cannot read {path} (permission denied) — skipping", flush=True)
        return out
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key:
            out[key] = val
    return out


def load_env_overlays() -> None:
    """Merge file env into os.environ without overwriting existing vars.

    Load order (first file wins per key):
      1. .env in repo root  — local dev on Windows/Mac
      2. /etc/royalmnl-timing-node.env — production Pi
    """
    for path in (DEV_ENV_PATH, DEFAULT_ENV_PATH):
        for k, v in _parse_env_file(path).items():
            os.environ.setdefault(k, v)


@dataclass(frozen=True)
class NodeConfig:
    timing_node_id: str
    timing_api_base_url: str
    timing_api_key: str
    reader_ip: str
    reader_port: int
    dedupe_window_sec: float
    assignment_poll_sec: float
    assignment_poll_stable_sec: float
    timing_db_path: str
    lock_file_path: str
    log_level: str
    sync_batch_size: int
    sync_interval_sec: float
    sync_max_retries: int


def load_config() -> NodeConfig:
    """Load config after calling load_env_overlays() if desired."""
    timing_node_id = os.environ.get("TIMING_NODE_ID", "").strip()
    base = os.environ.get("TIMING_API_BASE_URL", "").strip().rstrip("/")
    key = os.environ.get("TIMING_API_KEY", "").strip()

    reader_ip = os.environ.get("READER_IP", "192.168.1.200").strip()
    reader_port = int(os.environ.get("READER_PORT", "4000"))

    dedupe = float(os.environ.get("DEDUPE_WINDOW_SEC", "20"))
    poll_setup = float(os.environ.get("ASSIGNMENT_POLL_SEC", "5"))
    poll_stable = float(os.environ.get("ASSIGNMENT_POLL_STABLE_SEC", "20"))

    db_default = str(Path.home() / ".royalmnl-timing" / "outbox.db")
    timing_db_path = os.environ.get("TIMING_DB_PATH", db_default).strip()

    lock_file = os.environ.get("TIMING_LOCK_FILE", "/var/run/timing-node.lock").strip()
    log_level = os.environ.get("TIMING_LOG_LEVEL", "INFO").strip().upper()

    sync_batch_size = int(os.environ.get("SYNC_BATCH_SIZE", "50"))
    sync_interval_sec = float(os.environ.get("SYNC_INTERVAL_SEC", "1.5"))
    sync_max_retries = int(os.environ.get("SYNC_MAX_RETRIES", "5"))

    return NodeConfig(
        timing_node_id=timing_node_id,
        timing_api_base_url=base,
        timing_api_key=key,
        reader_ip=reader_ip,
        reader_port=reader_port,
        dedupe_window_sec=dedupe,
        assignment_poll_sec=poll_setup,
        assignment_poll_stable_sec=poll_stable,
        timing_db_path=timing_db_path,
        lock_file_path=lock_file,
        log_level=log_level,
        sync_batch_size=sync_batch_size,
        sync_interval_sec=sync_interval_sec,
        sync_max_retries=sync_max_retries,
    )


def validate_required(cfg: NodeConfig) -> Optional[str]:
    """Return error message if required fields missing."""
    if not cfg.timing_node_id:
        return "TIMING_NODE_ID is required"
    if not cfg.timing_api_base_url:
        return "TIMING_API_BASE_URL is required"
    if not cfg.timing_api_key:
        return "TIMING_API_KEY is required"
    return None
