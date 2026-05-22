# royalmnl-timing-pi

## Mandatory Pre-Work Rule

**Before any planning or implementation — no exceptions — read the context file:**

- `timing-node-context.mdc` (this repo root)

This file defines the system model, terminology, invariants, and guardrails. Terminology mistakes (e.g. calling checkpoints "timing mats") happen when this step is skipped. Read it even if the task seems small.

## How to Use This File

**Read this file first before exploring the codebase.** It is the authoritative fast-path for context. Cross-check the actual source only when this file is ambiguous or the task requires it. This saves redundant file reads on every session.

**After every significant implementation, update this file in the same commit.** "Significant" means: new module, new config/env var, new protocol behavior, new persistence/sync behavior, or any change that would surprise a future session without reading the diff. Update the relevant section in place — do not append a changelog. The goal is that this file always reflects current reality.

---

RFID timing node for RoyalMNL races. Runs on a Raspberry Pi at each checkpoint (start/finish/splits). Reads RFID tags from a connected reader, persists locally to SQLite, and syncs to the timing server. All race logic is server-side — this node only captures and pushes raw reads.

## Stack

- Python 3 (no third-party dependencies beyond stdlib)
- SQLite with WAL mode for local durability
- systemd autostart on Pi (headless operation)

## Running

```bash
# Requires env vars: TIMING_NODE_ID, TIMING_API_BASE_URL, TIMING_API_KEY
python main.py

# Optionally from /etc/royalmnl-timing-node.env file (non-overwriting)

# Manual reader test (diagnostic only, not production)
python test_reader.py
```

## Architecture

Three independent daemon threads, never blocking each other:

| Thread | File | State |
|---|---|---|
| Reader | `reader_loop.py` | `ReaderState`: DISCONNECTED → CONNECTING → CONFIGURING → CAPTURING → RECONNECTING |
| Network | `network_loop.py` | `NetworkState`: OFFLINE → PROBING → ONLINE → DEGRADED |
| Sync | `sync_loop.py` | Flushes outbox rows to backend via HTTP POST |

Shared state: `node_state.py` (`NodeState` dataclass, thread-safe getters/setters).

## Boot Order (must not regress)

1. Load config (`config.py`) and validate required env vars
2. Acquire single-instance lock (`TIMING_LOCK_FILE`, default `/var/run/timing-node.lock`)
3. Connect to SQLite and init schema (`db.py`) with WAL mode — **must complete before reader thread starts**
4. Run clock trust check (`clock_check.py`) — never blocks capture
5. Start reader + network + sync threads

## Ingest Write Order (strict)

Per tag read inside `reader_loop.py`:
1. Normalize EPC: `epc = raw_epc.strip().upper()`
2. Apply local time-window dedup (`DEDUPE_WINDOW_SEC`, default 20s)
3. Persist to SQLite (`reads` + `outbox` tables)
4. Sync worker picks up from outbox — never POST directly from reader callback

## Key Invariants

- **`readAt` on retry:** Always reuse the original `readAt` from the SQLite row. Never regenerate. Server dedupe key includes the exact timestamp string.
- **Offline capture:** Reader captures regardless of network state. Sync catches up when online.
- **No race logic on the Pi:** EPC resolution, results, and ranking are all server-side.
- **Clock untrusted:** Flag persisted to outbox, read is still pushed — never silently dropped.
- **Assignment pending:** If checkpoint assignment not yet received from server, rows stored with `assignment_pending=1` and backfilled when assignment arrives.

## SQLite Tables

| Table | Purpose |
|---|---|
| `meta` | Key-value (e.g., node_id, schema_version) |
| `reads` | Canonical raw read log (one row per accepted EPC) |
| `outbox` | Sync queue with status: `queued` → `sent` / `dead_letter` |

## Outbox HTTP Response Handling

| Response | Action |
|---|---|
| 200 accepted | Mark `sent` |
| 200 duplicate | Mark `sent` (idempotent) |
| 401 INVALID_API_KEY | Pause sync, raise alert — do not dead-letter |
| 422 EVENT_NOT_INGESTIBLE | Loud operator alert — do not dead-letter |
| Other 4xx | Mark `dead_letter` |
| 429 / 5xx / network error | Exponential backoff + jitter, retry |

Dead-letter cap: 10,000 rows max; rows older than 24h auto-purged.

## Configuration Env Vars

| Var | Default | Purpose |
|---|---|---|
| `TIMING_NODE_ID` | required | Identifies this node to the backend |
| `TIMING_API_BASE_URL` | required | Backend base URL |
| `TIMING_API_KEY` | required | Shared API key (x-api-key header) |
| `READER_IP` | `192.168.1.200` | RFID reader IP |
| `READER_PORT` | `4000` | RFID reader TCP port |
| `TIMING_DB_PATH` | `~/.royalmnl-timing/outbox.db` | SQLite file path |
| `TIMING_LOCK_FILE` | `/var/run/timing-node.lock` | Single-instance lock |
| `DEDUPE_WINDOW_SEC` | `20` | Local EPC dedup window; `0` disables |
| `ASSIGNMENT_POLL_SEC` | `5` | Poll interval when assignment pending |
| `ASSIGNMENT_POLL_STABLE_SEC` | `20` | Poll interval when assignment valid |
| `TIMING_LOG_LEVEL` | `INFO` | Log verbosity |
| `INV_POLL_MODE` | — | Inventory mode override |
| `INV_ROUND_TIMEOUT_SEC` | — | Per-round reader timeout |
| `POLL_PAUSE_SEC` | — | Sleep between inventory rounds |
| `DEBUG_FRAMES` | `0` | Set `1` to log raw A0 protocol frames |

## Protocol (A0 RFID)

`reader_protocol.py` handles all binary framing. Key classes/functions:
- `A0Framer` — stateful TCP framer; validates header, length, checksum; buffers partial packets
- `drain_inventory_round()` — collects all replies per round; calls `on_tag` callback per tag
- `run_configuration_and_health()` — sends setup commands, runs health checks
- `select_inventory_mode()` — probes 8-ant, 4-ant, falls back to 0x8B/0x89

Never modify checksum logic, startup sequence, or frame parsing without cross-checking against `test_reader.py` baseline.

## What Is Not Yet Implemented

- GPIO (LED/buzzer) physical feedback — code has hooks but hardware layer not wired
- Full graceful SQLite drain on SIGTERM (shutdown flag exists; drain loop TBD)
- Dead-letter cap auto-purge job

## Guardrails

- Do not add race ranking, category, or result logic here. Backend only.
- Do not remove offline-first behavior or local durability guarantees.
- Do not block local capture for any reason — offline capture is always higher priority than sync.
- Keep error handling and logs explicit; operators diagnose from field hardware with no SSH access.
