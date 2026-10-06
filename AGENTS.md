# AGENTS.md — Creek

> Instructions and shared context for AI coding agents (and humans) working in this repository.
> Read this file completely before making changes. Update it whenever you make a decision that
> future work must respect. See also `PROTOCOL.md` for the wire specification.
>
> **Status: early implementation.** The design targets **async Python (asyncio)**. `AGENTS.md` and
> `PROTOCOL.md` describe the intended architecture; the M0 skeleton (`src/creek/`, `tests/`) exists
> and runs both transports. The roadmap (§17) is the authoritative record of what is built.

## 1. What Creek is

Creek is a Redis-like, in-memory key–value store in **async Python** whose primary workload is
**typed publish/subscribe**. The design keeps its working set in RAM and persists to disk for
durability.

Core traits:

- **In-memory** hot path, **on-disk** durability (write-ahead log + snapshots).
- **Primitive, typed values** — fixed-width integers, floats, strings, and booleans (see §6).
- **Auth exists to identify the client**, so the correct namespace is applied. It is not a general
  multi-tenant security boundary in v1 (see §8).
- **WebSocket is the native transport**; both WebSocket and plain **TCP** speak the same
  **RESP2** subset (WebSocket is framing only, one command per binary frame).
- **Single asyncio event loop** owns all mutable state; blocking work is offloaded (see §11).
- **TLS is out of scope** — terminate it at a reverse proxy in front of the TCP listener.
- **Redis-like command surface** (see §7 and `PROTOCOL.md`).

Creek is *not* (v1 non-goals): a Redis drop-in replacement, a general document store, a SQL
database, a distributed/clustered system, or a full RBAC platform. Avoid scope creep in these
directions unless this file is updated first.

## 2. How to use this file (agents)

- Treat this as the source of truth for architecture and conventions. If code and this file
  disagree, fix the code or update this file — do not silently diverge.
- Before finishing a task: run `uv run ruff format --check .`, `uv run ruff check .`,
  `uv run mypy src`, and `uv run pytest -q`. All four must pass.
- Prefer the standard library. Any new third-party dependency must be justified in the PR/commit
  message and, if structural, recorded in §12.
- Keep public behavior changes documented here and in `PROTOCOL.md`.
- Anything marked **DECISION PENDING** in §14 is an open question: ask before assuming.

## 3. Quick start

```bash
# create the virtualenv and install the package + dev tools
uv sync

# run with defaults (WS + static :7100, TCP :7101), anonymous access
uv run creek

# run with persistence and a token file
uv run creek \
  --ws-addr :7100 \
  --tcp-addr :7101 \
  --web-dir ./web \
  --data-dir ./data \
  --auth-file ./auth.json \
  --fsync everysec \
  --snapshot-interval 5m

# tests
uv run pytest -q
uv run pytest -q -k tcp          # one transport

# lint / type-check
uv run ruff format --check .
uv run ruff check .
uv run mypy src

# browser load / performance tests
uv run creek                     # then open http://localhost:7100/
```

- Requires **Python 3.12+** (local Homebrew interpreter is 3.14). `uv` manages the interpreter and
  venv. If interpreter discovery breaks, pin it: `uv sync --python /opt/homebrew/bin/python3.14`.
- Entry point: console script `creek = creek.cli:main`; also `python -m creek`.
- Flags (all parsed in `src/creek/cli.py` via `argparse`): `--ws-addr`, `--tcp-addr`, `--web-dir`,
  `--data-dir`, `--auth-file`, `--fsync` (`always|everysec|no`), `--snapshot-interval`.

## 4. Architecture at a glance

```
        WebSocket (RESP frames)     TCP (RESP stream)
                   │                     │
                   └──────────┬──────────┘
                              ▼
                    ┌───────────────────────────────┐
                    │        Transport layer        │  creek.server
                    │  accept, frame, decode/encode │
                    └───────────────┬───────────────┘
                                    ▼
                    ┌───────────────────────────────┐
                    │      Protocol / command       │  creek.protocol, creek.commands
                    │  parse → typed Command → exec │
                    └───────┬───────────────┬───────┘
                            ▼               ▼
              ┌──────────────────┐   ┌──────────────────┐
              │   Store (KV)     │   │   Pub/Sub bus    │
              │   keys + TTL     │   │ subs + fanout    │
              └────────┬─────────┘   └────────┬─────────┘
                       └──────────┬───────────┘
                                  ▼
                    ┌───────────────────────────────┐
                    │   Persistence (WAL+snapshot)  │  creek.persist
                    └───────────────────────────────┘
```

| Component | Module | Responsibility |
|---|---|---|
| Transports | `creek.server` | asyncio TCP/WS listeners, framing, static files |
| Protocol | `creek.protocol` | RESP2 codec: pure `parse` / `encode` functions |
| Commands | `creek.commands` | Registry, dispatch, ACLs, built-in handlers |
| Value model | `creek.value` | (M1) Primitive types, codecs, arithmetic |
| Patterns | `creek.pattern` | (M1) Glob matching |
| Store | `creek.store` | (M1) Single-dict in-memory KV, TTL, namespace ownership |
| Pub/Sub | `creek.pubsub` | (M2) Subscription registry, pattern matching, fanout, retained |
| Auth | `creek.auth` | Client identification → identity + namespace |
| Persistence | `creek.persist` | (M3) WAL, snapshots, recovery |
| CLI | `creek.cli` | Flags, wiring, lifecycle, signals, snapshots |
| Browser tests | `web/` | Load and performance pages (`bench.js`) |

## 5. Repository layout (target)

```
creek/
├── AGENTS.md
├── PROTOCOL.md
├── pyproject.toml
├── uv.lock
├── src/creek/
│   ├── __init__.py __main__.py
│   ├── cli.py          flags, Config, lifecycle
│   ├── server.py       TCP + WebSocket listeners, static files
│   ├── protocol.py     RESP2 codec (pure parse/encode)
│   ├── commands.py     registry, dispatch, built-in commands
│   ├── auth.py         token -> Identity
│   ├── errors.py       ProtocolError, CommandError
│   ├── value.py        (M1) typed values
│   ├── store.py        (M1) keys + TTL
│   ├── pattern.py      (M1) glob matching
│   ├── pubsub.py       (M2) subscriptions
│   └── persist.py      (M3) WAL + snapshots
├── tests/          test_protocol.py test_commands.py test_auth.py
│                   test_cli.py test_server.py test_value.py ...
└── web/            index.html loadtest.html perf.html bench.js
```

A `creek-client` Python client package is planned but not yet implemented (see §14).

## 6. Value model

Every value is a tagged primitive. The type is part of the value, not inferred at read time.

| Tag | Meaning | Width | Python storage / range |
|---|---|---|---|
| `i4`  | signed int   | 4 bits  | `int`, −8 … 7 (masked byte) |
| `u4`  | unsigned int | 4 bits  | `int`, 0 … 15 (masked byte) |
| `i8` / `u8`   | signed/unsigned int | 8 | `int`, −128 … 127 / 0 … 255 |
| `i16` / `u16` | signed/unsigned int | 16 | `int` |
| `i32` / `u32` | signed/unsigned int | 32 | `int` |
| `f8`  | float | 8 | OCP E4M3 (only variant; NaN at `0x7F`, max finite 448) |
| `f16` | float | 16 | IEEE 754 binary16 (`struct` `"e"`) |
| `f32` | float | 32 | `struct` `"f"` |
| `f64` | float | 64 | `struct` `"d"` |
| `str` | UTF-8 string | var | `str`, length-prefixed, max 512 KiB |
| `bool`| boolean | 1 bit | `bool`, encoded as `u8` 0/1 on the wire |

Encoding rules:

- Wire/storage format is **little-endian** for fixed-width numerics (`struct` prefix `<`).
- Canonical binary layout: `1 type byte || payload` (see `PROTOCOL.md` §5.1).
  `Value.marshal_binary()` / `Value.unmarshal_binary()` implement it; persistence embeds the blob.
- Python `int` is unbounded, so **every** integer type is range-checked on write and after
  arithmetic. `i4`/`u4` are logically 4-bit but stored as a masked byte in v1 (see §14).
- `f16` uses `struct`'s `"e"`; `f8` (E4M3) is implemented in `creek/value/codec.py` (no dependency).
- Values are immutable once stored (`frozen=True` dataclass); mutations replace the value. Every
  entry carries an **owner** (namespace) so a shared store can scope keys per client.

## 7. Command surface (Redis-like)

Case-insensitive command names, whitespace-separated args. Typed values are given explicitly with
`TYPE`; if omitted, the server defaults to `str`. The full grammar and examples are in
`PROTOCOL.md`.

### Connection / auth
| Command | Notes |
|---|---|
| `AUTH <token>` | Identify client; selects namespace. |
| `PING [msg]` / `ECHO msg` | Health check / echo. |
| `QUIT` | Close connection. |

### Key–value
| Command | Notes |
|---|---|
| `SET key value [TYPE t] [EX s\|PX ms] [NX\|XX]` | Upsert. |
| `GET key` / `MGET key…` | Returns `[type, value]`. |
| `DEL key…` / `EXISTS key…` | Delete / test. |
| `TYPE key` | Returns type tag or `none`. |
| `EXPIRE key seconds` / `TTL key` | TTL management. |
| `KEYS pattern` / `SCAN cursor [MATCH p] [COUNT n]` | Enumeration. |
| `INCR key [BY n]` / `DECR key [BY n]` | Integer types only; preserves type; range-checked. |
| `DBSIZE` | Live key count. |

### Pub/Sub
| Command | Notes |
|---|---|
| `PUBLISH channel value [TYPE t] [RETAIN]` | Publish; `RETAIN` persists last value. |
| `SUBSCRIBE channel…` / `PSUBSCRIBE pattern…` | Subscribe (literal / glob). |
| `UNSUBSCRIBE [channel…]` / `PUNSUBSCRIBE [pattern…]` | Unsubscribe. |
| `PUBSUB CHANNELS\|NUMSUB\|NUMPAT [arg]` | Introspection. |

### Admin
| Command | Notes |
|---|---|
| `INFO [section]`, `FLUSHALL`, `COMMAND [COUNT]` | Operational. |

Notes:
- The command registry is data-driven: write an `async def` handler and call
  `register(name, handler, min_args=…, max_args=…, scope=…)` in `creek.commands`.
  Add commands there, never with ad-hoc string matching in the transport layer.
- New commands must be reflected in this table and in `PROTOCOL.md`.

## 8. Auth model

Auth exists to **recognize the client and select its namespace**, not to be a full security
boundary in v1.

- `AUTH <token>` resolves a token to `Identity(id, namespace, acl)`.
- Token store: a JSON file (`--auth-file`, see `creek.auth`), loaded with `auth.load_file`.
  Tokens are compared with `hmac.compare_digest` (constant time).
- When `--auth-file` is omitted, anonymous access is allowed with a full ACL.
- ACLs are coarse: `read`, `write`, `subscribe`, `admin` (or `all`/`*`).
- Every command handler receives the resolved identity; the store owner is derived from it. Never
  trust a client-supplied namespace.

Auth file schema:

```json
{
  "allow_anonymous": false,
  "clients": [
    {"id": "sensor-a", "namespace": "sensors", "token": "t1", "acl": ["read", "subscribe"]},
    {"id": "admin", "token": "t2", "acl": ["all"]}
  ]
}
```

## 9. Transports and framing

- **WebSocket (native):** framing only. Each binary frame carries exactly one RESP2
  command; the reply is one binary frame. Streamed pub/sub messages are the same 4-element
  RESP arrays as TCP. Served by the `websockets` library; HTTP requests to `/` are answered
  from `--web-dir` via the library's `process_request` hook.
- **TCP:** the same RESP2 subset (arrays of bulk strings or inline commands) as a byte
  stream; redis-cli compatible for scalar commands. Served by `asyncio.start_server`.
- **TLS:** intentionally **out of scope**. Run a reverse proxy in front of the TCP listener.
- All transports funnel into the same `creek.protocol.Command` model; the transport only
  handles framing, never command semantics.
- Backpressure: pub/sub delivery is non-blocking; a subscriber whose bounded queue is full or whose
  `send` fails is dropped rather than blocking the bus.
- WebSocket read limit: 1 MiB; static files are served from `--web-dir` at `/`.

## 10. Persistence

- **WAL (write-ahead log):** every mutation (SET/DEL/EXPIRE/FLUSH and `PUBLISH … RETAIN`) is
  appended as a `u32 length || u32 CRC32 || payload` record (`creek/persist/wal.py`). The payload
  is UTF-8 JSON describing the mutation, with typed values carried as base64 of the canonical
  binary blob. `--fsync=always|everysec|no` (default `everysec`).
- **Snapshot:** periodic full dump (versioned JSON header + entries, values base64) written
  atomically via temp file → `fsync` → rename, then directory `fsync`. Triggered by
  `--snapshot-interval` and once on shutdown.
- **Recovery:** `Engine.recover` loads the snapshot then replays the WAL. Expired entries
  (absolute deadline in the past) are dropped, not resurrected.
- Replay of the full WAL after a snapshot is idempotent; the WAL is **not yet compacted/rotated**
  after a snapshot (see §14).
- Blocking disk work (write + `os.fsync`) runs off the event loop; see §11.
- Non-retained pub/sub messages are not persisted.
- On-disk formats are versioned (`persist.SNAPSHOT_VERSION`, value type byte). Bump the version and
  provide a migration/reader for old files.

## 11. Concurrency

- **One asyncio event loop owns all state.** The store is a plain `dict` and the pub/sub registry a
  plain collection — no locks, because the loop serializes access.
- **Rule: never `await` inside a logical critical section.** A command handler mutates state and
  computes its reply synchronously, then awaits I/O. Violating this lets another task observe a
  half-applied mutation.
- Blocking work is offloaded: the WAL/snapshot writer is a dedicated task consuming an
  `asyncio.Queue`; `os.fsync` and snapshot serialization run via `asyncio.to_thread`.
- Pub/Sub fanout is a linear scan over a snapshot of subscribers. Delivery uses
  `Queue.put_nowait`; on `QueueFull` (or a closed writer) the subscriber is pruned, never blocked on.
- Per-connection outbound writes are serialized (a per-connection `asyncio.Lock`, or a single
  writer task) because interleaving writes corrupt framing.
- No threads except short-lived `to_thread` workers for blocking I/O; they must not touch store
  state directly.
- Run everything under `pytest-asyncio`; a flaky/racy test is a release blocker.

## 12. Dependencies

Prefer the standard library. Current external dependencies:

| Concern | Package | Status |
|---|---|---|
| WebSocket transport | `websockets` | required |
| Testing | `pytest`, `pytest-asyncio` | dev |
| Lint/format | `ruff` | dev |
| Type checking | `mypy` | dev |

`f16`/`f8` are implemented in-tree (`struct` + custom E4M3). Do not add an ORM, a web framework, or
a logging framework; stdlib `logging` is the logging standard. `aiohttp`/`uvicorn`/FastAPI are out
of scope: Creek speaks its own protocol, not HTTP APIs.

## 13. Testing and quality (planned)

- Table-driven (parametrized) unit tests for `value`, `pattern`, `protocol`, `store`, and `pubsub`.
- Codec golden/round-trip tests in `tests/test_protocol.py` and `tests/test_value.py`.
- Command-level tests in `tests/test_commands.py` (ACLs, typed values, TTL, pub/sub, recovery).
- Integration tests in `tests/test_server.py` start WS + TCP listeners on ephemeral ports.
- Persistence tests: WAL round-trip, truncated-tail tolerance, reset, snapshot round-trip.
- **Browser load and performance tests** live in `web/` and are served by the WS listener:
  - `web/index.html` — landing page.
  - `web/loadtest.html` — many concurrent WS clients, live ops/s chart, latency percentiles.
  - `web/perf.html` — per-command latency/throughput (SET/GET/PUBLISH), bar chart.
  - `web/bench.js` — dependency-free helpers (client, percentiles, canvas charts).
  These pages must stay dependency-free (no CDNs) so they work offline.
- Required checks before merge: `ruff format --check .` clean, `ruff check .`, `mypy src`, and
  `pytest -q` all green.

## 14. Open questions (DECISION PENDING)

1. **WebSocket library** — `websockets` chosen (asyncio-native, HTTP hooks for static files).
   Revisit if a stdlib-only server becomes a hard requirement.
2. **Nibble packing** for `i4`/`u4` — the design byte-masks them; pack two per byte?
3. **`f8` variants** — E4M3 is the only planned variant; add E5M2?
4. **WAL compaction/rotation** — snapshots do not truncate the WAL; add prefix truncation.
5. **Token hashing** — plaintext tokens at rest. Prefer stdlib `hashlib.scrypt` (no dependency) or
   Argon2id via `argon2-cffi`?
6. **Anonymous default** — allowed when no `--auth-file` is given.
7. **Python client library** (`creek-client`) — not yet implemented.
8. **Clustering** — explicitly out of scope for v1.
9. **Streaming parser re-parse** — the TCP reader re-parses its buffer from the start after each
   read (simplest correct approach). Add a cursor if very large commands become a real workload.

## 15. Coding conventions

- **Python 3.12+**. Format and lint with `ruff` (line length 100, double quotes). Type-annotate all
  public functions; `mypy` must pass on `src`.
- Use `from __future__ import annotations` for forward references in typing.
- Errors: a small exception hierarchy in `creek/errors.py` (`CreekError` → `CommandError`,
  `ProtocolError`). Never use bare `except`; never `sys.exit` in library code.
- Public modules, classes, and functions have docstrings. Command handlers are small and pure where
  possible.
- `asyncio` throughout: all I/O is `async`; never call blocking APIs on the loop (offload with
  `asyncio.to_thread`). Never `await` in a state-mutating critical section (§11).
- Logging via stdlib `logging` with structured fields (`logger.info("msg", extra={...})`); no
  `print` outside `creek/cli.py`.
- Keep modules cohesive and dependency-acyclic: `creek.commands` may import `creek.store`,
  `creek.pubsub`, `creek.persist`, `creek.protocol`, and `creek.value`, but those must not import
  `creek.commands` or `creek.server`. Prefer flat modules over packages; split only when a file
  outgrows a few hundred lines.
- No global mutable state outside the command registry built at import time.
- Data carriers are `@dataclass(frozen=True, slots=True)` (e.g. `Value`, `Command`, `Identity`).

## 16. Glossary

| Term | Meaning |
|---|---|
| **Creek** | This project: typed, in-memory KV + pub/sub. |
| **Client identity** | The authenticated principal that owns a namespace. |
| **Owner** | Namespace recorded on an entry so a shared store can scope keys per client. |
| **Retained message** | A published value stored as the channel's last value and persisted. |
| **WAL** | Write-ahead log used for crash recovery. |
| **Critical section** | A synchronous block that mutates shared state; must not contain `await`. |

## 17. Roadmap

- **M0 — Skeleton:** package, `pyproject.toml`, flags, asyncio TCP + WS listeners, command
  registry. *(implemented)*
- **M1 — Value + store:** all primitive types, TTL, simple dict store, tests.
- **M2 — Pub/Sub:** subscribe/publish, pattern matching, linear fanout, retained.
- **M3 — Persistence:** WAL + snapshot + recovery tests. (WAL compaction pending)
- **M4 — Auth:** token file, per-client namespaces, coarse ACLs. (token hashing pending)
- **M5 — Hardening:** browser load/perf pages; fuzz tests, Python client pending.

## 18. Decision log

Notable decisions future work must respect (newest last).

- **2026-10-06 — Async Python.** Creek pivoted from Go to asyncio Python. See §1–§15.
- **2026-10-06 — One RESP2 wire protocol.** WebSocket carries the same RESP2 bytes as TCP;
  WebSocket is framing only (one command per binary frame). There is no JSON codec. See §9 and
  `PROTOCOL.md` §3.
- **2026-10-06 — Flat module layout.** An earlier package-per-layer layout (`protocol/`,
  `command/`, `server/`) was collapsed into single modules (`protocol.py`, `commands.py`,
  `server.py`): the indirection cost more than it explained. The RESP parser is a pure function
  over bytes, shared by both transports. See §5 and §15; the TCP re-parse trade-off is §14.9.

---

*Keep this file current — it is the project's shared memory.*
