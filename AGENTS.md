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
- **Auth identifies the client and selects its namespace.** Namespaces scope data today; turning
  them into hard, isolated tenants is a future milestone (see §8 and §17 M6).
- **WebSocket is the native transport**; both WebSocket and plain **TCP** speak the same
  **RESP2** subset (WebSocket is framing only, one command per binary frame).
- **Single asyncio event loop** owns all mutable state; blocking work is offloaded (see §11).
- **TLS is out of scope** — terminate it at a reverse proxy in front of the TCP listener.
- **Redis-like command surface** (see §7 and `PROTOCOL.md`).

Creek is *not* (v1 non-goals): a Redis drop-in replacement, a general document store, a SQL
database, or a full RBAC platform. **v1 is single-node** and namespaces are a scoping mechanism,
not a hard tenant boundary: multitenancy (**M6**) and clustering (**M7**) are planned future
milestones (§17), not v1 features. Avoid scope creep in these directions unless this file is
updated first.

## 2. How to use this file (agents)

- Treat this as the source of truth for architecture and conventions. If code and this file
  disagree, fix the code or update this file — do not silently diverge.
- Before finishing a task: run `ruff format --check .`, `ruff check .`, `mypy src`, and
  `pytest -q` (prefix with `uv run` when not in an activated venv). All four must pass.
- Prefer the standard library. Any new third-party dependency must be justified in the PR/commit
  message and, if structural, recorded in §12.
- Keep public behavior changes documented here and in `PROTOCOL.md`.
- Anything marked **DECISION PENDING** in §14 is an open question: ask before assuming.

## 3. Quick start

Requires **Python 3.12+**. Use either **uv** (fast, locks the interpreter) or plain **pip**.

### With uv

```bash
uv sync                          # creates .venv, installs deps + dev group
uv run creek                     # WS + static :7100, TCP :7101
uv run pytest -q
uv run pytest -q -k tcp          # one transport
uv run ruff format --check .
uv run ruff check .
uv run mypy src
```

### With pip

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"         # [dev] adds pytest, ruff, mypy
creek
pytest -q
ruff format --check .
ruff check .
mypy src
```

With persistence and a token file (drop the `uv run` prefix under pip):

```bash
uv run creek \
  --ws-addr :7100 \
  --tcp-addr :7101 \
  --web-dir ./web \
  --data-dir ./data \
  --auth-file ./auth.json \
  --fsync everysec \
  --snapshot-interval 5m
```

- Dev dependencies are declared in **both** `[project.optional-dependencies].dev` (pip) and
  `[dependency-groups].dev` (uv); `tests/test_packaging.py` fails if they drift.
- On this machine the system interpreter is old and `/usr/local/bin/python` is broken. If uv
  discovery fails, pin it: `uv sync --python /opt/homebrew/bin/python3.14`.
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
| Pub/Sub | `creek.pubsub` | Subscription registry, pattern matching, fanout, retained |
| Auth | `creek.auth` | Client identification → identity + namespace |
| Persistence | `creek.persist` | (M3) WAL, snapshots, recovery |
| CLI | `creek.cli` | Flags, wiring, lifecycle, signals, snapshots |
| Client library | `creek.client` | Async client over WebSocket/TCP |
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
│   ├── value.py        typed values (M1a)
│   ├── store.py        keys + TTL (M1b)
│   ├── pattern.py      glob matching (M1b)
│   ├── pubsub.py       subscriptions + fanout (M2)
│   ├── persist.py      WAL + snapshots (M3)
│   ├── tenant.py       tenant registry (M6a)
│   └── client.py       async client library (M5)
├── tests/          test_protocol.py test_commands.py test_auth.py
│                   test_cli.py test_server.py test_value.py
│                   test_store.py test_pubsub.py test_persist.py
│                   test_client.py test_fuzz.py test_packaging.py ...
├── scripts/        bench.py (load/benchmark harness), hash_token.py
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
  `Value.to_bytes()` / `Value.from_bytes()` implement it; persistence embeds the blob.
- Python `int` is unbounded, so **every** integer type is range-checked on write and after
  arithmetic. `i4`/`u4` are logically 4-bit but stored as a masked byte in v1 (see §14).
- `f16` uses `struct`'s `"e"`; `f8` (E4M3) is implemented in `creek/value.py` (no dependency).
  f16/f32 overflow rounds to ±inf; f8 saturates to ±448 (no infinity in E4M3).
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
| `INFO [section]`, `FLUSHALL`, `COMMAND [COUNT]`, `TENANT …` | Operational. |

Notes:
- The command registry is data-driven: write an `async def` handler and call
  `register(name, handler, min_args=…, max_args=…, scope=…)` in `creek.commands`.
  Add commands there, never with ad-hoc string matching in the transport layer.
- New commands must be reflected in this table and in `PROTOCOL.md`.

## 8. Auth model

Auth exists to **recognize the client and select its namespace**, not to be a full security
boundary in v1.

- `AUTH <token>` resolves a token to `Identity(id, namespace, acl)`.
- Token store: a JSON file (`--auth-file`, see `creek.auth`), loaded with `Authenticator.from_file`.
  Tokens are compared with `hmac.compare_digest` (constant time). A token may be stored plaintext
  (development) or hashed with scrypt; a hashed token is a string starting with `scrypt$`
  (`hash_token()` / `scripts/hash_token.py` generate them). Each `authenticate` tries every stored
  verifier, so the work is constant-time across clients.
- When `--auth-file` is omitted, anonymous access is allowed with a full ACL.
- ACLs are coarse: `read`, `write`, `subscribe`, `admin` (or `all`/`*`).
- Every command handler receives the resolved identity; the store owner is derived from it. Never
  trust a client-supplied namespace.
- `AUTH` also moves the connection's pub/sub subscriber to the new namespace and drops any
  subscriptions registered under the previous one (a namespace switch must not leave stale
  subscriptions behind).
- Tenants carry optional quotas (M6b): keys, approximate memory, connections, subscriptions, and
  command rate. Exceeding one returns a `QUOTA` error. Memory is approximated as
  `len(key) + len(value blob)` per entry plus retained values.

Auth file schema:

```json
{
  "allow_anonymous": false,
  "clients": [
    {"id": "sensor-a", "namespace": "sensors", "token": "t1", "acl": ["read", "subscribe"]},
    {"id": "admin", "token": "scrypt$16384$8$1$<salt>$<hash>", "acl": ["all"]}
  ]
}
```

### Path to multitenancy

Today a "namespace" is a scoping prefix on keys and channels, not a hard isolation boundary: one
process, one store, one WAL, coarse ACLs. The data model is tenant-ready — every entry carries its
owner and every WAL record is tagged — and **M6a** (§17) now makes tenants explicit
(`TENANT …`, auto-provisioned on `AUTH`, journaled and snapshotted), while **M6b** adds per-tenant
quotas. Per-tenant observability/persistence are **M6c**.

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

Dev dependencies are listed twice — `[project.optional-dependencies].dev` for pip and
`[dependency-groups].dev` for uv — because pip cannot read PEP 735 groups. `uv.lock` is committed.
`tests/test_packaging.py` asserts the two lists are identical, so update both when adding a tool.

## 13. Testing and quality (planned)

- Table-driven (parametrized) unit tests for `value`, `pattern`, `protocol`, `store`, and `pubsub`.
- Codec golden/round-trip tests in `tests/test_protocol.py` and `tests/test_value.py`.
- Fuzz-style tests in `tests/test_fuzz.py`: seeded random input to `parse`, `Value.from_bytes`,
  `decode_records`, and the client reply parser, asserting only expected exceptions escape.
- Command-level tests in `tests/test_commands.py` (ACLs, typed values, TTL, pub/sub, recovery).
- Integration tests in `tests/test_server.py` start WS + TCP listeners on ephemeral ports.
- Persistence tests: WAL round-trip, truncated-tail tolerance, reset, snapshot round-trip.
- **Developer benchmark:** `scripts/bench.py` starts a server in a child process and drives it with
  concurrent clients (`--transport ws|tcp`, `--command ping|set|get|publish`), printing throughput
  and p50/p95/p99/max latency. It is a regression baseline, not a promise.
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
4. **WAL compaction/rotation** — *resolved*: after a snapshot the WAL is truncated when no write
   raced the snapshot (`Wal.compact`); otherwise it is left intact. Under sustained writes the log
   grows until a quiet snapshot.
5. **Token hashing** — *resolved*: stdlib `hashlib.scrypt`, plaintext still accepted for dev.
   Revisit Argon2id only if scrypt proves insufficient.
6. **Anonymous default** — allowed when no `--auth-file` is given.
7. **Python client library** — *resolved*: `creek.client` (async, both transports). A separate
   `creek-client` distribution is still optional.
8. **Clustering** — v1 is single-node. Planned as **M7** (§17) in phases; treat as exploratory and
   do not let it shape v1 code.
9. **Streaming parser re-parse** — the TCP reader re-parses its buffer from the start after each
   read (simplest correct approach). Add a cursor if very large commands become a real workload.
10. **SCAN cursor** — an index into the sorted key list, not a reverse-binary cursor like Redis;
    iteration is not stable across concurrent writes. Fine for v1; revisit if it matters.
11. **Pattern deliveries** — a delivered frame is always `[message, channel, vtype, value]`
    (`PROTOCOL.md` §4.4); a pattern subscriber is not told *which* pattern matched. Add a
    `pmessage` form if that's needed.
12. **Pub/sub channel scoping** — channels are scoped by namespace (owner), like keys. Decide
    whether cross-namespace channels are ever wanted.
13. **Multitenancy isolation** — namespaces share one process, store, and WAL. **M6** (§17) defines
    quotas, lifecycle, and optional per-tenant persistence. Decide whether a tenant can be exported
    or moved between nodes (this interacts with M7b sharding).

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
- **M1a — Value model:** all primitive types, text parsing, binary codec, tests. *(implemented)*
- **M1b — Store:** TTL, simple dict store, and the KV commands (`SET`/`GET`/`DEL`/`TTL`/…).
  *(implemented)*
- **M2 — Pub/Sub:** subscribe/publish, pattern matching, linear fanout, retained.
  *(implemented)*
- **M3 — Persistence:** WAL + snapshot + recovery tests. *(implemented; WAL compaction added in M4)*
- **M4 — Auth:** token file, per-client namespaces, coarse ACLs, scrypt token hashing.
  *(implemented)*
- **M5 — Hardening:** browser load/perf pages; fuzz tests; async client library. *(implemented)*
- **M6 — Multitenancy:** promote namespaces to first-class tenants with lifecycle and limits.
  - **M6a — Registry + admin API:** `TENANT CREATE|LIST|DROP|INFO` (admin scope), auto-provisioned
    when a client authenticates, journaled and snapshotted. *(implemented)*
  - **M6b — Quotas:** keys, approximate memory, connections, subscriptions, and command rate per
    tenant; quota failures return a `QUOTA` error. *(implemented)*
  - **M6c — Observability + hardening:** per-tenant counters (`INFO tenant`), key/channel length
    limits, no cross-tenant enumeration, audit logging of admin actions. Requires `--auth-file`.
- **M7 — Clustering (multi-phase; exploratory):** v1 is single-node and nothing in it assumes a
  cluster.
  - **M7a — Read replicas:** one writer, N followers streaming the WAL; `ROLE` / `INFO replication`;
    clients may read from any node.
  - **M7b — Sharding:** hash-slot partitioning (e.g. 16384 slots), `CLUSTER` commands, and
    `MOVED`/`ASK` redirects; a cluster-aware client or a stateless proxy.
  - **M7c — Failover:** membership and failure detection, leader election (Raft-style), automatic
    promotion, and cluster-wide pub/sub routing (the hardest part).

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
- **2026-10-06 — uv and pip both supported.** uv is the agent-default (locks the interpreter,
  `uv.lock`), but the project is pip-installable: `pip install -e ".[dev]"`. Dev deps are declared
  in both the PEP 621 extra and the PEP 735 group, guarded by `tests/test_packaging.py`. See §3.
- **2026-10-06 — Value type bytes.** Type tags are bytes `0x01`–`0x0E` (`i4`=1 … `bool`=14),
  fixed by `creek.value.ValueType` and documented in `PROTOCOL.md` §5.1. Changing a value is a
  format break. `f8` is E4M3 and saturates; `f16`/`f32` overflow to ±inf. Text parsing is
  range-checked (`InvalidValue`), and `to_bytes`/`from_bytes` are the storage codec.
- **2026-10-06 — Store ownership and TTL.** Keys are scoped by `(owner, key)` where `owner` is
  the identity's namespace, never client-supplied. The store is a shared service injected into
  each connection's `Context` by the server (no module-global state). TTLs are absolute wall-clock
  deadlines (`time.time`) so M3 can persist them; expiry is lazy on access. `INCR`/`DECR` create a
  missing key as `i32`, preserve the existing integer type and TTL, and range-check the result.
  `SCAN` uses an integer index into the sorted key list (a simplification; see §14.10).
- **2026-10-06 — Client library.** `creek.client` is async, supports both transports, decodes
  replies to native Python, and raises `CommandError` on server errors. `subscribe()` is an async
  generator; a subscribed client must not run other commands concurrently. It lives in the package
  (not a separate distribution).
- **2026-10-06 — Browser pages.** `web/` is dependency-free ES modules (`bench.js`) served by the
  WS listener: `index.html`, `loadtest.html`, `perf.html`. No CDNs, so they work offline.
- **2026-10-06 — Fuzzing.** `tests/test_fuzz.py` uses seeded `random` (no property-testing
  dependency) to check the parsers only raise expected exceptions and that the value codec is
  canonical.
- **2026-10-06 — Auth token hashing.** Tokens may be plaintext or `scrypt$n$r$p$salt$hash`
  (stdlib `hashlib.scrypt`, n=2^14, r=8, p=1). `authenticate` tries every verifier so the timing
  does not reveal which client matched. `hash_token()` and `scripts/hash_token.py` generate hashes.
- **2026-10-06 — WAL compaction.** `Server._snapshot` captures the mutation counter, writes the
  snapshot, then calls `Wal.compact`: if the counter is unchanged it enqueues a truncate marker the
  writer task applies (dropping records already in the snapshot); otherwise the log is left intact.
- **2026-10-06 — Benchmark harness.** `scripts/bench.py` runs the server in a **child process**
  (the load generator and server must not share an event loop) and reports throughput + latency
  percentiles. It is deliberately thin and lives outside `src/` (not part of the package). Browser
  charts stay in M5. See §13.
- **2026-10-06 — Roadmap extended: multitenancy and clustering.** v1 stays single-node with
  namespace scoping. §1 (non-goals), §8 (auth), and §14 were updated so they no longer contradict
  the roadmap; **M6** (multitenancy) and **M7** (clustering, multi-phase) are now planned.
  Multitenancy builds on the existing owner tagging; clustering is exploratory and must not shape
  v1 code.
- **2026-10-06 — Persistence.** WAL records are ``u32 length || u32 CRC32 || UTF-8 JSON`` with
  values as base64 of `Value.to_bytes()`; a truncated/corrupt tail is tolerated. Snapshots are a
  versioned JSON document written atomically (temp → fsync → rename → dir fsync). The `Wal` runs a
  background writer task (draining an `asyncio.Queue`, `os.fsync` in a thread); `--fsync=always`
  fsyncs after every record *in the background*, so a command never blocks on disk. The store and
  bus journal mutations via the `Journal` protocol; recovery uses non-journaling `restore` paths.
  A snapshot is written on shutdown and every `--snapshot-interval`. See §10 and §14.4.
- **2026-10-06 — Tenant quotas (M6b).** Optional per-tenant limits: `max_keys`, `max_memory`,
  `max_connections`, `max_subscriptions`, and `max_rate` (a token bucket). Memory is an
  **approximation**: `len(key) + len(value.to_bytes())` per key plus retained channel values, not
  allocator overhead. Set via `TENANT CREATE … MAXKEYS n …` / `TENANT SET`; violations raise a
  `QUOTA` error. `Store.usage` / `Bus.usage` compute the estimate on demand (only when a limit is
  set). Connection slots are acquired at `AUTH` (and for anonymous connections at accept) and
  released on disconnect.
- **2026-10-06 — Tenants (M6a).** Tenants are explicit records (`creek.tenant`) with a lifecycle;
  `TENANT CREATE|LIST|DROP|INFO` is admin-scoped. Tenants are auto-provisioned when a client
  authenticates (so existing auth files keep working) and are journaled + snapshotted. `TENANT
  DROP` clears keys, channels, and retained values, then removes the record; a namespace can be
  re-provisioned by authenticating again (DROP is "forget", not a permanent ban).
- **2026-10-06 — Namespace isolation is pinned by tests.** `tests/test_tenancy.py` asserts that
  keys, channels, enumeration, flushing, retained messages, and persisted records are scoped by
  owner, and that ACLs are per-identity (not per-namespace). `AUTH` moves the subscriber to the new
  namespace and clears its subscriptions. These tests are the safety net for M6.
- **2026-10-06 — Pub/sub delivery.** Each connection owns one `Subscriber` with a single bounded
  outbound queue; one writer task drains it, so replies and messages never interleave. Replies
  use blocking `send` (backpressure); pub/sub uses `deliver` (drop the subscriber if full). The
  `Bus` is shared via `Context` like the store. Fanout is a linear scan; a subscriber matching
  both a channel and a pattern is delivered once. Channels are namespaced by owner (§14.12).
  `RETAIN` stores the channel's last value and it is replayed *after* the subscribe confirmation
  (handlers queue it in `Context.pending`, flushed by the transport after the reply).

---

*Keep this file current — it is the project's shared memory.*
