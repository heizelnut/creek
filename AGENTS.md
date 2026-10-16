# AGENTS.md — creek-plc

> Instructions and shared context for AI coding agents (and humans) working in this repository.
> Read this file completely before making changes. Update it whenever you make a decision that
> future work must respect. See also `PLC.md` for the driver/tag-map design and `STATUS.md` for a
> point-in-time snapshot.
>
> **Status: implemented.** `creek-plc` is a single asyncio daemon that mirrors PLC tags into an
> existing Redis instance and forwards writes from a Redis Stream back to the PLC.

## 1. What creek-plc is

`creek-plc` is a **PLC ↔ Redis sync daemon**. It does not implement a database, a protocol, or a
server: **Redis is the store, the pub/sub bus, and the command queue.** The daemon owns only the
part Redis cannot do — talking to PLCs.

```
   PLCs                       creek-plc (this repo)                 Redis
 ┌──────────┐  Modbus/TCP   ┌───────────────────────────┐       ┌──────────────┐
 │ S7-1200  │◄─────────────►│  drivers → Poller         │─SET─► │ plc:<key>    │
 │ Modbus   │  S7comm       │   (read → scale)          │─PUB─► │ plc:<key>    │
 └──────────┘               │                           │       │ (channel)    │
                            │  CommandConsumer          │◄XREAD │ plc:commands │
                            │   → driver.write          │       │ (Stream)     │
                            └───────────────────────────┘       └──────────────┘
```

Core traits:

- **Read path:** a `Poller` per connection reads its tags on an interval, applies scale/offset,
  and writes changed values to Redis as a string key plus a `PUBLISH` on the same channel name.
- **Write path:** Redis cannot tell the daemon that a client wrote a key, so writes are explicit.
  A SCADA/HMI appends to a Redis **Stream** (`XADD plc:commands * key <tag> value <v>`); the daemon
  consumes it in a **consumer group**, writes the PLC, reads the tag back, and acknowledges.
- **Redis owns durability.** The daemon is stateless: PLC state is derived and re-polled on
  restart, so nothing is written to disk by the daemon and there is no WAL or snapshot.
- **Redis owns auth.** There is no token file or ACL layer in the daemon; use Redis ACLs and run
  the daemon on a trusted network (OT segment). See §8.
- **Quality** is exposed as a companion key `<prefix><key>.q` = `good` / `stale` so a SCADA can
  tell whether a value is trustworthy.
- **Single asyncio event loop** owns all mutable state; blocking S7 calls are offloaded with
  `asyncio.to_thread` (see §11).
- **Drivers:** Modbus/TCP (in-tree, no dependency) and Siemens S7 (`python-snap7`).

Non-goals: reimplementing Redis, a RESP server, persistence, auth, clustering, a web UI, or an
HTTP API. If a change starts to add one of those, stop and revisit this file first.

## 2. How to use this file (agents)

- Treat this as the source of truth for architecture and conventions. If code and this file
  disagree, fix the code or update this file — do not silently diverge.
- `STATUS.md` is a snapshot (what's built, what's verified, next steps); keep it roughly current.
- Before finishing a task: run `ruff format --check .`, `ruff check .`, `mypy src`, and `pytest -q`
  (prefix with `uv run` when not in an activated venv). All four must pass.
- Prefer the standard library. Any new third-party dependency must be justified in the commit
  message and, if structural, recorded in §12.
- Keep public behavior changes documented here and in `PLC.md`.
- Anything marked **DECISION PENDING** in §14 is an open question: ask before assuming.

## 3. Quick start

Requires **Python 3.12+**. Use either **uv** (fast, locks the interpreter) or plain **pip**.

### With uv

```bash
uv sync                          # creates .venv, installs deps + dev group
uv run creek-plc --config plc.json
uv run pytest -q
uv run ruff format --check .
uv run ruff check .
uv run mypy src
```

### With pip

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"         # [dev] adds pytest, ruff, mypy, pymodbus
creek-plc --config plc.json
```

### Running it

```bash
uv run creek-plc \
  --config ./plc.json \
  --redis-url redis://127.0.0.1:6379/0 \
  --key-prefix plc: \
  --command-stream plc:commands \
  --log-level info
```

Flags (all parsed in `src/creek/cli.py` via `argparse`): `--config` (required), `--redis-url`,
`--key-prefix`, `--command-stream`, `--consumer-group`, `--timestamps`, `--dry-run`,
`--log-level`, `--version`. `--dry-run` polls the PLCs and logs changes without touching Redis.

- Entry point: console script `creek-plc = creek.cli:main`; also `python -m creek`.
- Dev dependencies are declared in **both** `[project.optional-dependencies].dev` (pip) and
  `[dependency-groups].dev` (uv); `tests/test_packaging.py` fails if they drift.
- On this machine the system interpreter is old; if uv discovery fails, pin it:
  `uv sync --python /opt/homebrew/bin/python3.14`.

## 4. Architecture at a glance

```
                    ┌───────────────────────────────────────────┐
   PLC ──driver──►  │  Poller        read → scale → Mirror      │ ──► Redis SET + PUBLISH
                    │                                           │
   PLC ◄─driver──   │  CommandConsumer   XREADGROUP → write     │ ◄── Redis Stream
                    └───────────────────────────────────────────┘
                              creek.daemon wires it together
```

| Component | Module | Responsibility |
|---|---|---|
| CLI | `creek.cli` | Flags, logging, signals, lifecycle |
| Daemon | `creek.daemon` | Wire config + Redis + manager; `serve_forever` |
| Mirror | `creek.mirror` | `Mirror` protocol, `RedisMirror`, `MemoryMirror` |
| Commands | `creek.commands` | `CommandConsumer`: Redis Stream → PLC writes |
| Tag map | `creek.plc.tags` | `Connection`, `Tag`, `PlcConfig` (load + validate) |
| Driver iface | `creek.plc.driver` | `PlcDriver` protocol (connect/close/read/write) |
| Poller | `creek.plc.poller` | Poll loop, scale/offset, change detection, quality |
| Manager | `creek.plc.manager` | Owns connections/drivers/pollers; routes writes |
| Modbus | `creek.plc.modbus` (+ `modbus_server`) | Modbus/TCP driver + in-tree simulator |
| S7 | `creek.plc.s7` | Siemens S7 driver (`python-snap7`) |
| Fake driver | `creek.plc.fake` | In-memory driver for tests/dry runs |
| Values | `creek.value` | Typed primitives, text/binary codec |

## 5. Repository layout

```
creek/
├── AGENTS.md  PLC.md  STATUS.md  README.md
├── pyproject.toml  uv.lock
├── src/creek/
│   ├── __init__.py __main__.py
│   ├── cli.py          flags, lifecycle
│   ├── daemon.py       RedisConfig, Daemon wiring
│   ├── mirror.py       Mirror protocol, RedisMirror, MemoryMirror
│   ├── commands.py     CommandConsumer (write path)
│   ├── errors.py       CreekError, InvalidValue
│   ├── value.py        typed values
│   └── plc/            driver, tags, poller, manager, modbus, modbus_server, s7, fake
└── tests/              test_value test_tags test_mirror test_poller
                        test_commands test_daemon test_cli test_packaging
                        test_modbus test_modbus_pymodbus test_s7
                        support.py (fakes)
```

## 6. Value model

`creek.value` is retained from the earlier project: a tagged primitive used by drivers to decode
raw PLC words. The type is part of the value, not inferred at read time.

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

- Wire/storage format is **little-endian** for fixed-width numerics (`struct` prefix `<`).
- Python `int` is unbounded, so **every** integer type is range-checked on write and after
  arithmetic. `i4`/`u4` are logically 4-bit but stored as a masked byte in v1.
- `Value.text()` is what the daemon writes to Redis: `"1500"`, `"1.5"`, `"true"`, the string.
- Values are immutable (`frozen=True` dataclass); mutations replace the value.

## 7. Redis layout and the command stream

### Keys and channels

| Redis key / channel | Written when | Value |
|---|---|---|
| `<prefix><key>` | a polled tag changes, or a write is read back | the value as `Value.text()` |
| `<prefix><key>.q` | quality changes (`good` ↔ `stale`) | `good` / `stale` |
| `<prefix><key>.ts` | every write, only with `--timestamps` | unix seconds, 3 decimals |

`<prefix>` defaults to `plc:`. Each write is a pipeline of `SET` (value, and `.ts` if enabled) plus
`PUBLISH` on the same channel, so consumers can read current state or subscribe to changes. A
`PUBLISH` happens only when the value actually changed (the poller keeps the last value locally);
quality is published only on a change.

### Write path (Stream)

Redis has no server-side hook to observe a client writing a key, so writes are explicit commands on
a Stream. The daemon consumes them as a consumer group and acknowledges only after the PLC write.

```bash
# a SCADA client asks the daemon to write motor.speed = 20 (engineering units)
redis-cli XADD plc:commands '*' key motor.speed value 20
```

- Fields: `key` (the tag key) and `value` (engineering units; scaled tags are converted back to raw
  counts). Extra fields are ignored.
- Group/consumer: `--consumer-group` (default `creek-plc`) and a per-process consumer name
  (`hostname-pid`), so several daemons can share a group.
- **Every** entry is `XACK`ed, including malformed or rejected ones, so one bad entry cannot block
  the queue. Rejections are logged on the `creek.audit` logger (unknown tag, read-only tag, driver
  error) and counted on the consumer.
- Trade-off: rejected commands are dropped, not dead-lettered (see §14).

### Read-back after a write

`PlcManager.write` writes the PLC, then reads the tag back and mirrors the result, so Redis reflects
the PLC's actual value (not the requested one) as soon as the write completes.

## 8. Auth and safety

- The daemon has **no auth of its own**. Use Redis ACLs to restrict who can `XADD` to the command
  stream, and run the daemon on the OT network. Do not expose the command stream to untrusted
  clients: it is a direct write path to physical equipment.
- Writes are **opt-in per tag** (`"writable": true`); everything is read-only by default.
- Writes are logged to the `creek.audit` logger with `key`, `value`, `entry`, and `stream` fields.
- Consider a global write-enable flag and per-connection rate limits (see §14) so a runaway client
  cannot hammer a PLC.
- `creek-plc` is **not safety-certified**. Interlocks, watchdogs, and fail-safes stay in the PLC;
  the daemon must never be in a control loop.
- There is no bus/PLC TLS in v1. Modbus/TCP and S7 are plaintext by design.

## 9. Configuration

A single JSON file (`--config`):

```json
{
  "connections": [
    {"name": "line1", "driver": "modbus", "host": "10.0.0.10", "port": 502,
     "poll_interval": 0.5, "params": {"unit": 1}},
    {"name": "line2", "driver": "s7", "host": "10.0.0.11", "port": 102,
     "params": {"rack": 0, "slot": 1}}
  ],
  "tags": [
    {"key": "motor.speed", "connection": "line1", "address": "HR0", "type": "u16", "scale": 0.1},
    {"key": "motor.run",   "connection": "line1", "address": "C0",  "type": "bool", "writable": true},
    {"key": "temp",        "connection": "line2", "address": "DB1.DBD0", "type": "f32"}
  ]
}
```

- `key` is the Redis key suffix (prefixed with `--key-prefix`).
- `address` is protocol-specific (see `PLC.md` §4/§5).
- `type` is a value tag (`bool`, `u16`, `i16`, `i32`, `u32`, `f32`, …).
- `scale`/`offset` map raw counts to engineering units (`value * scale + offset`, result `f64`).
- `writable` must be true for a tag to accept writes.
- `params` is driver-specific (`unit`/`word_order` for Modbus; `rack`/`slot` for S7).
- `params.quality = "false"` disables the `<key>.q` companion for that connection.
- A legacy `namespace` field is ignored (it belonged to the removed store).

## 10. Persistence

There is none in the daemon. PLC state is **derived** and re-polled on restart, so mirroring it to
disk would be pointless. Redis owns durability of the mirrored values; the daemon is stateless and
can be restarted or run in parallel (though concurrent pollers for the same tag will race — run one
daemon per PLC, or per tag set).

## 11. Concurrency

- **One asyncio event loop owns all state.** No locks around daemon state.
- **Rule: never `await` inside a logical critical section.** Mutate/decide synchronously, then
  await I/O.
- One `Poller` task per connection and one `CommandConsumer` task. Each writes only its own tags, so
  there is no shared mutable state between them beyond the `Mirror` (stateless) and the manager's
  tag table (built once at startup).
- Blocking work is offloaded: `python-snap7` calls run via `asyncio.to_thread`, serialized by a
  per-connection lock in `S7Driver`. `redis.asyncio` is native async.
- A failed poll or a Redis error **never kills the loop**: both the poller and the consumer catch,
  log, and back off (doubling, capped). A flaky/racy test is a release blocker.
- Run everything under `pytest-asyncio`.

## 12. Dependencies

Prefer the standard library. Current external dependencies:

| Concern | Package | Status |
|---|---|---|
| Redis client | `redis` (`redis.asyncio`) | required |
| Siemens S7 transport | `python-snap7` | required (needs the native `libsnap7`) |
| Testing | `pytest`, `pytest-asyncio` | dev |
| Lint/format | `ruff` | dev |
| Type checking | `mypy` | dev |
| Modbus cross-check | `pymodbus` | dev (tests only) |

Modbus/TCP is implemented **in-tree** (MBAP framing + function codes) with an in-tree simulator;
`pymodbus` is a dev-only dependency used to cross-check our framing against an independent
implementation. `f16`/`f8` are implemented in-tree. Do not add an ORM, a web framework, or a
logging framework; stdlib `logging` is the logging standard.

Dev dependencies are listed twice — `[project.optional-dependencies].dev` for pip and
`[dependency-groups].dev` for uv — because pip cannot read PEP 735 groups. `uv.lock` is committed.
`tests/test_packaging.py` asserts the two lists are identical, so update both when adding a tool.

## 13. Testing and quality

- Table-driven (parametrized) unit tests for `value`, `tags`, `mirror`, and `poller`.
- Codec golden/round-trip tests in `tests/test_value.py`.
- Driver tests in `tests/test_modbus.py` and `tests/test_s7.py`, with an in-tree
  `ModbusSimulator` and one `pymodbus`-backed cross-check (`tests/test_modbus_pymodbus.py`).
- `tests/test_commands.py` covers stream parsing, write application, acknowledgement, rejection,
  and Redis-error recovery.
- `tests/test_daemon.py` is an end-to-end test: a real `ModbusSimulator` plus a fake Redis
  (`tests/support.py`) proves poll → Redis and Stream → PLC write without external services.
- **No test requires a live Redis or real hardware.** `tests/support.py` holds `FakeRedis`
  (strings, pub/sub, streams, consumer groups) and the daemon is injectable.
- Required checks before merge: `ruff format --check .` clean, `ruff check .`, `mypy src`, and
  `pytest -q` all green.

## 14. Open questions (DECISION PENDING)

1. **Pending reclaim** — a consumer that dies with unacknowledged entries leaves them in the
   pending list. Add `XAUTOCLAIM`/`XPENDING` handling so another daemon picks them up?
2. **Rejected commands** — currently dropped (acked + logged). Add a dead-letter stream?
3. **Write safety** — a global `--read-only`/write-enable flag and per-connection rate limits.
4. **Redis Cluster** — key names use `plc:` (no hash tag), so the stream and its entries live on
   one node. Support `{tag}` hash tags / Sentinel? (The daemon does not use cluster redirects.)
5. **Deadband** — publish only when a value changes by more than a per-tag deadband, to cut chatter.
6. **Timestamps/metadata** — `.ts` exists; add a source/quality JSON form or Redis Streams of
   history instead of overwriting keys?
7. **Config reload** — watch the config file and add/remove connections without a restart.
8. **Nibble packing** for `i4`/`u4` — the value codec byte-masks them; pack two per byte?
9. **`f8` variants** — E4M3 is the only variant; add E5M2?

## 15. Coding conventions

- **Python 3.12+**. Format and lint with `ruff` (line length 100, double quotes). Type-annotate all
  public functions; `mypy --strict` must pass on `src`.
- Use `from __future__ import annotations` for forward references in typing.
- Errors: a small exception hierarchy in `creek/errors.py` (`CreekError` → `InvalidValue`); driver
  and command errors subclass `CreekError`. Never use bare `except`; never `sys.exit` in library
  code (only `creek.cli` decides exit codes).
- Public modules, classes, and functions have docstrings. Handlers are small and pure where
  possible.
- `asyncio` throughout: all I/O is `async`; never call blocking APIs on the loop (offload with
  `asyncio.to_thread`). Never `await` in a state-mutating critical section.
- Logging via stdlib `logging` with structured fields (`logger.info("msg", extra={...})`); no
  `print` outside `creek/cli.py`. Write audits go to the `creek.audit` logger.
- Keep modules cohesive and dependency-acyclic: `creek.mirror` must not import `creek.plc` at
  runtime (it uses `TYPE_CHECKING` for `Tag`) — this avoids an import cycle with `creek.plc`.
- No global mutable state.
- Data carriers are `@dataclass(frozen=True, slots=True)` (e.g. `Value`, `Tag`, `Connection`,
  `RedisConfig`, `WriteCommand`).

## 16. Glossary

| Term | Meaning |
|---|---|
| **creek-plc** | This project: the PLC ↔ Redis sync daemon. |
| **Mirror** | The sink for polled values (`RedisMirror`, `MemoryMirror`). |
| **Tag** | One PLC point mapped to a Redis key (address, type, scale, writable). |
| **Connection** | One PLC link: driver, host/port, poll interval, driver params. |
| **Poller** | Per-connection task: read → scale → mirror, with quality and backoff. |
| **Command stream** | Redis Stream of write requests consumed by the daemon. |
| **Quality** | `good`/`stale` companion key for a tag's trustworthiness. |

## 17. Roadmap

- **M0 — Daemon skeleton:** config, `RedisMirror`, `Poller`, CLI, lifecycle. *(implemented)*
- **M1 — Write path:** Redis Stream command consumer, read-back, audit. *(implemented)*
- **M2 — Drivers:** Modbus/TCP (in-tree + simulator + pymodbus cross-check) and S7 via
  `python-snap7`. *(implemented)*
- **M3 — Quality/staleness:** `<key>.q`, backoff, per-connection status. *(implemented)*
- **M4 — Packaging/tests:** fake Redis, end-to-end daemon tests, `--dry-run`. *(implemented)*
- **Next (candidate):** pending reclaim (§14.1), dead-letter stream (§14.2), write safety limits
  (§14.3), deadband (§14.5), config reload (§14.7), Redis Cluster/Sentinel (§14.4).

## 18. Decision log

Notable decisions future work must respect (newest last).

- **2026-10-16 — Pivot: sync daemon over an existing Redis.** The earlier project reimplemented a
  typed KV store, RESP2 server, persistence, auth, tenancy, and clustering. That was dismantled in
  favour of a daemon that uses Redis as the store, bus, and command queue. Rationale: Redis already
  does those things well; the project's value is PLC connectivity. `value.py` and the `plc/` drivers
  were retained.
- **2026-10-16 — Writes go through a Redis Stream, not keyspace notifications.** Redis has no
  server-side hook for "a client wrote a key". A Stream gives durable, ackable, at-least-once
  commands that work on any Redis without server configuration, and it is an explicit control
  surface (a client can see the queue). Keyspace notifications would require enabling
  `notify-keyspace-events` and are lossy. See §7.
- **2026-10-16 — Redis owns durability and auth.** The daemon keeps no WAL/snapshot (PLC state is
  re-polled) and has no token/ACL layer (Redis ACLs + network placement). This removes persistence
  and tenancy code entirely. See §8/§10.
- **2026-10-16 — Every stream entry is acknowledged, even on failure.** A malformed or rejected
  command is logged on `creek.audit` and acked so it cannot block the queue; the alternative
  (leaving it pending forever) is worse for an operational daemon. Dead-lettering is §14.2.
- **2026-10-16 — One flat value per key.** Values are written as `Value.text()` on `<prefix><key>`,
  with `.q` and optional `.ts` companions, rather than a JSON blob or a Redis hash. This keeps the
  data trivially consumable by any Redis client; type comes from the config. See §7.
- **2026-10-16 — `Mirror` is the seam.** Poller and manager depend on a `Mirror` protocol, not on
  Redis, so `MemoryMirror` powers `--dry-run` and tests without a Redis server. See §4/§13.
- **2026-10-16 — Modbus stays in-tree; S7 uses `python-snap7`.** The in-tree Modbus/TCP client is
  small, tested, and dependency-free (cross-checked against `pymodbus` in dev); S7comm is fiddly
  enough that a battle-tested library is worth the native dependency. See §12 and `PLC.md`.

---

*Keep this file current — it is the project's shared memory.*
