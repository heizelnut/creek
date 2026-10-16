# PLC connectivity — design

> **Status: implemented.** `creek` mirrors PLC state into an existing Redis instance and
> forwards writes from a Redis Stream back to the PLC. Tag map, driver interface, poller, Modbus/TCP
> driver + simulator, S7 driver (`python-snap7`), the Redis mirror, the command consumer, quality,
> and write audit are all built. See `AGENTS.md` §17.

## 1. The goal

`creek` sits between PLCs and a SCADA/HMI, using Redis as the shared store and bus:

```
   PLCs                  creek (this project)                  Redis                SCADA / HMI
 ┌──────────┐  Modbus  ┌──────────────────────────────┐   SET/PUBLISH   ┌─────────┐  GET/SUBSCRIBE
 │ S7-1200  │◄────────►│  drivers → Poller            │────────────────►│ plc:*   │◄──────────────
 │ Modbus   │  S7comm  │   read → scale → mirror      │                 │ Stream  │
 └──────────┘          │  CommandConsumer             │◄─── XREADGROUP ─│ plc:cmd │◄── XADD
                       │   → driver.write             │                 └─────────┘
                       └──────────────────────────────┘
```

- **Read path:** a poller reads a connection's tags on an interval, applies scale/offset, and writes
  changed values to Redis (`SET` the key, `PUBLISH` the channel).
- **Write path:** a SCADA client `XADD`s to the command stream; the daemon consumes it, writes the
  PLC, reads the tag back, and acknowledges.
- **View path:** SCADA clients `GET` the mirror keys and `SUBSCRIBE` to change channels.

The PLC remains the source of truth for inputs; the daemon is a **mirror**, not a control loop.
Safety interlocks stay in the PLC.

## 2. Configuration

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

- `key` is the Redis key suffix; the daemon prefixes it with `--key-prefix` (default `plc:`).
- `address` is protocol-specific (see §4/§5).
- `type` is a value tag (`bool`, `u16`, `i16`, `i32`, `u32`, `f32`, …).
- `scale`/`offset` map raw counts to engineering units (`value * scale + offset`, result `f64`).
- `writable` must be true for a tag to accept writes; everything is read-only by default.
- `params.quality = "false"` disables the `<key>.q` companion for that connection.
- A legacy `namespace` field is ignored.

## 3. Components

| Module | Responsibility |
|---|---|
| `creek.plc.tags` | `Connection`, `Tag`, `PlcConfig` (load + validate) |
| `creek.plc.driver` | `PlcDriver` protocol (connect/close/read/write) |
| `creek.plc.poller` | `Poller`: read → scale → mirror, change detection, quality, backoff |
| `creek.plc.manager` | owns connections, drivers, pollers; routes writes and reads back |
| `creek.plc.modbus` | Modbus/TCP driver |
| `creek.plc.modbus_server` | in-tree Modbus/TCP simulator (tests/dry runs) |
| `creek.plc.s7` | S7 driver (`python-snap7`) |
| `creek.plc.fake` | in-memory driver (tests/dry runs) |
| `creek.mirror` | `Mirror` protocol, `RedisMirror`, `MemoryMirror` |
| `creek.commands` | `CommandConsumer`: Redis Stream → PLC writes |

## 4. Modbus/TCP address forms

| Prefix | Area | Access |
|---|---|---|
| `C<n>` | coils | read/write |
| `DI<n>` | discrete inputs | read-only |
| `IR<n>` | input registers | read-only |
| `HR<n>` | holding registers | read/write |

`n` is the **0-based** protocol offset (so `HR0` is holding register 1 in 1-based docs). Values are
16-bit big-endian words; `u32`/`i32`/`f32` span two registers with a configurable
`params.word_order` (`"big"` default, `"little"`). Function codes: 01/02/03/04 read, 05/06/15/16
write. Reads are grouped into contiguous runs (≤125 registers / ≤2000 bits) for efficiency.

## 5. S7 (Siemens) address forms

| Address | Meaning |
|---|---|
| `DB<n>.DBX<m>` | data block bit |
| `DB<n>.DBB<m>` | data block byte |
| `DB<n>.DBW<m>` | data block word |
| `DB<n>.DBD<m>` | data block dword |
| `I<...>` / `Q<...>` / `M<...>` | inputs / outputs / markers (same X/B/W/D suffixes) |

Targets: **S7-300/400**, and **S7-1200/1500** with PUT/GET enabled and **non-optimized** DBs.
Symbolic/optimized access needs S7comm-plus, which is undocumented and out of scope.

The driver is built on **`python-snap7`** (pure Python since v3: it speaks TPKT/COTP/S7 directly,
with no native `libsnap7` to install). `python-snap7` is synchronous, so every device call is offloaded with
`asyncio.to_thread` and serialized by a per-connection lock; reads are grouped by `(area, db)` into
one span request per group. S7 is big-endian; bit writes are read-modify-write so neighbouring bits
are preserved.

## 6. Quality, staleness, and backoff

A SCADA system needs to know *whether* a value is trustworthy. While a connection is healthy every
tag is `good`; when a poll fails the tags become `stale` (the last known value is kept, not cleared).
Quality is written to the companion key `<prefix><key>.q` and published on that channel, so a SCADA
can subscribe to it. It can be disabled per connection with `params.quality = "false"`. A failed
poll also backs off (doubling, capped at 60s) so an offline PLC is not hammered.

## 7. Writes

Writes are opt-in per tag (`writable`) and arrive on the Redis Stream configured by
`--command-stream` (default `plc:commands`):

```bash
redis-cli XADD plc:commands '*' key motor.speed value 20
```

- `key` is the tag key; `value` is in engineering units (scaled tags are converted back to raw
  counts via `(value - offset) / scale`).
- The daemon writes the PLC, reads the tag back into the mirror, and `XACK`s the entry.
- Rejections (unknown tag, read-only tag, driver error, malformed entry) are logged on the
  `creek.audit` logger and acknowledged anyway, so one bad entry cannot block the queue.
- The consumer group (`--consumer-group`, default `creek`) with a per-process consumer name lets
  several daemons share the work.

## 8. Safety

- Writes are opt-in per tag (`writable`) and audited on `creek.audit`.
- The daemon has no auth of its own: protect the command stream with Redis ACLs and run on the OT
  network. Anyone who can `XADD` can write to a PLC.
- Consider a global write-enable flag and per-connection rate limits (see `AGENTS.md` §14).
- `creek` is **not safety-certified**. Interlocks, watchdogs, and fail-safes stay in the PLC;
  it must never be in a control loop.
- No bus/PLC TLS in v1.

## 9. Open questions

1. **Dependencies** — resolved: Modbus/TCP is in-tree (no dependency); S7 uses `python-snap7`
   (pure Python since v3, no native `libsnap7` needed). `pymodbus` is a dev-only cross-check.
2. **Rejected commands** — dropped and logged today; add a dead-letter stream?
3. **Pending reclaim** — reclaim unacknowledged entries from dead consumers with `XAUTOCLAIM`?
4. **Deadband** — publish only when a value changes by more than a per-tag threshold.
5. **Batching** — reads are already grouped; consider pipelining mirror writes across tags.
