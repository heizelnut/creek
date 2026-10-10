# PLC connectivity — design sketch

> **Status: design sketch + foundation implemented.** This describes how Creek centralizes PLC
> state for a SCADA system over **Modbus/TCP** and **Siemens S7**. The tag model, driver interface,
> and poller exist (`src/creek/plc/`); the Modbus and S7 wire drivers are next. See `AGENTS.md`
> §17 (M9).

## 1. The goal

Creek sits between PLCs and a SCADA/HMI:

```
   PLCs                    Creek (this project)                   SCADA / HMI
 ┌──────────┐  Modbus/TCP  ┌────────────────────────────┐  RESP/WS  ┌───────────┐
 │ S7-1200  │◄────────────►│  drivers → poller          │◄─────────►│ SCADA     │
 │ Modbus   │  S7comm      │   typed keys + pub/sub     │           │ client    │
 └──────────┘              │   SET → driver.write       │           └───────────┘
                           └────────────────────────────┘
```

- **Read path:** a poller reads a connection's tags on an interval, writes them into the store as
  **typed values**, and publishes changes.
- **Write path:** a SCADA client issues `SET <tag> <value>` (or `PLC WRITE`); Creek forwards it to
  the PLC and updates the mirror. Writes are explicit, per-tag opt-in, and audited.
- **View path:** SCADA clients read tags with `GET`/`MGET` and subscribe to change notifications.

The PLC remains the source of truth for inputs; Creek is a **mirror**, not a control loop. Safety
interlocks stay in the PLC.

## 2. Configuration

A single JSON file (`--plc-config`):

```json
{
  "connections": [
    {"name": "line1", "driver": "modbus", "host": "10.0.0.10", "port": 502,
     "namespace": "line1", "poll_interval": 0.5, "params": {"unit": 1}},
    {"name": "line2", "driver": "s7", "host": "10.0.0.11", "port": 102,
     "namespace": "line2", "params": {"rack": 0, "slot": 1}}
  ],
  "tags": [
    {"key": "motor.speed", "connection": "line1", "address": "HR0", "type": "u16", "scale": 0.1},
    {"key": "motor.run",   "connection": "line1", "address": "C0",  "type": "bool", "writable": true},
    {"key": "temp",        "connection": "line2", "address": "DB1.DBD0", "type": "f32"}
  ]
}
```

- `namespace` is the Creek owner: SCADA clients authenticate into it to read/write its tags.
- `address` is protocol-specific (see §4/§5).
- `type` is a Creek type tag (`bool`, `u16`, `i16`, `i32`, `u32`, `f32`, …).
- `scale`/`offset` map raw counts to engineering units (`value * scale + offset`, result `f64`).
- `writable` must be true for a tag to accept writes; everything is read-only by default.

## 3. Components

| Module | Responsibility |
|---|---|
| `creek.plc.tags` | `Connection`, `Tag`, `PlcConfig` (load + validate) |
| `creek.plc.driver` | `PlcDriver` protocol (connect/close/read/write) |
| `creek.plc.poller` | `Poller`: read → scale → store → publish |
| `creek.plc.modbus` | Modbus/TCP driver *(next)* |
| `creek.plc.s7` | S7comm driver *(next)* |
| `creek.plc.manager` | owns connections, drivers, pollers; routes writes *(next)* |

The store write uses the non-journaling `Store.restore`: PLC state is **derived** and re-polled on
restart, so it must not bloat the WAL. Each changed tag is published on a channel named after its
key, so `SUBSCRIBE motor.speed` (or `PSUBSCRIBE motor.*`) streams updates.

## 4. Modbus/TCP address forms

| Prefix | Area | Access |
|---|---|---|
| `C<n>` | coils | read/write |
| `DI<n>` | discrete inputs | read-only |
| `IR<n>` | input registers | read-only |
| `HR<n>` | holding registers | read/write |

`n` is the **0-based** protocol offset (so `HR0` is holding register 1 in 1-based docs). Values are
16-bit big-endian words; `u32`/`i32`/`f32` span two registers with a configurable `word_order`
(default high word first). Function codes: 01/02/03/04 read, 05/06/15/16 write. Reads are grouped
into contiguous runs (≤125 registers / ≤2000 bits) for efficiency.

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

The driver speaks TPKT (RFC 1006) → COTP connection → S7 Job: `Setup Communication`, `Read Var`,
`Write Var`, batching multiple items per request.

## 6. Quality, staleness, and timestamps

A SCADA system needs to know *whether* a value is trustworthy. The typed-value model has no quality
field, so the plan is a companion key per tag (`<key>.q`) carrying `good|bad|stale` and a
timestamp, or a structured metadata map. **Decision pending** — see §8.

## 7. Safety

- Writes are opt-in per tag (`writable`) and go through an audit log (`creek.audit`).
- Consider a global write-enable flag and per-connection rate limits so a runaway SCADA client
  cannot hammer a PLC.
- Creek is **not** safety-certified. Interlocks, watchdogs, and fail-safes stay in the PLC; Creek
  must never be in a control loop.
- Run Creek on the OT network; segment and monitor it. No bus/PLC TLS in v1.

## 8. Open questions

1. **Dependencies** — implement Modbus/TCP and S7comm in-tree (stdlib only), or use `pymodbus` and
   `python-snap7` (the latter needs the native snap7 library)? Recommendation: in-tree Modbus (it
   is simple); in-tree minimal S7comm to avoid the native dependency, flagged as the riskier piece.
2. **Target S7 models** — S7-300/400 only, or also S7-1200/1500 with PUT/GET + non-optimized DBs?
3. **Control vs monitor** — start read-only, then enable writes per tag, or support writes from day
   one? (Recommendation: read-only first, writes behind `writable` + audit.)
4. **Quality model** — companion `.q` keys, a structured value, or a separate `PLC` command surface?
5. **Batching** — per-tag requests first (simple), then contiguous-run grouping.

## 9. Roadmap (M9)

| Phase | Deliverable |
|---|---|
| **M9a** | Tag map, driver interface, poller *(implemented)* |
| **M9b** | Modbus/TCP driver + fake-server tests |
| **M9c** | S7comm driver + protocol tests |
| **M9d** | Manager + `--plc-config` wiring; `PLC` admin commands; write audit |
| **M9e** | Quality/staleness, reconnect/backoff, per-connection metrics |
