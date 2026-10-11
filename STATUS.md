# Creek — status

> A point-in-time snapshot of what is built, what is verified, and what is left. The architecture
> and decisions live in `AGENTS.md`; the wire spec in `PROTOCOL.md`; the PLC and clustering designs
> in `PLC.md` and `CLUSTERING.md`. Update this file as the project moves.

- **Snapshot:** 2026-10-11, commit `62550a9`.
- **Tests:** 392 passing.
- **Gates:** `ruff format --check .`, `ruff check .`, `mypy src` (strict), `pytest -q` — all clean.

## What Creek is

An in-memory, typed key–value store with **publish/subscribe**, written in **async Python**. Its
primary application is **centralizing PLC/SCADA state**: protocol drivers poll PLCs into typed keys
and publish changes, so a SCADA/HMI can view and control the process over Creek's RESP2
(TCP/WebSocket) interface. It also does the Redis-like things you'd expect: KV with TTL, pub/sub,
persistence, auth, and multi-tenancy.

## Milestones

| Milestone | Status |
|---|---|
| M0 — skeleton: TCP + WS, shared RESP2, command registry | ✅ |
| M1a — value model (14 typed primitives, binary codec, f8 E4M3) | ✅ |
| M1b — store + KV commands (`SET`/`GET`/`DEL`/`TTL`/`SCAN`/`INCR`…) | ✅ |
| M2 — pub/sub (fanout, patterns, retained) | ✅ |
| M3 — persistence (WAL + atomic snapshots + recovery) | ✅ |
| M4 — auth (scrypt tokens) + WAL compaction | ✅ |
| M5 — hardening (async client, fuzz tests, browser load/perf pages) | ✅ |
| M6a — tenant registry + admin API | ✅ |
| M6b — per-tenant quotas | ✅ |
| M6c — tenant observability + hardening | ✅ |
| M7a — read replicas (pull-based) | ✅ |
| M7b — sharding (hash slots + `MOVED`) | ✅ |
| M7c — failover (manual + opt-in) | ⚠️ best-effort; not consensus-safe |
| M8 — full clustering (gossip, elections, resharding, cluster pub/sub) | design sketch (`CLUSTERING.md`) |
| M9a — PLC tag map, driver interface, poller | ✅ |
| M9b — Modbus/TCP driver + simulator | ✅ |
| M9c — S7 driver (`python-snap7`) | ✅ |
| M9d — PLC manager, `--plc-config`, `PLC` commands, write audit | ✅ |
| M9e — PLC quality/staleness, backoff, per-connection status | ✅ |

## What works

- **Transport:** one RESP2 codec over TCP and WebSocket (binary frames, one command per frame);
  WebSocket also serves static files.
- **Values:** `i4/u4`…`i32/u32`, `f8` (E4M3), `f16`, `f32`, `f64`, `str`, `bool`; text and binary
  codecs; range-checked.
- **KV:** `SET`/`GET`/`MGET`/`DEL`/`EXISTS`/`TYPE`/`EXPIRE`/`TTL`/`KEYS`/`SCAN`/`DBSIZE`/`INCR`/`DECR`.
- **Pub/sub:** `PUBLISH` (`TYPE`/`RETAIN`), `SUBSCRIBE`/`PSUBSCRIBE`/`UNSUBSCRIBE`/`PUNSUBSCRIBE`,
  `PUBSUB` introspection; per-connection outbound queue with backpressure.
- **Persistence:** length+CRC WAL, atomic snapshots, recovery, best-effort compaction.
- **Auth:** JSON token file, plaintext or `scrypt$…`, constant-time compare, coarse ACLs.
- **Tenancy:** explicit tenants (`TENANT …`), auto-provisioned on `AUTH`; quotas (keys, memory,
  connections, subscriptions, command rate → `QUOTA`); counters; key/channel length limits; audit log.
- **Replication/sharding:** `REPLICAOF`/`ROLE`/`INFO replication`; `KEYSLOT`, `CLUSTER INFO|SLOTS`,
  `MOVED`/`CROSSSLOT`; the TCP client follows `MOVED`.
- **PLC:** Modbus/TCP (in-tree) and S7 (`python-snap7`); tag map, polling, scaling, quality
  (`<key>.q`), `PLC LIST|TAGS|INFO|WRITE`, write audit.
- **Clients/tooling:** `creek.client` (async, both transports), `scripts/bench.py`,
  `scripts/hash_token.py`, `web/` load & perf pages.

## Run it

```bash
uv sync                                    # or: pip install -e ".[dev]"
uv run creek                               # WS + static :7100, TCP :7101
uv run pytest -q                           # 392 tests
uv run python scripts/bench.py --command ping

# PLC gateway (Modbus + S7)
uv run creek --plc-config plc.json
```

S7 requires the native snap7 library (`brew install snap7`, `apt install libsnap7`).

## Sizes

| | lines |
|---|---|
| `src/` | ~4,800 |
| `tests/` | ~3,750 |
| `web/` | ~430 |
| `scripts/` | ~260 |

## Dependencies

- **Runtime:** `websockets` (transport), `python-snap7` (S7; needs the native library).
- **Dev:** `pytest`, `pytest-asyncio`, `ruff`, `mypy`, `pymodbus` (Modbus cross-check only).
- Modbus/TCP, the value codecs (`f8`/`f16`), persistence, and replication are all in-tree.

## Known limitations

- **Not safety-certified.** PLC writes can move machinery; interlocks stay in the PLC. Test
  read-only against real hardware first, on a segmented OT network.
- **Durability gap:** a command replies before its WAL record is fsynced (background writer). A power
  loss can lose the last acknowledged writes. No `WAIT`/group-commit barrier yet.
- **M7c failover is best-effort:** no leader election; a returning primary can split-brain.
- **No cluster-wide pub/sub** (node-local), no resharding/`ASK`, no gossip membership.
- **Single event loop, single node by default**; clustering is opt-in and partial.
- SCAN uses an index cursor; `i4`/`u4` are byte-masked (not nibble-packed); `f8` is E4M3 only.
- Pattern deliveries don't carry the matched pattern.

## Next steps

1. **Durable-ack barrier** (`WAIT` / group commit) — prerequisite for trusting failover; also
   fixes the reply-before-fsync gap.
2. **Crash-recovery test** — kill mid-write, restart, assert consistency.
3. **M8** — full clustering per `CLUSTERING.md` (config epochs → gossip → failure quorum →
   elections → resharding → cluster pub/sub).
4. **PLC breadth** — OPC UA or CIP drivers, `PLC PAUSE`/`RESUME`, more per-connection metrics.

## Open questions

See `AGENTS.md` §14 (DECISION PENDING): parser re-parse, SCAN cursor, pattern deliveries, pub/sub
channel scoping, failover safety, cluster-wide pub/sub.
