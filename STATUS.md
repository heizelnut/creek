# creek-plc — status

> A point-in-time snapshot of what is built, what is verified, and what is left. The architecture
> and decisions live in `AGENTS.md`; the driver/tag-map design in `PLC.md`. Update this file as the
> project moves.

- **Snapshot:** 2026-10-16, after the Redis-sync pivot.
- **Tests:** 215 passing.
- **Gates:** `ruff format --check .`, `ruff check .`, `mypy src` (strict), `pytest -q` — all clean.

## What creek-plc is

A single asyncio daemon that mirrors PLC tags into an existing Redis instance and forwards writes
from a Redis Stream back to the PLC. Redis is the store, the pub/sub bus, and the command queue; the
daemon owns only PLC connectivity. The earlier typed KV store / RESP server / persistence / auth /
tenancy / clustering code was removed in favour of Redis (see `AGENTS.md` §18).

## Milestones

| Milestone | Status |
|---|---|
| M0 — daemon skeleton: config, `RedisMirror`, `Poller`, CLI, lifecycle | ✅ |
| M1 — write path: Redis Stream consumer, read-back, audit | ✅ |
| M2 — drivers: Modbus/TCP (in-tree + simulator + pymodbus cross-check), S7 (`python-snap7`) | ✅ |
| M3 — quality/staleness (`<key>.q`), backoff, per-connection status | ✅ |
| M4 — packaging/tests: fake Redis, end-to-end daemon tests, `--dry-run` | ✅ |

## What works

- **Read path:** per-connection `Poller` reads tags, applies scale/offset, writes changed values to
  `<prefix><key>` (`SET` + `PUBLISH`) and quality to `<prefix><key>.q`.
- **Write path:** `CommandConsumer` reads `XADD plc:commands key <tag> value <v>` via a consumer
  group, writes the PLC, reads the tag back into the mirror, and acknowledges. Rejections are logged
  and acked.
- **Drivers:** Modbus/TCP (coils, discrete inputs, input/holding registers; contiguous-run batching;
  in-tree simulator) and Siemens S7 (DB/I/Q/M read/write; span grouping; bit read-modify-write).
- **Values:** `i4/u4`…`i32/u32`, `f8` (E4M3), `f16`, `f32`, `f64`, `str`, `bool`; text and binary
  codecs; range-checked.
- **Resilience:** poller and consumer back off (doubling, capped) on PLC or Redis errors instead of
  dying; quality flips to `stale` and the last value is kept.
- **Ops:** `--dry-run`, `--timestamps`, structured logging, `creek.audit` write log, signal handling.

## Verification

- 215 tests, no live Redis and no hardware required.
- `tests/support.py` provides a fake Redis (strings, pub/sub, streams, consumer groups) and the
  daemon takes an injected client; `tests/test_daemon.py` runs a real `ModbusSimulator` end to end.
- Modbus framing is cross-checked against `pymodbus` (dev-only dependency) in
  `tests/test_modbus_pymodbus.py`.

## Not built / open

- Pending reclaim (`XAUTOCLAIM`) for entries left by a dead consumer — `AGENTS.md` §14.1.
- Dead-letter stream for rejected commands — §14.2.
- Global write-enable flag and per-connection write rate limits — §14.3.
- Redis Cluster/Sentinel support (key names have no hash tag) — §14.4.
- Per-tag deadband and config hot-reload — §14.5/§14.7.
