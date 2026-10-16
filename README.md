# creek-plc

`creek-plc` is a small **async Python daemon that syncs PLCs with an existing Redis instance**.

It polls **Modbus/TCP** and **Siemens S7** devices into Redis keys, publishes changes, and forwards
writes from a Redis Stream back to the PLC. Redis is the store, the pub/sub bus, and the command
queue — this project only owns the part Redis cannot do: talking to PLCs.

```
   PLCs                creek-plc                    Redis                 SCADA / HMI
 ┌──────────┐  Modbus  ┌──────────────┐   SET/PUBLISH   ┌─────────┐  GET/SUBSCRIBE
 │ S7-1200  │◄────────►│  poller      │────────────────►│ plc:*   │◄──────────────
 │ Modbus   │  S7comm  │  command c.  │◄── XREADGROUP ──│ Stream  │◄── XADD
 └──────────┘          └──────────────┘                 └─────────┘
```

> **Status:** implemented (M0–M4). See `AGENTS.md` for the architecture and `PLC.md` for the
> driver/tag-map design.

Requires **Python 3.12+** and a reachable **Redis**. S7 support uses `python-snap7`, which is pure
Python (no native library to install). The project works with either **uv** or plain **pip**.

## Quick start

### With uv

```bash
cp plc.example.json plc.json      # sample tag map; edit hosts/tags for your PLCs
uv sync
uv run creek-plc --config plc.json
```

### With pip

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp plc.example.json plc.json      # sample tag map; edit hosts/tags for your PLCs
creek-plc --config plc.json
```

### Tag map

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
    {"key": "motor.run", "connection": "line1", "address": "C0", "type": "bool", "writable": true},
    {"key": "temp", "connection": "line2", "address": "DB1.DBD0", "type": "f32"}
  ]
}
```

S7 uses `python-snap7`, which is pure Python — no native library to install. See `PLC.md`.

### What appears in Redis

For each tag the daemon writes, when the value changes:

```
plc:motor.speed      "150.0"     # SET (last value) + PUBLISH on the same channel
plc:motor.speed.q    "good"      # quality: good / stale (opt-out per connection)
```

`--key-prefix` changes `plc:`; `--timestamps` also writes `plc:motor.speed.ts`.

Read current state or subscribe to changes with any Redis client:

```bash
redis-cli GET plc:motor.speed
redis-cli SUBSCRIBE plc:motor.speed
```

### Writing to a PLC

Redis has no server-side hook for "a client wrote a key", so writes are explicit commands on a
Stream. The daemon consumes them, writes the PLC, reads the tag back, and acknowledges:

```bash
# motor.speed = 20 engineering units (scaled tags are converted back to raw counts)
redis-cli XADD plc:commands '*' key motor.speed value 20
```

Only tags marked `"writable": true` accept writes; every write is audited on the `creek.audit`
logger. Protect the stream with Redis ACLs and run on the OT network — anyone who can `XADD` can
write to a PLC.

## CLI

```bash
creek-plc \
  --config ./plc.json \
  --redis-url redis://127.0.0.1:6379/0 \
  --key-prefix plc: \
  --command-stream plc:commands \
  --consumer-group creek-plc \
  --timestamps \
  --log-level info
```

`--dry-run` polls the PLCs and logs changes without touching Redis — useful when commissioning a
connection.

## Development

Both environments run the same checks.

```bash
# with uv
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest -q
```

```bash
# with pip (virtualenv activated)
ruff format --check .
ruff check .
mypy src
pytest -q
```

Tests need no live Redis and no hardware: `tests/support.py` provides a fake Redis, and the Modbus
driver is tested against an in-tree simulator (plus a `pymodbus` cross-check).
