# Creek

Creek is an in-memory key–value store with **typed publish/subscribe** as its primary workload,
written in **async Python** (asyncio). It speaks a **RESP2 subset** over both **WebSocket**
(native, one command per binary frame) and **TCP** (redis-cli compatible for scalar commands), and
persists to disk with a write-ahead log plus periodic snapshots.

> **Status:** early implementation (M0 skeleton). See `AGENTS.md` for the full architecture and
> `PROTOCOL.md` for the wire specification.

Requires **Python 3.12+**. The project works with either **uv** or plain **pip**.

## Quick start

### With uv

```bash
uv sync
uv run creek                    # WS + static :7100, TCP :7101
```

### With pip

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"         # the [dev] extra adds pytest, ruff, mypy
creek
```

Then connect:

```bash
redis-cli -p 7101 PING
```

```python
import asyncio, websockets


async def main():
    async with websockets.connect("ws://localhost:7100/ws") as ws:
        await ws.send(b"PING\r\n")  # same RESP bytes as the TCP listener
        print(await ws.recv())  # b"+PONG\r\n"


asyncio.run(main())
```

## Client library

`creek.client` is an async client that speaks both transports and decodes replies to native
Python values (a server error raises `CommandError`).

```python
import asyncio
from creek.client import connect


async def main():
    async with await connect("ws://localhost:7100/ws") as client:  # or tcp://host:port
        await client.set("temp", "21.5", type="f32", ex=60)
        print(await client.get("temp"))  # ['f32', '21.5']
        await client.publish("news", "hello")


asyncio.run(main())
```

## Replication and sharding

Read replicas (pull-based) and static hash-slot sharding are opt-in:

```bash
# primary
uv run creek --tcp-addr :7101
# replica
uv run creek --tcp-addr :7102 --replicaof 127.0.0.1:7101
# a sharded node (JSON topology; see AGENTS.md §17 M7b)
uv run creek --tcp-addr :7101 --cluster-config cluster.json
```

Replicas serve reads and reject writes with `READONLY`. In a cluster, a key owned by another node
returns `MOVED <slot> <addr>`; the TCP client follows it automatically.

## Browser pages

With `--web-dir ./web`, the WebSocket listener also serves dependency-free load and performance
pages at `/` (`index.html`, `loadtest.html`, `perf.html`, `bench.js`).

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

## Benchmark

`scripts/bench.py` starts a server in a child process and drives it with concurrent clients,
reporting throughput and latency percentiles. Treat the numbers as a regression baseline.

```bash
uv run python scripts/bench.py --command ping --clients 10 --requests 1000
uv run python scripts/bench.py --transport tcp --command set
uv run python scripts/bench.py --command publish --subscribers 5
```
