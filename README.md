# Creek

Creek is an in-memory key–value store with **typed publish/subscribe** as its primary workload,
written in **async Python** (asyncio). It speaks a **RESP2 subset** over both **WebSocket**
(native, one command per binary frame) and **TCP** (redis-cli compatible for scalar commands), and
persists to disk with a write-ahead log plus periodic snapshots.

> **Status:** early implementation (M0 skeleton). See `AGENTS.md` for the full architecture and
> `PROTOCOL.md` for the wire specification.

## Quick start

```bash
uv sync
uv run creek                    # WS + static :7100, TCP :7101
uv run pytest -q
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

## Development

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest -q
```
