# Creek Protocol

This document describes the wire protocols Creek speaks. There is **one command
model** and **two codecs**:

| Transport | Codec | Native? |
|---|---|---|
| WebSocket (`/ws`) | JSON envelope | **Yes** |
| TCP | RESP (Redis-style) | Compatibility |
| TLS | *out of scope* | Terminate at a reverse proxy |

Both codecs map onto the same `protocol.Command` / `protocol.Reply` types, so
every command behaves identically regardless of transport. TLS is deliberately
not implemented; put a reverse proxy (nginx, Caddy, Envoy, …) in front of the
TCP listener and terminate TLS there.

---

## 1. Command model

A command is a name plus a list of string arguments:

```
Command{ Name: "SET", Args: ["sensor.r1.temp", "21.5", "TYPE", "f32"] }
```

- Command names are case-insensitive and normalized to upper case.
- Arguments are opaque byte strings at the codec layer; the command layer parses
  them. Numeric values are always sent as strings with an explicit `TYPE`.
- The first argument is conventionally the key or channel.

## 2. Reply model

Replies are one of six kinds, mirroring RESP2:

| Kind | RESP form | JSON form |
|---|---|---|
| Simple string | `+OK\r\n` | `"OK"` |
| Error | `-ERR …\r\n` | `{"ok":false,"error":"ERR …"}` |
| Integer | `:42\r\n` | `42` |
| Bulk string | `$5\r\nhello\r\n` | `"hello"` |
| Null | `$-1\r\n` | `null` |
| Array | `*N\r\n…` | `[…]` |

Every non-error JSON response is wrapped as `{"ok":true,"result":…}`.

---

## 3. WebSocket codec (native)

Connect to `ws://<host>:7100/ws`. Messages are UTF-8 text frames containing a
JSON object.

### 3.1 Request

```json
{ "cmd": "SET", "args": ["sensor.r1.temp", "21.5", "TYPE", "f32", "TAG", "floor1"] }
```

- `cmd` (required) — command name. `command` is accepted as an alias.
- `args` (optional) — array of values. Strings, numbers, and booleans are all
  accepted; numbers are stringified (`21.5` → `"21.5"`, `42` → `"42"`).

### 3.2 Response

```json
{ "ok": true, "result": ["f32", "21.5", "floor1"] }
{ "ok": false, "error": "ERR unknown command 'FOO'" }
```

Arrays nest naturally; `null` results are `{"ok":true,"result":null}`.

### 3.3 Streaming pub/sub messages

After `SUBSCRIBE`/`PSUBSCRIBE`, delivered messages arrive as their own frames,
distinct from command responses:

```json
{ "type": "message", "channel": "sensor.r1.temp", "vtype": "f32",
  "value": "21.5", "tag": "floor1" }
```

The `type` field is `"message"`; command responses never carry a `type` field.
Clients should route frames with `type === "message"` to their subscription
handler and everything else to the pending command response.

---

## 4. RESP codec (TCP)

The TCP listener speaks a RESP2-compatible subset so `redis-cli` and existing
Redis clients can talk to it. Two request forms are accepted.

### 4.1 Array of bulk strings (preferred)

```
*3\r\n$3\r\nSET\r\n$14\r\nsensor.r1.temp\r\n$4\r\n21.5\r\n
```

### 4.2 Inline commands

Whitespace-separated, CRLF-terminated (or LF-terminated):

```
PING\r\n
SET sensor.r1.temp 21.5 TYPE f32\r\n
```

### 4.3 Replies

Standard RESP2 framing, e.g.:

```
+OK\r\n
:1\r\n
$-1\r\n
*3\r\n$3\r\nf32\r\n$4\r\n21.5\r\n$6\r\nfloor1\r\n
```

### 4.4 Streaming pub/sub messages

A delivered message is an array of five bulk strings:

```
*5
$7  message
$14 sensor.r1.temp
$3  f32
$4  21.5
$6  floor1
```

(shown expanded; on the wire it is `*5\r\n$7\r\nmessage\r\n…`).

---

## 5. Typed values

Creek values are primitives with an explicit type tag. Non-string values
**must** declare their type with `TYPE`; omitting it defaults to `str`.

| Tag | Width | Notes |
|---|---|---|
| `i4` / `u4` | 4 bits | signed −8…7 / unsigned 0…15 (byte-masked in v1) |
| `i8` / `u8` | 8 bits | |
| `i16` / `u16` | 16 bits | |
| `i32` / `u32` | 32 bits | |
| `f8` | 8 bits | E4M3 |
| `f16` | 16 bits | IEEE 754 binary16 |
| `f32` | 32 bits | |
| `f64` | 64 bits | |
| `str` | variable | UTF-8, max 512 KiB |
| `bool` | 1 bit | `true` / `false` |

On the wire (RESP and JSON) values are transmitted as **text**. The binary
encoding below is used only for storage and persistence.

### 5.1 Binary encoding (storage / WAL)

`1 type byte || payload`, little-endian for fixed-width numerics:

| Type | Payload |
|---|---|
| `i4`,`u4` | 1 byte, low nibble used |
| `i8`,`u8`,`bool` | 1 byte (`bool`: `0`/`1`) |
| `i16`,`u16`,`f16` | 2 bytes LE |
| `i32`,`u32`,`f32` | 4 bytes LE |
| `f64` | 8 bytes LE |
| `str` | `uvarint` length + UTF-8 bytes |

`f8` is OCP E4M3: 1 sign, 4 exponent (bias 7), 3 mantissa. Exponent field 15
with mantissa 7 is NaN; overflow saturates to the largest finite value (448).

---

## 6. Command grammar

### Connection / auth

| Command | Args | Reply |
|---|---|---|
| `PING [msg]` | | `PONG` or `msg` |
| `ECHO msg` | | `msg` |
| `AUTH token` | | `OK` / `WRONGPASS` |
| `QUIT` | | `OK`, then the server closes the connection |

### Key–value

| Command | Args | Reply |
|---|---|---|
| `SET key value [TYPE t] [TAG tag] [EX s\|PX ms] [NX\|XX]` | | `OK` / `null` |
| `GET key` | | `[type, value, tag]` / `null` |
| `MGET key…` | | array of entries / `null` |
| `DEL key…` | | integer (deleted count) |
| `EXISTS key…` | | integer (existing count) |
| `TYPE key` | | type tag or `none` |
| `TAG key` | | tag / `null` |
| `SETTAG key tag` | | `OK` / `ERR no such key` |
| `EXPIRE key seconds` | | `1` / `0` |
| `TTL key` | | seconds, `-1` (no TTL), `-2` (missing) |
| `KEYS pattern` | | array of keys |
| `SCAN cursor [MATCH p] [COUNT n]` | | `[nextCursor, [keys…]]` |
| `INCR key [BY n]` / `DECR key [BY n]` | | integer |
| `DBSIZE` | | integer |

### Pub/Sub

| Command | Args | Reply |
|---|---|---|
| `PUBLISH channel value [TYPE t] [TAG tag] [RETAIN]` | | integer (receivers) |
| `SUBSCRIBE channel…` | | array of `[subscribe, channel, count]` |
| `PSUBSCRIBE pattern…` | | array of `[psubscribe, pattern, count]` |
| `UNSUBSCRIBE [channel…]` | | integer |
| `PUNSUBSCRIBE [pattern…]` | | integer |
| `PUBSUB CHANNELS [pattern]` | | array of channels |
| `PUBSUB NUMSUB [channel…]` | | `[channel, count, …]` |
| `PUBSUB NUMPAT` | | integer |

`RETAIN` stores the message as the channel's last value and persists it; new
matching subscribers receive it immediately after subscribing.

### Index

> Note: the index is currently a **naive placeholder**. `TREE`/`STATS` rebuild the tree by
> scanning every key on each call, and `FIND` is a linear scan. The response shapes below are
> stable; the implementation is expected to become an incremental trie later.

| Command | Args | Reply |
|---|---|---|
| `INDEX TREE [prefix] [DEPTH n]` | | JSON `NodeView` / `null` |
| `INDEX STATS` | | JSON `Stats` |
| `INDEX FIND pattern` | | array of keys |
| `INDEX REBUILD` | | `OK` (admin only) |
| `INDEX SEPARATOR [byte]` | | current or `OK` |

`NodeView`:

```json
{
  "segment": "sensor", "count": 3, "terminal": 0,
  "types": {"f32": 3}, "tags": {"floor1": 2},
  "homogeneous": "f32", "wide": false,
  "children": [ { "segment": "room1", "count": 2, "terminal": 0, "children": [] } ]
}
```

`Stats`:

```json
{ "keys": 3, "nodes": 6, "separator": ".", "max_depth": 3,
  "homogeneous_nodes": 4, "wide_nodes": 0,
  "types": {"f32": 3}, "tags": {"floor1": 2} }
```

### Admin

| Command | Args | Reply |
|---|---|---|
| `INFO [section]` | | bulk text |
| `FLUSHALL` | | `OK` (admin only) |
| `COMMAND [COUNT]` | | array of names / integer |

---

## 7. Patterns

Two glob dialects are used:

- **Channel/key globs** (`KEYS`, `PUBSUB`, `PSUBSCRIBE`, `INDEX FIND`):
  `*` matches any run of characters including delimiters; `?` matches one
  character. This matches Redis semantics, so `news.*` also matches
  `news.tech.redis`.
- **Segment patterns** (`INDEX FIND`): `*` and `?` operate within one segment
  (split on the detected separator), while `**` or `>` matches zero or more
  whole segments. `sensor.*.temp` matches `sensor.room1.temp` but not
  `sensor.room1.sub.temp`; `sensor.>` matches both.

## 8. Errors

Errors are `ERR …` (or `WRONGPASS`, `NOPERM`, `NOAUTH`) and never terminate the
connection. Unknown commands return `ERR unknown command '<name>'`; arity
violations return `ERR wrong number of arguments for '<cmd>'`.

## 9. Versioning

- The WebSocket envelope is currently unversioned. A future `HELLO` handshake
  will negotiate versions.
- The RESP subset tracks RESP2 framing.
- Storage formats are versioned separately (`persist.SnapshotVersion`, and the
  value codec's type byte); see `AGENTS.md` §11.
