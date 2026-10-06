# Creek Protocol

> **Status: complete for v1 (M0–M5).** Framing, connection/admin, key–value, and pub/sub commands
> over both transports, with WAL + snapshot persistence, scrypt token hashing, a client library,
> fuzz tests, and browser load/perf pages (see `AGENTS.md` §17).

There is **one command model** and **one codec** (a RESP2 subset). It is carried by
two transports:

| Transport | Framing | Native? |
|---|---|---|
| WebSocket (`/ws`) | one RESP2 command per binary frame | **Yes** |
| TCP | RESP2 byte stream | Compatibility |
| TLS | *out of scope* | Terminate at a reverse proxy |

The transports only differ in framing. The bytes on the wire are identical, and
every command behaves the same regardless of transport. TLS is deliberately not
implemented; put a reverse proxy (nginx, Caddy, Envoy, …) in front of the TCP
listener and terminate TLS there.

---

## 1. Command model

A command is a name plus a list of string arguments:

```python
Command(name="SET", args=("sensor.r1.temp", "21.5", "TYPE", "f32"))
```

- Command names are case-insensitive and normalized to upper case.
- Arguments are opaque byte strings at the codec layer; the command layer parses
  them. Numeric values are always sent as strings with an explicit `TYPE`.
- The first argument is conventionally the key or channel.

## 2. Reply model

Replies are one of six kinds, mirroring RESP2:

| Kind | RESP form |
|---|---|
| Simple string | `+OK\r\n` |
| Error | `-ERR …\r\n` |
| Integer | `:42\r\n` |
| Bulk string | `$5\r\nhello\r\n` |
| Null | `$-1\r\n` |
| Array | `*N\r\n…` |

---

## 3. WebSocket transport (native)

Connect to `ws://<host>:7100/ws`. WebSocket is used **only as framing**: each
message is a **binary frame** carrying exactly one RESP2 command, and the reply
is sent back as a single binary frame carrying the RESP2 reply. There is no JSON
layer; a JSON blob is not a valid command and is rejected as an unknown command.

### 3.1 Request

One command per frame, in either RESP form (see §4): an array of bulk strings

```
*3\r\n$3\r\nSET\r\n$14\r\nsensor.r1.temp\r\n$4\r\n21.5\r\n
```

or an inline command

```
PING\r\n
SET sensor.r1.temp 21.5 TYPE f32\r\n
```

A frame must contain exactly one command; trailing bytes are a protocol error.

### 3.2 Response

One reply per frame, using standard RESP2 framing:

```
+OK\r\n
:1\r\n
$-1\r\n
*2\r\n$3\r\nf32\r\n$4\r\n21.5\r\n
```

### 3.3 Streaming pub/sub messages

After `SUBSCRIBE`/`PSUBSCRIBE`, delivered messages arrive as their own frames,
identical to the TCP form: an array of four bulk strings.

```
*4
$7  message
$14 sensor.r1.temp
$3  f32
$4  21.5
```

(shown expanded; on the wire it is `*4\r\n$7\r\nmessage\r\n…`). Command replies
and delivered messages are distinguished by the first bulk string: `message` for
a delivery, anything else for a reply.

### 3.4 Static files

The WebSocket listener also answers plain HTTP requests from `--web-dir` at `/`;
only the `/ws` path upgrades to a WebSocket.

---

## 4. RESP2 codec (shared)

Both transports speak a RESP2-compatible subset so `redis-cli` and existing
Redis clients can talk to the TCP listener. Two request forms are accepted.

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
*2\r\n$3\r\nf32\r\n$4\r\n21.5\r\n
```

### 4.4 Streaming pub/sub messages

A delivered message is an array of four bulk strings:

```
*4
$7  message
$14 sensor.r1.temp
$3  f32
$4  21.5
```

(shown expanded; on the wire it is `*4\r\n$7\r\nmessage\r\n…`).

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

On the wire values are transmitted as **text**. The binary encoding below is used
only for storage and persistence.

### 5.1 Binary encoding (storage / WAL)

`1 type byte || payload`, little-endian for fixed-width numerics. The type byte
is **stable** (it is part of the on-disk format):

| Type | Byte | Payload |
|---|---|---|
| `i4` | `0x01` | 1 byte, low nibble used |
| `u4` | `0x02` | 1 byte, low nibble used |
| `i8` | `0x03` | 1 byte |
| `u8` | `0x04` | 1 byte |
| `i16` | `0x05` | 2 bytes LE |
| `u16` | `0x06` | 2 bytes LE |
| `i32` | `0x07` | 4 bytes LE |
| `u32` | `0x08` | 4 bytes LE |
| `f8` | `0x09` | 1 byte, E4M3 |
| `f16` | `0x0A` | 2 bytes LE |
| `f32` | `0x0B` | 4 bytes LE |
| `f64` | `0x0C` | 8 bytes LE |
| `str` | `0x0D` | `uvarint` length + UTF-8 bytes |
| `bool` | `0x0E` | 1 byte, `0`/`1` |

`f8` is OCP E4M3: 1 sign, 4 exponent (bias 7), 3 mantissa. Exponent field 15
with mantissa 7 is NaN; overflow saturates to the largest finite value (448).
`f16`/`f32` follow IEEE 754, so overflow rounds to ±infinity (they have an
infinity; E4M3 does not).

In the Python implementation, fixed-width payloads are packed with `struct`
(little-endian prefix `<`); `f16` uses the `"e"` format. Python integers are
unbounded, so integer types are range-checked on write and after arithmetic.
Implementation: `src/creek/value.py`.

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
| `SET key value [TYPE t] [EX s\|PX ms] [NX\|XX]` | | `OK` / `null` |
| `GET key` | | `[type, value]` / `null` |
| `MGET key…` | | array of entries / `null` |
| `DEL key…` | | integer (deleted count) |
| `EXISTS key…` | | integer (existing count) |
| `TYPE key` | | type tag or `none` |
| `EXPIRE key seconds` | | `1` / `0` |
| `TTL key` | | seconds, `-1` (no TTL), `-2` (missing) |
| `KEYS pattern` | | array of keys |
| `SCAN cursor [MATCH p] [COUNT n]` | | `[nextCursor, [keys…]]` |
| `INCR key [BY n]` / `DECR key [BY n]` | | integer |
| `DBSIZE` | | integer |

Notes:

- `GET` returns a 2-element array `[type, value]`; `MGET` returns one such array per key, with
  `null` for keys that are missing or expired.
- `TYPE` returns a simple string: the type tag, or `none`.
- Keys are scoped to the client's namespace (`AGENTS.md` §8); a client cannot address another
  namespace.
- `SET` clears any existing TTL; `INCR`/`DECR` preserve it. `EX`/`PX` must be positive.
- `INCR`/`DECR` require an integer-typed value; a missing key starts as `i32` `0`. The result is
  range-checked against the value's type and the command fails on overflow.
- `SCAN` returns `[nextCursor, keys]`; the cursor is an index, `COUNT` defaults to `10`, `MATCH`
  filters the output, and a next cursor of `"0"` means iteration is complete.

### Pub/Sub

| Command | Args | Reply |
|---|---|---|
| `PUBLISH channel value [TYPE t] [RETAIN]` | | integer (receivers) |
| `SUBSCRIBE channel…` | | array of `[subscribe, channel, count]` |
| `PSUBSCRIBE pattern…` | | array of `[psubscribe, pattern, count]` |
| `UNSUBSCRIBE [channel…]` | | integer |
| `PUNSUBSCRIBE [pattern…]` | | integer |
| `PUBSUB CHANNELS [pattern]` | | array of channels |
| `PUBSUB NUMSUB [channel…]` | | `[channel, count, …]` |
| `PUBSUB NUMPAT` | | integer |

Notes:

- Channels are scoped to the client's namespace, like keys.
- A delivered frame is always `[message, channel, vtype, value]` (see §4.4); a pattern subscriber
  is not told which pattern matched.
- A subscriber that matches a channel *and* a pattern receives a single copy.
- `count` is the connection's total subscriptions (channels + patterns) after the call.
- `PUBSUB NUMSUB` counts direct channel subscribers only; `NUMPAT` counts distinct patterns.
- `RETAIN` stores the message as the channel's last value; a new matching subscriber receives it
  immediately after its subscribe confirmation.

### Admin

| Command | Args | Reply |
|---|---|---|
| `INFO [section]` | | bulk text |
| `FLUSHALL` | | `OK` (admin only) |
| `COMMAND [COUNT]` | | array of names / integer |

---

## 7. Patterns

Glob patterns are used by `KEYS`, `PUBSUB`, and `PSUBSCRIBE`:

- `*` matches any run of characters including delimiters; `?` matches one
  character. This matches Redis semantics, so `news.*` also matches
  `news.tech.redis`.

## 8. Errors

Errors are `ERR …` (or `WRONGPASS`, `NOPERM`, `NOAUTH`) and never terminate the
connection. Unknown commands return `ERR unknown command '<name>'`; arity
violations return `ERR wrong number of arguments for '<cmd>'`.

## 9. Versioning

- The protocol is currently unversioned. A future `HELLO` handshake will
  negotiate versions.
- Both transports track RESP2 framing; WebSocket adds message framing only.
- Storage formats will be versioned separately (`persist.SNAPSHOT_VERSION`, and the
  value codec's type byte); see `AGENTS.md` §10.
