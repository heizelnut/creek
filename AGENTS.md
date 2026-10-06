# AGENTS.md — Creek

> Instructions and shared context for AI coding agents (and humans) working in this repository.
> Read this file completely before making changes. Update it whenever you make a decision that
> future work must respect. See also `PROTOCOL.md` for the wire specification.

## 1. What Creek is

Creek is a Redis-like, in-memory key–value store written in Go whose primary workload is
**typed publish/subscribe**. It keeps its working set in RAM and persists to disk for durability.

Core traits:

- **In-memory** hot path, **on-disk** durability (write-ahead log + snapshots).
- **Primitive, typed values** — fixed-width integers, floats, strings, and booleans (see §6).
- **Per-client index** — currently a naive, on-demand placeholder (see §8). The real-time
  auto-optimization (incremental trie + heuristics) is deliberately deferred.
- **Auth exists to identify the client**, so the correct per-client index and namespace are applied.
  It is not a general multi-tenant security boundary in v1 (see §9).
- **WebSocket is the native transport** (JSON envelope); plain **TCP** speaks a RESP subset.
- **TLS is out of scope** — terminate it at a reverse proxy in front of the TCP listener.
- Every entry (key/value or retained channel message) can carry one **metadata tag**.
- **Redis-like command surface** (see §7 and `PROTOCOL.md`).

Creek is *not* (v1 non-goals): a Redis drop-in replacement, a general document store, a SQL
database, a distributed/clustered system, or a full RBAC platform. Avoid scope creep in these
directions unless this file is updated first.

## 2. How to use this file (agents)

- Treat this as the source of truth for architecture and conventions. If code and this file
  disagree, fix the code or update this file — do not silently diverge.
- Before finishing a task: run `gofmt -l .`, `go vet ./...`, `go build ./...`, and
  `go test -race ./...`.
- Prefer the standard library. Any new third-party dependency must be justified in the PR/commit
  message and, if structural, recorded in §13.
- Keep public behavior changes documented here and in `PROTOCOL.md`.
- Anything marked **DECISION PENDING** in §15 is an open question: ask before assuming.

## 3. Quick start

```bash
# build
go build ./...

# run with defaults (WS + static :7100, TCP :7101), anonymous access
go run ./cmd/creek

# run with persistence and a token file
go run ./cmd/creek \
  --ws-addr :7100 \
  --tcp-addr :7101 \
  --web-dir ./web \
  --data-dir ./data \
  --auth-file ./auth.json \
  --fsync everysec \
  --snapshot-interval 5m

# tests
go test ./...
go test -race ./...

# browser load / performance tests
go run ./cmd/creek           # then open http://localhost:7100/
```

Module path: `github.com/heizelnut/creek`.

Flags (all in `cmd/creek/main.go`): `--ws-addr`, `--tcp-addr`, `--web-dir`, `--data-dir`,
`--auth-file`, `--fsync` (`always|everysec|no`), `--snapshot-interval`.

## 4. Architecture at a glance

```
             WebSocket (native)        TCP (RESP)
                   │                     │
                   └──────────┬──────────┘
                              ▼
                    ┌───────────────────────────────┐
                    │        Transport layer        │  internal/server
                    │  accept, frame, decode/encode │
                    └───────────────┬───────────────┘
                                    ▼
                    ┌───────────────────────────────┐
                    │      Protocol / command       │  internal/protocol, internal/command
                    │  parse → typed Command → exec │
                    └───────┬───────────────┬───────┘
                            ▼               ▼
              ┌──────────────────┐   ┌──────────────────┐
              │   Store (KV)     │   │   Pub/Sub bus    │
              │  keys+tags+TTL   │   │ subs + fanout    │
              └────────┬─────────┘   └────────┬─────────┘
                       └──────────┬───────────┘
                                  ▼
                    ┌───────────────────────────────┐
                    │   Index (per client, naive)   │  internal/index
                    │  on-demand tree over records  │
                    └───────────────┬───────────────┘
                                    ▼
                    ┌───────────────────────────────┐
                    │   Persistence (WAL+snapshot)  │  internal/persist
                    └───────────────────────────────┘
```

| Component | Package | Responsibility |
|---|---|---|
| Transports | `internal/server` | WS/TCP listeners, framing, session lifecycle |
| Protocol | `internal/protocol` | Codecs: RESP (TCP) and JSON envelope (WS) |
| Commands | `internal/command` | Registry, dispatch, ACLs, per-client indexes, WAL logging |
| Value model | `internal/value` | Primitive types, codecs, arithmetic |
| Patterns | `internal/pattern` | Glob and segment-aware matching |
| Store | `internal/store` | Single-map in-memory KV, TTL, owner metadata |
| Pub/Sub | `internal/pubsub` | Subscription registry, pattern matching, fanout, retained |
| Index | `internal/index` | Naive per-client record set; tree derived on demand |
| Auth | `internal/auth` | Client identification → identity + namespace |
| Persistence | `internal/persist` | WAL, snapshots, recovery |
| CLI | `cmd/creek` | Flags, wiring, lifecycle, signals, snapshots |
| Browser tests | `web/` | Load and performance pages (`bench.js`) |

## 5. Repository layout (current)

```
creek/
├── AGENTS.md
├── PROTOCOL.md
├── go.mod / go.sum
├── cmd/creek/main.go
├── internal/
│   ├── server/     server.go tcp.go ws.go server_test.go
│   ├── protocol/   protocol.go resp.go json.go protocol_test.go
│   ├── command/    engine.go commands_kv.go commands_pubsub.go
│   │               commands_index.go commands_admin.go command_test.go
│   ├── value/      value.go codec.go ops.go value_test.go
│   ├── pattern/    pattern.go pattern_test.go
│   ├── store/      store.go store_test.go
│   ├── pubsub/     bus.go bus_test.go
│   ├── index/      index.go index_test.go
│   ├── auth/       auth.go auth_test.go
│   └── persist/    wal.go snapshot.go persist_test.go
└── web/            index.html loadtest.html perf.html bench.js
```

`pkg/client` (a Go client library) is planned but not yet implemented.

## 6. Value model

Every value is a tagged primitive. The type is part of the value, not inferred at read time.

| Tag | Meaning | Width | Range / notes |
|---|---|---|---|
| `i4`  | signed int   | 4 bits  | −8 … 7 (stored masked in a byte) |
| `u4`  | unsigned int | 4 bits  | 0 … 15 (stored masked in a byte) |
| `i8` / `u8`   | signed/unsigned int | 8 | Go `int8` / `uint8` |
| `i16` / `u16` | signed/unsigned int | 16 | Go `int16` / `uint16` |
| `i32` / `u32` | signed/unsigned int | 32 | Go `int32` / `uint32` |
| `f8`  | float | 8 | OCP E4M3 (only variant; NaN at `0x7F`, max finite 448) |
| `f16` | float | 16 | IEEE 754 binary16 |
| `f32` | float | 32 | Go `float32` |
| `f64` | float | 64 | Go `float64` |
| `str` | UTF-8 string | var | length-prefixed, max 512 KiB |
| `bool`| boolean | 1 bit | encoded as `u8` 0/1 on the wire |

Encoding rules:

- Wire/storage format is **little-endian** for fixed-width numerics.
- Canonical binary layout: `1 type byte || payload` (see `PROTOCOL.md` §5.1). `Value` implements
  `encoding.BinaryMarshaler`/`BinaryUnmarshaler`, which gob uses for persistence.
- `i4`/`u4` are logically 4-bit but stored as a masked byte in v1 (see §15).
- `f16` and `f8` are implemented in `internal/value/codec.go` (no external dependency).
- Values are immutable once stored; mutations replace the value and bump the index.

Every stored entry may also carry exactly one **tag**: an opaque, short UTF-8 string used for
metadata, filtering, and indexing. `tag` is indexed and included in heuristic clustering.
Entries also carry an **owner** (namespace) so per-client indexes can be rebuilt after restart.

## 7. Command surface (Redis-like)

Case-insensitive command names, whitespace-separated args. Typed values are given explicitly with
`TYPE`; if omitted, the server defaults to `str`. The full grammar and examples are in
`PROTOCOL.md`.

### Connection / auth
| Command | Notes |
|---|---|
| `AUTH <token>` | Identify client; selects namespace + index. |
| `PING [msg]` / `ECHO msg` | Health check / echo. |
| `QUIT` | Close connection. |

### Key–value
| Command | Notes |
|---|---|
| `SET key value [TYPE t] [TAG tag] [EX s\|PX ms] [NX\|XX]` | Upsert. |
| `GET key` / `MGET key…` | Returns `[type, value, tag]`. |
| `DEL key…` / `EXISTS key…` | Delete / test. |
| `TYPE key` | Returns type tag or `none`. |
| `TAG key` / `SETTAG key tag` | Get / set the metadata tag. |
| `EXPIRE key seconds` / `TTL key` | TTL management. |
| `KEYS pattern` / `SCAN cursor [MATCH p] [COUNT n]` | Enumeration. |
| `INCR key [BY n]` / `DECR key [BY n]` | Integer types only; preserves type; range-checked. |
| `DBSIZE` | Live key count. |

### Pub/Sub
| Command | Notes |
|---|---|
| `PUBLISH channel value [TYPE t] [TAG tag] [RETAIN]` | Publish; `RETAIN` persists last value. |
| `SUBSCRIBE channel…` / `PSUBSCRIBE pattern…` | Subscribe (literal / glob). |
| `UNSUBSCRIBE [channel…]` / `PUNSUBSCRIBE [pattern…]` | Unsubscribe. |
| `PUBSUB CHANNELS\|NUMSUB\|NUMPAT [arg]` | Introspection. |

### Index / admin
| Command | Notes |
|---|---|
| `INDEX TREE [prefix] [DEPTH n]` | Discovered tree for the calling client (JSON). |
| `INDEX STATS` | Node counts, type/tag histograms, heuristic scores (JSON). |
| `INDEX FIND <pattern>` | Segment-aware query against the index. |
| `INDEX REBUILD` | Re-run heuristics from the store (admin only). |
| `INDEX SEPARATOR [byte]` | Get / override the segment delimiter. |
| `INFO [section]`, `FLUSHALL`, `COMMAND [COUNT]` | Operational. |

Notes:
- The command registry is data-driven: `register(name, arity, scope, fn)` in `internal/command`.
  Add commands there, never with ad-hoc string matching in the transport layer.
- New commands must be reflected in this table and in `PROTOCOL.md`.

## 8. Indexing heuristics (the heart of Creek)

**Status: naive placeholder.** This is the *unoptimized* version. `internal/index` keeps a flat
`map[string]Record` per client and derives everything on demand. There is no incremental trie, no
maintained histograms, and no real-time learning beyond one cheap delimiter heuristic. The full
heuristic implementation is preserved in git tag `optimized-baseline` and is intended to replace
this later (see §15 and §18).

**Goal (deferred):** without user-declared schemas, discover the tree-like structure inside each
client's keys and use it for fast pattern subscription, enumeration, and queries.

**Per-client scope.** Each identity owns an index keyed by namespace (falling back to ID). The
store is shared, but every entry records its `Owner`, so indexes are reconstructible via
`Engine.RebuildIndexes`.

**What exists today:**

- **Delimiter detection.** `DetectSeparator` counts candidate separators (`.`, `:`, `/`, `-`, `_`)
  across a client's keys and picks the dominant one (requires evidence in ≥2 keys, defaults to `.`).
  This is the only retained heuristic.
- **`Find`** is a linear scan using segment-aware `pattern.MatchSegments`.
- **`Tree`** and **`Stats`** rebuild a tree by scanning and re-splitting every key on each call.
- Homogeneity (≥90% one type) and wide nodes (>10 000 children) are computed on demand, not
  maintained incrementally.

**Deferred (tracked in §15):** incremental segment trie, per-node type/tag histograms, chain
compaction, and the rest of the heuristic signal set. None of this is wired into the write path.

**Lifecycle:** the index is never a source of truth and is rebuilt with `INDEX REBUILD` or
`Engine.RebuildIndexes`.

**Matching:** `*`/`?` match within a segment; `**`/`>` match zero or more segments (see
`internal/pattern`).

## 9. Auth model

Auth exists to **recognize the client and select its index/namespace**, not to be a full security
boundary in v1.

- `AUTH <token>` resolves a token to `Identity{ID, Namespace, ACL}`.
- Token store: a JSON file (`--auth-file`, see `internal/auth`), loaded with `auth.LoadFile`.
  Tokens are compared in constant time.
- When `--auth-file` is omitted, anonymous access is allowed with a full ACL.
- ACLs are coarse: `read`, `write`, `subscribe`, `admin` (or `all`/`*`).
- Every command handler receives the resolved identity; the index and store owner are derived from
  it. Never trust a client-supplied namespace.

Auth file schema:

```json
{
  "allow_anonymous": false,
  "clients": [
    {"id": "sensor-a", "namespace": "sensors", "token": "t1", "acl": ["read", "subscribe"]},
    {"id": "admin", "token": "t2", "acl": ["all"]}
  ]
}
```

## 10. Transports and framing

- **WebSocket (native):** text frames carrying JSON command envelopes. Responses are
  `{"ok":true,"result":…}`; streamed pub/sub messages are `{"type":"message",…}`.
- **TCP:** RESP2 subset (arrays of bulk strings or inline commands); redis-cli compatible for
  scalar commands.
- **TLS:** intentionally **out of scope**. Run a reverse proxy in front of the TCP listener.
- All transports funnel into the same `protocol.Command` model; codecs only translate.
- Backpressure: pub/sub delivery is non-blocking; a subscriber whose buffer is full or whose
  `Send` fails is dropped rather than blocking the bus.
- WebSocket read limit: 1 MiB; static files are served from `--web-dir` at `/`.

## 11. Persistence

- **WAL (write-ahead log):** every mutation (SET/DEL/TAG/EXPIRE/FLUSH and `PUBLISH … RETAIN`) is
  appended as a length- and CRC32-prefixed gob record (`internal/persist/wal.go`).
  `--fsync=always|everysec|no` (default `everysec`).
- **Snapshot:** periodic full dump (gob, versioned header) written atomically via temp + rename.
  Triggered by `--snapshot-interval` and once on shutdown.
- **Recovery:** `Engine.Recover` loads the snapshot then replays the WAL. Expired entries
  (absolute deadline in the past) are dropped, not resurrected. Indexes are rebuilt from owners.
- Replay of the full WAL after a snapshot is idempotent; the WAL is **not yet compacted/rotated**
  after a snapshot (see §15).
- Non-retained pub/sub messages are not persisted.
- On-disk formats are versioned (`persist.SnapshotVersion`, value type byte). Bump the version and
  provide a migration/reader for old files.

## 12. Concurrency

- Store is a **single map behind one `sync.RWMutex`** — deliberately unoptimized. Lock striping
  and sharding are future work if the workload needs it.
- Per-client indexes have their own `RWMutex`; the engine guards the index map with `imu`.
- Pub/Sub fanout is a linear scan: it copies the target list under a lock, then sends outside it;
  failed subscribers are pruned. No subscription index yet.
- Connection writers (TCP/WS) serialize writes with a per-connection mutex because
  `coder/websocket` permits only one concurrent writer.
- Run everything under `-race`; a race is a release blocker.

## 13. Dependencies

Prefer the standard library. Current external dependencies:

| Concern | Package | Status |
|---|---|---|
| WebSocket | `github.com/coder/websocket` | adopted |
| Argon2id token hashing | `golang.org/x/crypto/argon2` | not yet (see §15) |

`f16`/`f8` are implemented in-tree. Do not add an ORM, a web framework, or a logging framework;
`log/slog` is the logging standard.

## 14. Testing and quality

- Table-driven unit tests for `value`, `pattern`, `protocol`, `index`, `store`, and `pubsub`.
- Codec golden/round-trip tests in `internal/protocol` and `internal/value`.
- Command-level tests in `internal/command` (ACLs, typed values, TTL, pub/sub, index, recovery).
- Integration tests in `internal/server` start WS + TCP listeners on ephemeral ports
  (`Server.Listen` binds, `Server.Serve` runs, `Server.Ready` signals).
- Persistence tests: WAL round-trip, truncated-tail tolerance, reset, snapshot round-trip.
- **Browser load and performance tests** live in `web/` and are served by the WS listener:
  - `web/index.html` — landing page.
  - `web/loadtest.html` — many concurrent WS clients, live ops/s chart, latency percentiles.
  - `web/perf.html` — per-command latency/throughput (SET/GET/PUBLISH/INDEX), bar chart.
  - `web/bench.js` — dependency-free helpers (client, percentiles, canvas charts).
  These pages must stay dependency-free (no CDNs) so they work offline.
- Required checks before merge: `gofmt -l .` clean, `go vet ./...`, `go build ./...`,
  `go test -race ./...`.

## 15. Open questions (DECISION PENDING)

1. **Real-time auto-optimization (deferred)** — the incremental trie + heuristics live in git tag
   `optimized-baseline`; restore and wire into the write path only after measuring.
2. **Store lock striping / sharding** — the store is a single mutex-guarded map today.
3. **Nibble packing** for `i4`/`u4` — currently byte-masked; pack two per byte?
4. **`f8` variants** — E4M3 only today; add E5M2?
5. **Chain compaction** — part of the deferred trie work.
6. **WAL compaction/rotation** — snapshots do not truncate the WAL; add prefix truncation.
7. **Heuristic thresholds** — 90% homogeneity / 10 000 wide-node are constants in `internal/index`;
   make configurable when the optimized index returns?
8. **Argon2id** — token file currently stores plaintext tokens; hash at rest.
9. **Multi-tag** — one tag per entry today; support a tag set?
10. **Anonymous default** — currently allowed when no `--auth-file` is given.
11. **Go client library** (`pkg/client`) — not yet implemented.
12. **Clustering** — explicitly out of scope for v1.

## 16. Coding conventions

- Go 1.26+. Format with `gofmt`; imports grouped stdlib / external / internal.
- Errors: wrap with `%w`, return early, no panics in library code.
- Exported types and funcs have doc comments starting with the name.
- Contexts flow through I/O and long operations; never store a `context.Context` in a struct.
- Logging via `log/slog` with structured fields; no `fmt.Println` outside `cmd/`.
- Keep packages cohesive and dependency-acyclic: `command` may import `store`/`index`/`pubsub`/
  `persist`/`protocol`, but those must not import `command` or `server`.
- No global mutable state outside the command registry created at init.

## 17. Glossary

| Term | Meaning |
|---|---|
| **Creek** | This project: typed, indexed, in-memory KV + pub/sub. |
| **Client identity** | The authenticated principal that owns a namespace and an index. |
| **Index** | Per-client naive record set; tree/statistics derived on demand. |
| **Tag** | Single optional metadata string attached to an entry. |
| **Owner** | Namespace recorded on an entry so indexes can be rebuilt. |
| **Segment** | One component of a key after splitting on the detected delimiter. |
| **Retained message** | A published value stored as the channel's last value and persisted. |
| **WAL** | Write-ahead log used for crash recovery. |

## 18. Roadmap

- **M0 — Skeleton:** module, flags, listeners, command registry. ✅
- **M1 — Value + store:** all primitive types, tags, TTL, simple single-map store, tests. ✅
- **M2 — Pub/Sub:** subscribe/publish, pattern matching, linear fanout, retained. ✅
- **M3 — Index (placeholder):** flat record set, delimiter detection, `INDEX TREE`/`STATS`/`FIND`
  derived on demand. ✅ *(optimized trie deferred)*
- **M4 — Persistence:** WAL + snapshot + recovery tests. ✅ (WAL compaction pending)
- **M5 — Auth:** token file, per-client namespaces, coarse ACLs. ✅ (Argon2id pending)
- **M6 — Hardening:** browser load/perf pages ✅; fuzz tests, Go client, docs pending.
- **M7 — Real-time auto-optimization (deferred):** restore the incremental trie + heuristics from
  git tag `optimized-baseline`, wire it into the write path, and add lock striping to the store.
  Measure first; only then optimize.

---

*Keep this file current — it is the project's shared memory.*
