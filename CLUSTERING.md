# Clustering — design sketch

> **Status: design sketch, not implemented.** This describes how Creek would close the gap between
> its current M7 (read replicas + static sharding) and a Redis-Cluster-style system. It is future
> work; see `AGENTS.md` §14 and §17 (M8). Nothing here should shape the single-node code until it
> is built.

## 1. What exists today (M7)

| Piece | State |
|---|---|
| Hash slots | Redis-compatible CRC16, 16384, `{tag}` tags (`creek.cluster`) |
| Redirects | `MOVED`, `CROSSSLOT`, `CLUSTERDOWN`; the TCP client follows `MOVED` |
| Topology | **static**, from `--cluster-config` JSON |
| Replication | pull-based: `SYNC` snapshot + `REPLICATE <offset>` backlog |
| Failure detection | `master_link_down_since` in `INFO replication` |
| Failover | manual (`REPLICAOF NO ONE`) or opt-in timer; **no consensus** |
| Resharding | none |
| Cluster pub/sub | none (node-local) |

Missing, in dependency order: **config epochs → gossip membership → quorum failure detection →
replica election → resharding → cluster-wide pub/sub**.

## 2. Guiding constraints

- **Stay acyclic and flat.** `cluster.py` (slots + state) and `clusterbus.py` (transport, gossip,
  failure detection, election). No new third-party dependencies.
- **One codec for clients.** The cluster bus is a *separate listener* but reuses the RESP framing
  (`creek.protocol`); it carries an internal command namespace, never client commands.
- **Deterministic tests.** Distributed tests are the classic flake source. Time and the network must
  be injectable: a fake clock, an explicit `tick()`, and a `drop_link(a, b)` hook.
- **No strong consistency claims.** Replication stays asynchronous; failover can lose writes. Say
  so, loudly, in the docs.

## 3. Data model

```python
NodeId = str  # 40 hex chars, random at first boot, persisted in the data dir


@dataclass(slots=True)
class Node:
    id: NodeId
    addr: str  # client-facing host:port
    bus_addr: str  # cluster-bus host:port
    flags: set[str]  # master|replica|pfail|fail|handshake|noaddr
    config_epoch: int
    master_id: NodeId | None  # set for replicas
    slots: set[int]
    last_ping: float
    last_pong: float
    repl_offset: int
    link: str  # connected|disconnected


class ClusterState:
    my_id: NodeId
    current_epoch: int  # cluster-wide (max seen)
    nodes: dict[NodeId, Node]
    slots: list[NodeId | None]  # 16384 entries
    migrating: dict[int, NodeId]  # slot -> target (source side)
    importing: dict[int, NodeId]  # slot -> source (target side)
```

This replaces the immutable `Cluster` from M7. `key_slot` and `ClusterNode` stay.

**Config epochs** are the linchpin. Each node has a `config_epoch`; the cluster tracks a
`current_epoch`. Slot ownership is resolved by "highest `config_epoch` wins". A node bumps its
`config_epoch` (to `current_epoch + 1`) whenever it takes ownership of a slot — on election, on
reshard, or on first boot.

## 4. The cluster bus

A second listener (`--cluster-bus-addr`, default client port + 10000) speaking RESP arrays:

```
["CLUSTERMSG", <type>, <sender_id>, <current_epoch>, <config_epoch>, <flags>, <payload_json>]
```

Message types:

| Type | Purpose |
|---|---|
| `MEET` | Join request; receiver adds sender as `handshake` and replies `PING` |
| `PING` / `PONG` | Liveness + gossip (each carries a random gossip section) |
| `FAIL` | "Node X is FAIL" broadcast |
| `UPDATE` | Full slot map broadcast (sent when a receiver's map hash is stale) |
| `PUBLISH` | Cluster-wide pub/sub fanout (M8f) |
| `AUTH_REQ` / `AUTH_ACK` | Failover election votes |

The **gossip section** is a small array of `(node_id, addr, flags, ping_sent, pong_received)` for
randomly chosen peers — this is how knowledge propagates without a coordinator. Full slot maps are
*not* sent every ping: `PING`/`PONG` carry a `config_epoch` and a slot-map hash; a node that is
behind requests an `UPDATE`.

**Boot:** a node with an empty `nodes` table is a single-node cluster owning all 16384 slots.
`CLUSTER MEET host port` (admin) sends `MEET` to seed the graph.

## 5. Failure detection

- `--cluster-node-timeout` (default 15s). No `PONG` within it → mark the peer **PFAIL**.
- Gossip the PFAIL set (a bitmap in the message header).
- A node marks a peer **FAIL** when it *and* a majority of slot-owning primaries report PFAIL. It
  then broadcasts `FAIL`.
- `cluster_state` becomes `fail` when any slot is unserved (unless
  `--cluster-require-full-coverage no`), and the node rejects commands with `CLUSTERDOWN`.

This is a two-phase, quorum-based decision — the same shape as Redis, and it is what makes
automatic failover safe-ish without a coordinator.

## 6. Replica election (Raft-like, not Raft)

When a primary is marked FAIL, its replicas can promote:

1. After `failover_auth_timeout` (a delay), a replica becomes a candidate.
2. It sends `AUTH_REQ <epoch>` to every slot-owning primary.
3. A primary grants **one** vote per epoch — to the first candidate whose replication offset is
   fresh enough (within `cluster-replica-validity-factor`).
4. A candidate with a **majority** of primaries wins: it sets `config_epoch = current_epoch + 1`,
   takes over the failed primary's slots, and broadcasts the new map.
5. A returning old primary sees a higher `config_epoch` for its slots and demotes itself to a
   replica of the winner.

It is **election-only Raft**: there is no replicated log, so it is not linearizable. That is a
deliberate, documented trade-off — same as Redis.

`CLUSTER FAILOVER [FORCE|TAKEOVER]` is the manual path (`TAKEOVER` skips the vote).

## 7. Resharding and ASK

Move a slot without downtime:

1. Source: `CLUSTER SETSLOT <slot> MIGRATING <target>`.
   Target: `CLUSTER SETSLOT <slot> IMPORTING <source>`.
2. Enumerate keys with `CLUSTER GETKEYSINSLOT <slot> <count>`; move them with
   `MIGRATE <host> <port> <key>…`.
3. A request on the source: if the key exists, serve it; if not and the slot is `MIGRATING`,
   reply `-ASK <slot> <target>`.
4. A request on the target with `ASKING` first is served even though the target does not own the
   slot yet (one-shot).
5. Finish with `CLUSTER SETSLOT <slot> NODE <target>` on both nodes; each bumps its
   `config_epoch`.

`ASK` is one-shot and does **not** update the client's slot map — unlike `MOVED`.

## 8. Cluster-wide pub/sub

Two options, smallest first:

- **Broadcast (M8f-1):** `PUBLISH` is forwarded as a `PUBLISH` bus message to every node, which
  delivers to its local subscribers. Simple; O(N) traffic; matches classic Redis. Needs a message
  id + origin id for dedup, and a decision on what the returned receiver count means (aggregate).
- **Sharded (M8f-2):** the channel hashes to a slot; only the owning node serves
  `SPUBLISH`/`SSUBSCRIBE`. Scales, but requires a cluster-aware client to route by channel.

Creek's `RETAIN` complicates broadcast: retained values would need to converge cluster-wide. The
simplest rule is that `RETAIN` is only honored by the slot-owning node for sharded pub/sub, or is
replicated like any other write for broadcast.

## 9. Client and topology discovery

- `CLUSTER SHARDS` / `CLUSTER NODES` expose the live map for discovery.
- The cluster-aware client keeps a slot→node cache, refreshes on `MOVED`/`CLUSTERDOWN`, and
  implements `ASKING` for one-shot redirects. `creek.client` already follows `MOVED` over TCP;
  extend it for `ASK` and topology refresh.

## 10. Phased plan (proposed M8)

| Phase | Deliverable | Key tests |
|---|---|---|
| **M8a** | Config epochs + live slot map; `--cluster-config` becomes a seed | epoch conflict resolution; slot reassignment |
| **M8b** | Cluster bus + gossip; `CLUSTER MEET`, `CLUSTER NODES/SHARDS` | N-node convergence; gossip spread; late joiner |
| **M8c** | PFAIL/FAIL quorum; `cluster_state`; `CLUSTERDOWN` | quorum failure detection; minority cannot fail a node |
| **M8d** | Replica election + promotion + demotion; `CLUSTER FAILOVER` | majority election; split vote; old-primary demotion |
| **M8e** | Resharding (`MIGRATE`, IMPORTING/MIGRATING, `ASK`) + client `ASKING` | live slot move; `ASK` vs `MOVED` |
| **M8f** | Cluster-wide pub/sub (broadcast, then sharded) | cross-node delivery; retained convergence |

Each phase must keep the single-node tests green and the whole suite deterministic.

## 11. Testing strategy

- **In-process cluster harness** (`tests/cluster_harness.py`): spin up N `Server` instances on
  ephemeral ports in one event loop, wire them with `CLUSTER MEET`, and drive them.
- **Injectable time**: a fake clock so `cluster-node-timeout` and election timeouts can be advanced
  explicitly — no `sleep`-based tests.
- **Injectable network**: `drop_link(a, b)` / `partition({a, b}, {c})` hooks to simulate outages and
  partitions, and to assert split-brain behaviour deterministically.
- **Property checks**: at most one primary per slot; a majority is required to fail a node; a
  promoted replica's slots are consistent cluster-wide after convergence.

## 12. Risks and non-goals

- **Not linearizable.** Async replication means failover can lose acknowledged writes. `WAIT` (not
  implemented) would only narrow the window.
- **Flaky tests are the main risk.** Every timeout must be driven by the fake clock.
- **Split-brain is still possible** in a partition; the design only bounds it (majority quorum),
  it does not eliminate it.
- **No cross-slot transactions**, no `db > 0`, no bus TLS.
- **Broadcast pub/sub does not scale**; sharded pub/sub is the answer, at the cost of client
  complexity.

## 13. The durable-ack prerequisite

Clustering makes the reply-before-fsync gap (`AGENTS.md` §14) worse: a failover can promote a
replica that never received the last write. Before automatic failover is trusted, a `WAIT`-style
barrier (or group commit) should land so clients can ask for replication durability. Suggested
order: **group commit → `WAIT` → M8d**.
