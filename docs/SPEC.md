# streamq — Specification

Referenced from `CLAUDE.md`. Section numbers are stable; refer to them in plans and commits.

---

## 1. Goals

- Durable, append-only, partitioned log storage on disk.
- Producers publish to topics; records route to partitions by key; per-key order is preserved.
- Consumers read by offset, standalone or in consumer groups.
- A static cluster of 3–5 brokers with replication and automatic leader failover.
- At-least-once delivery by default; idempotent producers make retries duplicate-free.
- Observable, and shown correct under failure (chaos + linearizability tests).

---

## 2. Architecture Overview

```
             ┌──────────────────────────── Cluster ────────────────────────────┐
 Producer ──▶│ Broker 1 (controller)        Broker 2              Broker 3      │
 Consumer ◀──│  ├─ Raft voter (metadata) ◀────▶ Raft voter ◀────▶ Raft voter    │
             │  ├─ Partition replicas (leader/follower, pull replication)     │
             │  └─ gRPC: client API + internal API                            │
             └─────────────────────────────────────────────────────────────────┘
```

### 2.1 Control plane — metadata
- **Every broker is a Raft voter.** Membership is static, read from config. On first start, every
  node calls `BootstrapCluster` with the identical server list (ignore `raft.ErrCantBootstrap`).
- The Raft leader is the **controller**.
- The FSM (in `internal/metadata`) holds: registered brokers and their fenced/alive state, topics
  and configs, per-partition `{replicas, leader, leader_epoch, isr, partition_epoch}`, consumer
  group committed offsets, and the next producer ID block.
- Every broker applies the FSM locally, so every broker has a full, consistent metadata view and
  reacts to changes (become leader / become follower / stop replica) via an FSM change notifier.
- FSM snapshots are implemented (`raft.FSMSnapshot`) and tested.
- All FSM commands are versioned protobuf messages; FSM `Apply` must be deterministic
  (no time.Now, no randomness — timestamps come in the command).

### 2.2 Metadata Store abstraction
Define `metadata.Store` (read methods + `Propose(ctx, cmd)`) in **M2**. Implementations:
- `metadata/local`: single-node, in-memory with JSON-file persistence (M2–M4).
- `metadata/raftstore`: Raft-backed (M5). Brokers and groups must not care which one is in use.

### 2.3 Broker liveness
- Brokers send `BrokerHeartbeat` to the controller every `broker.heartbeat.interval.ms` (2s).
- If none arrives for `broker.session.timeout.ms` (9s), the controller proposes `FenceBroker`.
  Fencing removes the broker from all ISRs and triggers leader election for partitions it led.
- A restarted broker re-registers and is unfenced once caught up on metadata.

### 2.4 Data plane — partitions
Covered in §4. Records never go through Raft.

---

## 3. Storage (per partition)

### 3.1 Files
Directory `<data_dir>/<topic>-<partition>/`, zero-padded 20-digit base offsets:
- `<base>.log` — record batches
- `<base>.index` — sparse offset index, entries `(relative_offset uint32, position uint32)` every
  `index.interval.bytes` (4 KiB); memory-mapped; preallocated and trimmed on roll.
- `<base>.timeindex` — `(timestamp int64, relative_offset uint32)`, appended when a batch's
  `maxTimestamp` exceeds the last entry, at the same interval. Used by `ListOffsets` by timestamp.
- `<base>.snapshot` — producer-state snapshot written on segment roll (§6).
- `leader-epoch-checkpoint` — lines of `epoch startOffset` (§4.4).

Per data dir: `replication-offset-checkpoint` (HW per partition, written every 5s and on shutdown)
and `recovery-point-offset-checkpoint` (last flushed offset per partition).

### 3.2 Behavior
- **Single writer** per partition (append under a mutex). Readers are lock-free and read only up to
  an atomically published position.
- Roll when size would exceed `segment.bytes` (default 64 MiB, max 1 GiB) or age > `segment.ms`.
- Fsync policy: `always` | `interval` (default, every `flush.ms`) | `never`.
- Retention: delete whole non-active segments older than `retention.ms` or while total size
  exceeds `retention.bytes`.

### 3.3 Safe deletion
Remove the segment from the in-memory segment list first, rename its files with a `.deleted`
suffix, and unlink after `file.delete.delay.ms` (60s). Readers that already hold an open segment
can finish. On startup, delete any leftover `.deleted` files.

### 3.4 Recovery
On startup, for each partition: load checkpoints, then scan from the recovery point — validate
each batch CRC and length, truncate the log at the first invalid or partial batch, rebuild the
indexes for affected segments, reload producer state (§6), and load the leader-epoch cache.

### 3.5 Batch format (on disk == on wire, big-endian)
Batch header (61 bytes):
```
baseOffset       int64
batchLength      int32   // bytes after this field
leaderEpoch      int32
magic            int8    // = 1
crc              uint32  // CRC-32C over attributes..end of batch
attributes       int16   // reserved, 0
lastOffsetDelta  int32
firstTimestamp   int64   // unix ms
maxTimestamp     int64
producerID       int64   // -1 if not idempotent
producerEpoch    int16   // 0 for now
baseSequence     int32   // -1 if not idempotent
recordCount      int32
```
Each record (varints are zig-zag):
```
length          varint
attributes      int8
timestampDelta  varlong   // from firstTimestamp
offsetDelta     varint    // from baseOffset
keyLength       varint    // -1 = null
key             bytes
valueLength     varint    // -1 = null
value           bytes
headerCount     varint
headers         [keyLength varint, key, valueLength varint, value]...
```
The leader assigns `baseOffset` and `leaderEpoch` on append; clients send 0 for both.
`internal/record` has round-trip tests, a fuzz test on decoding (must never panic), and
rejects batches over `max.message.bytes` (1 MiB default).

---

## 4. Replication (data plane)

### 4.1 Roles
Each partition has a leader and followers. Followers **pull** with the internal `ReplicaFetch` RPC.

### 4.2 Offsets
- **LEO** — next offset to be written on a replica.
- **HW** — min LEO across the ISR, computed by the leader; followers learn it from fetch responses.
- Consumer `Fetch` returns only data below HW. `ReplicaFetch` returns data up to LEO.

### 4.3 ISR management
- The leader tracks each follower's last fetch offset and `lastCaughtUpTime`.
- **Shrink:** follower not caught up within `replica.lag.max.ms` (10s).
- **Expand:** follower's LEO ≥ current HW.
- The leader requests changes via the internal `AlterISR` RPC to the controller, which applies them
  through Raft only if `leader_epoch` and `partition_epoch` match (else `FENCED_LEADER_EPOCH`).
  The leader keeps using the old ISR until the change is committed.

### 4.4 Leader election & truncation
- On leader failure or fencing, the controller picks the first live replica in the ISR (in replica
  order), increments `leader_epoch`, and commits via Raft.
- **Unclean election is disabled:** if no ISR member is alive, the partition is offline
  (`NOT_LEADER` until a member returns).
- A new leader appends `(newEpoch, LEO)` to its leader-epoch cache.
- A follower starting to follow a leader calls `OffsetForLeaderEpoch(lastEpoch)`; the leader replies
  with the end offset of that epoch; the follower truncates to `min(own LEO, returned offset)`,
  then begins fetching. This prevents log divergence.
- Any partition-targeted request with a stale `leader_epoch` gets `FENCED_LEADER_EPOCH`; a request
  to a non-leader gets `NOT_LEADER` plus the current leader ID if known.

### 4.5 Produce acknowledgments
- `acks=0`: no response wait. `acks=1`: after the leader append.
- `acks=all`: if ISR size < `min.insync.replicas`, reject **before** append with
  `NOT_ENOUGH_REPLICAS`. Otherwise respond once HW passes the batch's last offset, or with
  `REQUEST_TIMED_OUT` after the request timeout (the data may still commit; idempotence handles retries).
- Waiting requests are parked in a purgatory keyed by partition and released on HW change — no polling.

### 4.6 Controlled shutdown (M8)
On SIGTERM a broker asks the controller to move leadership off it before stopping its server.

---

## 5. Consumer Groups

- **The controller is the coordinator for all groups.** Clients call `FindCoordinator`; non-coordinators
  answer `NOT_COORDINATOR`.
- Membership state lives in memory on the coordinator. After controller failover, members receive
  `NOT_COORDINATOR`/`UNKNOWN_MEMBER_ID`, re-discover, and rejoin. Committed offsets survive in the FSM.
- Protocol: `JoinGroup` → the coordinator waits up to `rebalance.timeout.ms` for members, bumps the
  **generation ID**, and computes assignments (range assignor; round-robin as a second assignor)
  → `SyncGroup` returns each member's assignment → `Heartbeat` every `heartbeat.interval.ms`.
- Missed heartbeats beyond `session.timeout.ms` or a `LeaveGroup` trigger a rebalance;
  heartbeats during a rebalance return `REBALANCE_IN_PROGRESS`.
- `CommitOffset` with a stale generation → `ILLEGAL_GENERATION`. Commits go through the metadata
  store; the client auto-commits in batches every `auto.commit.interval.ms` (5s) by default.
- Consumers start from the committed offset, else `auto.offset.reset` (`earliest` | `latest`).

---

## 6. Idempotent Producer

- `InitProducer` returns a unique `producerID`. The controller reserves IDs in blocks of 1000 via Raft.
- The client assigns a monotonically increasing `baseSequence` per partition per batch and keeps at
  most 5 in-flight batches per partition.
- The leader keeps, per `(producerID, partition)`, metadata for the last 5 appended batches:
  - Exact duplicate → success with the original base offset, no append.
  - Sequence gap → `OUT_OF_ORDER_SEQUENCE`.
- Followers update producer state as they replicate, so a new leader already has it.
- Producer state is snapshotted to `<base>.snapshot` on segment roll and rebuilt on recovery by
  loading the latest snapshot and replaying later batches.
- Producer IDs idle longer than `producer.id.expiration.ms` (7 days) are evicted.

---

## 7. APIs

### 7.1 Public (`api/v1/streamq.proto`)
```
service StreamQ {
  // Admin / metadata
  rpc CreateTopic(...)     // name, partitions, replication_factor, configs map
  rpc DeleteTopic(...)
  rpc ListTopics(...)
  rpc Metadata(...)        // brokers, per-partition leader, leader_epoch, replicas, isr

  // Produce
  rpc InitProducer(...)
  rpc Produce(...)         // per-partition record batches, acks, timeout_ms

  // Consume
  rpc Fetch(...)           // per-partition offset + leader_epoch, max_bytes, min_bytes, max_wait_ms
  rpc ListOffsets(...)     // earliest | latest | timestamp

  // Groups
  rpc FindCoordinator(...)
  rpc JoinGroup(...)  rpc SyncGroup(...)  rpc Heartbeat(...)  rpc LeaveGroup(...)
  rpc CommitOffset(...)  rpc FetchOffset(...)
}
```
Stretch (only after M10): `ProduceStream` bidirectional streaming.

`Fetch` long-polls: return when `min_bytes` are available below HW or `max_wait_ms` elapses.
It may return a partial final batch boundary only if a single batch exceeds `max_bytes`.

### 7.2 Internal (`api/v1/internal.proto`)
`RegisterBroker`, `BrokerHeartbeat`, `ReplicaFetch` (includes `replica_id`, `leader_epoch`, offsets),
`OffsetForLeaderEpoch`, `AlterISR`, `ControlledShutdown`. Raft uses its own TCP transport.

### 7.3 Error codes (enum in responses, per partition where relevant)
`NONE, UNKNOWN_TOPIC_OR_PARTITION, TOPIC_ALREADY_EXISTS, INVALID_REPLICATION_FACTOR, NOT_LEADER,
FENCED_LEADER_EPOCH, OFFSET_OUT_OF_RANGE, NOT_ENOUGH_REPLICAS, REQUEST_TIMED_OUT, CORRUPT_MESSAGE,
MESSAGE_TOO_LARGE, OUT_OF_ORDER_SEQUENCE, UNKNOWN_PRODUCER_ID, ILLEGAL_GENERATION,
REBALANCE_IN_PROGRESS, UNKNOWN_MEMBER_ID, NOT_COORDINATOR, NOT_CONTROLLER`.
gRPC status codes are used only for transport-level failures and invalid requests.

### 7.4 Partitioning
- Key present → `murmur2(key) & 0x7fffffff % numPartitions`, implemented once in a shared package
  used by client and tests.
- Key absent → sticky partitioner: the same partition until the batch is sent, then the next.

---

## 8. Requirements

### Functional
- **FR1** Create/delete/list topics; RF must be ≤ registered brokers.
- **FR2** Produce batches with `acks=0|1|all`; return the base offset per batch.
- **FR3** Per-key ordering within a partition.
- **FR4** Long-poll fetch; consumers never see data above HW.
- **FR5** `ListOffsets` for earliest, latest, and by timestamp.
- **FR6** Consumer groups per §5.
- **FR7** Idempotent producer per §6.
- **FR8** No loss of `acks=all` records while ISR ≥ `min.insync.replicas` and at most RF−1 brokers fail.
- **FR9** Broker restart recovers all partitions and rejoins.
- **FR10** Time- and size-based retention.
- **FR11** `streamqctl`: topics (create/delete/list/describe), produce, consume, groups
  (list/describe/lag), `cluster status`.
- **FR12** Client library: metadata cache + refresh on `NOT_LEADER`/`FENCED_LEADER_EPOCH`,
  retries with exponential backoff + jitter, batching (`linger.ms`, `batch.size`), `Close` flushes.

### Non-functional (measure and document actual numbers; targets are goals, not gates)
- **NFR1** 3 brokers, RF=3, `acks=all`, 1 KiB records, batched: ≥ 50k msgs/s aggregate on a laptop.
- **NFR2** Single broker, `acks=1`, 1 KiB, 5k msgs/s: p99 produce latency < 10 ms.
- **NFR3** `go test -race ./...` and `golangci-lint` clean.
- **NFR4** Graceful shutdown on SIGTERM: stop accepting, drain in-flight requests, flush, checkpoint.
- **NFR5** No goroutine leaks (`goleak`).
- **NFR6** Prometheus metrics: request rate and latency histograms per API, bytes in/out, LEO/HW per
  partition, ISR size, under-replicated and offline partition counts, consumer lag per group,
  Raft state and last-contact.
- **NFR7** Structured `slog` logs with broker/topic/partition fields.

---

## 9. Milestones (each must pass its criteria before moving on)

**M0 — Scaffolding.** go.mod, Makefile, buf config, `.golangci.yml`, GitHub Actions (lint, test -race, build), empty binaries, `docs/DECISIONS.md`.
✅ `make lint test build` passes.

**M1 — Storage engine.** `internal/record`, `internal/storage` (segments, both indexes, log, roll, retention + safe delete, checkpoints, recovery).
✅ Append/read/roll/retention tests; recovery test that truncates a file mid-batch and corrupts a CRC; fuzz test; append benchmark; reads concurrent with appends under `-race`.

**M2 — Single-node broker.** `metadata.Store` + local store, gRPC server, CreateTopic/Metadata/Produce/Fetch (long-poll)/ListOffsets, `streamqctl` topics/produce/consume, config loading + validation.
✅ Integration test: 100k keyed records across 6 partitions; consume all; per-key order verified; restart broker and re-read everything.

**M3 — Client library.** Producer (batching, partitioner, retries), Consumer, Admin, `examples/`.
✅ Examples run end-to-end; tests for retry on injected errors and for `Close` flushing.

**M4 — Consumer groups (single node).** Coordinator, both assignors, generations, auto-commit, `auto.offset.reset`, `streamqctl groups`.
✅ 3 consumers on 6 partitions: no double assignment; killing one rebalances within session timeout; stale-generation commit rejected; restarted group resumes from committed offsets.

**M5 — Metadata Raft.** `raftstore`, FSM + snapshots, broker registration/heartbeat/fencing, controller, `NOT_CONTROLLER` forwarding hints.
✅ In-process 3-broker test: topic created via any broker is visible everywhere; controller killed → new controller elected and metadata intact; FSM snapshot/restore round-trip test.

**M6 — Replication.** Follower fetcher, LEO/HW, ISR shrink/expand via `AlterISR`, `acks=all` purgatory, leader election, leader-epoch cache + `OffsetForLeaderEpoch` truncation.
✅ Kill leader during continuous `acks=all` produce → zero acknowledged records lost; restarted follower catches up and rejoins ISR; test with a deliberately divergent follower log proves truncation; `NOT_ENOUGH_REPLICAS` when ISR < min.

**M7 — Idempotent producer.** `InitProducer`, ID blocks, sequence checks, follower-maintained state, snapshots.
✅ Producer with injected network failures and retries → no duplicates and no reordering; state survives leader failover and broker restart.

**M8 — Observability & ops.** Metrics, logs, controlled + graceful shutdown, `streamqctl cluster status`, Docker Compose cluster with Prometheus + a Grafana dashboard JSON.

**M9 — Correctness & chaos.** Porcupine model for single-partition produce(acks=all)/fetch; chaos scripts: kill -9 random brokers, Toxiproxy partitions and latency, full-disk simulation.
✅ `test/chaos/REPORT.md` documents scenarios, invariants checked, and results.

**M10 — Benchmarks & docs.** `bench/` load tool reporting throughput and p50/p99/p999; README with architecture diagram, results table, and a design trade-offs section linking `docs/DECISIONS.md`.

---

## 10. Configuration (broker)

```yaml
broker_id: 1
listen_addr: ":9092"           # client gRPC
internal_addr: ":9093"         # broker-to-broker gRPC
raft_addr: ":9094"
advertised_host: "broker1"
cluster:                       # identical on every broker
  - {id: 1, raft: "broker1:9094", internal: "broker1:9093", client: "broker1:9092"}
  - {id: 2, raft: "broker2:9094", internal: "broker2:9093", client: "broker2:9092"}
  - {id: 3, raft: "broker3:9094", internal: "broker3:9093", client: "broker3:9092"}
data_dir: "/var/lib/streamq"
metrics_addr: ":9100"
topic_defaults:
  partitions: 3
  replication_factor: 3
  min_insync_replicas: 2
  segment_bytes: 67108864      # max 1073741824
  segment_ms: 604800000
  retention_ms: 604800000
  retention_bytes: -1
  max_message_bytes: 1048576
  flush_policy: interval        # always | interval | never
  flush_ms: 1000
replica_lag_max_ms: 10000
broker_heartbeat_interval_ms: 2000
broker_session_timeout_ms: 9000
group_session_timeout_ms: 10000
group_rebalance_timeout_ms: 30000
file_delete_delay_ms: 60000
producer_id_expiration_ms: 604800000
```
Precedence: flags > env (`STREAMQ_<UPPER_SNAKE_KEY>`) > file > defaults. Validate on startup and fail fast with a clear message.