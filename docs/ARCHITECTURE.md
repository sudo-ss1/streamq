# streamq architecture

This document shows how the pieces of a broker fit together and how the main flows run. The
rules behind each flow are in [SPEC.md](SPEC.md) (section numbers below) and
[DECISIONS.md](DECISIONS.md) (D-numbers). The overview diagram and quick start are in the
[README](../README.md).

- [Components and packages](#components-and-packages)
- [Produce with acks=all](#produce-with-acksall)
- [Leader failover and KIP-279 truncation](#leader-failover-and-kip-279-truncation)
- [Consumer group rebalance](#consumer-group-rebalance)
- [Controlled shutdown](#controlled-shutdown)
- [On-disk layout](#on-disk-layout)

## Components and packages

A broker process (`internal/node`) runs three listeners:

| Listener | Config key | Serves |
|---|---|---|
| client gRPC | `listen_addr` (`:9092`) | `StreamQService` (`api/v1/streamq.proto`): topics, metadata, produce, fetch, offsets, groups |
| internal gRPC | `internal_addr` (`:9093`) | `StreamQInternalService` (`api/v1/internal.proto`): RegisterBroker, BrokerHeartbeat, ForwardMetadata, ReplicaFetch, OffsetForLeaderEpoch, AlterISR, AllocateProducerIDs, ControlledShutdown |
| Raft transport | `raft_addr` (`:9094`) | hashicorp/raft TCP transport (metadata only) |

plus an HTTP `/metrics` endpoint on `metrics_addr` (`:9100`). In single-node mode (empty
`cluster`) there is no internal API and no Raft; metadata lives in a local JSON store
([D30](DECISIONS.md#d30-two-metadata-modes-no-migration-m5)).

Arrows point from caller to callee.

```mermaid
flowchart TB
    subgraph cmd["Binaries"]
        BROKERBIN["cmd/streamq-broker"]
        CTL["cmd/streamqctl"]
    end
    CLIENT["pkg/client<br/>Client, Producer, Consumer,<br/>GroupConsumer, Admin"]

    subgraph proc["internal/node: one broker process"]
        SERVER["internal/server<br/>gRPC handlers, error mapping,<br/>ReplicaClient, Forwarder"]
        BROKER["internal/broker<br/>replica manager, fetchers,<br/>ISR loop, producer-ID allocator"]
        PART["internal/partition<br/>replica state, LEO/HW, ISR,<br/>leader-epoch cache, acks=all wait"]
        PSTATE["internal/producerstate<br/>sequences, snapshots"]
        STORAGE["internal/storage<br/>Manager, Log, segments,<br/>indexes, checkpoints, recovery"]
        RECORD["internal/record<br/>batch codec, CRC-32C"]
        GROUP["internal/group<br/>coordinator, assignors"]
        CONTROLLER["internal/controller<br/>liveness, fencing, AlterISR,<br/>controlled shutdown, PID blocks"]
        META["internal/metadata<br/>Store interface, FSM state"]
        LOCAL["metadata/local<br/>metadata.json"]
        RAFT["metadata/raftstore<br/>hashicorp/raft + raft-boltdb"]
        METRICS["internal/metrics<br/>own Prometheus registry"]
        CONFIG["internal/config"]
    end

    BROKERBIN --> CONFIG
    BROKERBIN --> SERVER
    CTL -->|"gRPC"| SERVER
    CLIENT -->|"gRPC"| SERVER
    SERVER --> BROKER
    SERVER --> GROUP
    SERVER --> CONTROLLER
    SERVER --> META
    BROKER --> PART
    BROKER --> STORAGE
    BROKER --> META
    PART --> PSTATE
    PART --> STORAGE
    STORAGE --> RECORD
    GROUP --> META
    CONTROLLER --> RAFT
    META --- LOCAL
    META --- RAFT
    METRICS -.->|"scrape-time collectors"| BROKER
    METRICS -.-> RAFT
```

Responsibilities, from the bottom up:

- **`internal/record`** encodes and validates batches (SPEC §3.5); on-disk bytes equal wire bytes.
- **`internal/storage`** owns files: segments with an atomically published size so reads are
  lock-free, mmapped indexes, roll, retention with delayed delete, follower truncation, recovery,
  and the per-data-dir `Manager` with its background flush, retention and checkpoint passes.
- **`internal/partition`** is one replica. As leader it checks leader epochs and `min.insync`,
  runs producer-state checks before appending, tracks follower LEOs, computes the HW over the
  maximal ISR ([D45](DECISIONS.md#d45-maximal-isr-m6-kip-497)) and parks `acks=all` requests
  until the HW passes them ([D43](DECISIONS.md#d43-acksall-purgatory-m6)). As follower it
  appends fetched batches and keeps HW = min(leader HW, own LEO)
  ([D40](DECISIONS.md#d40-high-watermark-rules-m6)).
- **`internal/broker`** subscribes to metadata changes and turns them into leader/follower
  transitions. It runs one fetcher goroutine per followed partition and one ISR loop
  ([D42](DECISIONS.md#d42-replication-concurrency-model-m6)), serves produce, long-poll fetch,
  ListOffsets and the leader side of ReplicaFetch and OffsetForLeaderEpoch.
- **`internal/metadata`** defines `Store` (reads, `Subscribe`, `Propose`) and the deterministic
  FSM state: brokers, topics, partitions (`replicas`, `leader`, `leader_epoch`, `isr`,
  `partition_epoch`), group offsets and producer-ID blocks. `local` and `raftstore` implement it.
- **`internal/controller`** acts only on the Raft leader: it registers brokers, fences them after
  `broker_session_timeout_ms` without a heartbeat, unfences them once caught up
  ([D34](DECISIONS.md#d34-fencing-semantics-m5), [D35](DECISIONS.md#d35-registration-and-catch-up-m5)),
  applies AlterISR, allocates producer-ID blocks and handles controlled shutdown. Its commands
  carry the Raft term they were issued in, so a deposed controller can never write
  ([D33](DECISIONS.md#d33-forwarding-and-leader-only-writes-m5)).
- **`internal/group`** is the consumer group coordinator, running on the controller.
- **`internal/server`** is a thin gRPC layer: validate, delegate, and map sentinel errors to
  `ErrorCode`s in one table.

## Produce with acks=all

SPEC §4.2, §4.5; [D43](DECISIONS.md#d43-acksall-purgatory-m6),
[D44](DECISIONS.md#d44-not_enough_replicas-after-append-m6),
[D45](DECISIONS.md#d45-maximal-isr-m6-kip-497). Partition `orders-0` has replicas {1, 2, 3},
leader 1 in epoch 5, ISR {1, 2, 3} and `min.insync.replicas` 2.

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer (pkg/client)
    participant L as Broker 1 (leader)
    participant F2 as Broker 2 (follower fetcher)
    participant F3 as Broker 3 (follower fetcher)

    P->>L: Produce(orders-0, batch pid=7 seq=40, leader_epoch=5, acks=all)
    L->>L: check leader epoch, committed ISR size >= min.insync
    L->>L: producer-state check (duplicate, gap or in sequence)
    L->>L: append at LEO 100..109, stamp epoch 5 (LEO now 110)
    Note over L: handler parks on the HW and leadership channels (purgatory)

    F2->>L: ReplicaFetch(replica_id=2, leader_epoch=5, fetch_offset=100)
    L-->>F2: records 100..109, high_watermark=100
    F2->>F2: append as follower, update producer state (LEO 110)
    F3->>L: ReplicaFetch(replica_id=3, leader_epoch=5, fetch_offset=100)
    L-->>F3: records 100..109, high_watermark=100
    F3->>F3: append as follower (LEO 110)

    F2->>L: ReplicaFetch(fetch_offset=110)
    L->>L: follower 2 LEO = 110, HW = min LEO over maximal ISR = 100
    F3->>L: ReplicaFetch(fetch_offset=110)
    L->>L: follower 3 LEO = 110, HW advances to 110
    Note over L: HW change wakes the parked request

    alt still leader in epoch 5 and HW passed offset 109
        L-->>P: base_offset=100, NONE
    else committed ISR dropped below min.insync
        L-->>P: NOT_ENOUGH_REPLICAS (retriable, data may commit)
    else leadership moved
        L-->>P: NOT_LEADER
    else timeout_ms elapsed
        L-->>P: REQUEST_TIMED_OUT
    end
    Note over F2,F3: followers learn HW 110 in their next fetch responses
```

Points to note:
- If the committed ISR is already below `min.insync.replicas`, step 2 returns
  `NOT_ENOUGH_REPLICAS` and nothing is appended.
- A duplicate idempotent batch is not appended again: the leader returns the original offsets,
  and the response still waits for the HW like any other `acks=all` batch.
- Consumers' `Fetch` reads only below the HW; `ReplicaFetch` reads up to the leader's LEO.
- A lagging follower stays in the ISR until it has not caught up for `replica_lag_max_ms`; the
  leader then sends AlterISR to the controller and keeps counting it towards the HW until the
  shrink is committed ([D41](DECISIONS.md#d41-isr-expansion-rule-m6),
  [D45](DECISIONS.md#d45-maximal-isr-m6-kip-497)).

## Leader failover and KIP-279 truncation

SPEC §2.3, §4.4; [D34](DECISIONS.md#d34-fencing-semantics-m5),
[D35](DECISIONS.md#d35-registration-and-catch-up-m5),
[D46](DECISIONS.md#d46-kip-279-follower-truncation-m6). Broker 1 leads `orders-0` in epoch 5 and
crashes with records at offsets 110..119 that only it has. Broker 3 is the controller.

```mermaid
sequenceDiagram
    autonumber
    participant B1 as Broker 1 (old leader)
    participant C as Broker 3 (controller)
    participant R as Raft FSM (all brokers)
    participant B2 as Broker 2
    participant B3 as Broker 3 (follower)
    participant P as Producer

    Note over B1: crashes, LEO 120, HW 110
    C->>C: no BrokerHeartbeat from 1 for broker_session_timeout_ms (9 s)
    C->>R: Propose FenceBroker(1, broker epoch)
    R->>R: remove 1 from ISRs, elect first unfenced ISR member (2), leader_epoch 6
    R-->>B2: change, leader of orders-0 in epoch 6
    R-->>B3: change, follow broker 2 in epoch 6
    B2->>B2: stop fetcher, become leader, epoch cache += (6, LEO 110)
    B3->>B3: stop fetcher for broker 1

    B3->>B2: OffsetForLeaderEpoch(current_leader_epoch=6, requested_epoch=5)
    B2-->>B3: leader_epoch=5, end_offset=110
    B3->>B3: truncate to min(own LEO 110, 110), checkpoint recovery point and HW
    B3->>B2: ReplicaFetch(leader_epoch=6, fetch_offset=110)

    P->>B1: Produce (retry)
    B1--xP: transport error
    P->>B2: Metadata(orders)
    B2-->>P: leader 2, leader_epoch 6
    P->>B2: Produce(same pid and sequence, leader_epoch=6)
    B2-->>P: base offset (a duplicate returns the original offset)

    Note over B1: restarts, recovers its log from the recovery point
    B1->>C: RegisterBroker
    C->>R: Propose RegisterBroker(1), fenced, new broker epoch
    R-->>B1: catch up on metadata, follow broker 2 in epoch 6
    B1->>B2: OffsetForLeaderEpoch(current_leader_epoch=6, requested_epoch=5)
    B2-->>B1: leader_epoch=5, end_offset=110
    B1->>B1: truncate 110..119 (never acknowledged, above the HW)
    B1->>B2: ReplicaFetch(fetch_offset=110)
    B1->>C: BrokerHeartbeat(processed index >= registration index)
    C->>R: Propose UnfenceBroker(1)
    B2->>C: AlterISR(add 1) once LEO(1) >= HW and >= start of epoch 6
    C->>R: Propose AlterISR {2, 3, 1}, applied if leader_epoch and partition_epoch match
```

The truncation loop handles more cases than this one ([D46](DECISIONS.md#d46-kip-279-follower-truncation-m6)):
if the leader only knows an older epoch, the follower truncates to its own end of that epoch and
asks again; without epoch information on either side it falls back to its HW; below the leader's
log start it restarts empty at the leader's log start. Truncation makes the lowered recovery
point and HW durable before the next append. Followers truncate whole segments immediately
([D39](DECISIONS.md#d39-follower-truncation-deletes-segments-immediately-m6)).

If no ISR member is alive, the partition gets leader -1 (offline) and stays so until an ISR
member returns; there is no unclean election. Any request carrying an older `leader_epoch` gets
`FENCED_LEADER_EPOCH`.

## Consumer group rebalance

SPEC §5; [D25](DECISIONS.md#d25-consumer-group-protocol-server-side-assignment-with-kafkas-state-machine-m4).
Consumer A is the only member of group `g` (generation 1, all partitions of `orders`); consumer
B joins. The coordinator is the controller. Assignment is computed by the coordinator, not by a
group leader, and `JoinGroup` blocks until the rebalance completes.

```mermaid
sequenceDiagram
    autonumber
    participant A as Consumer A (member a)
    participant B as Consumer B (new)
    participant K as Coordinator (controller)
    participant S as Metadata store (Raft)

    B->>K: FindCoordinator(g)
    K-->>B: coordinator = controller id
    B->>K: JoinGroup(g, member_id="", topics=[orders], assignors=[range])
    Note over K: state PreparingRebalance, B parked in Join
    A->>K: Heartbeat(g, a, generation=1)
    K-->>A: REBALANCE_IN_PROGRESS
    A->>K: CommitOffset(g, a, generation=1, positions)
    K->>S: Propose CommitOffsets (term-stamped)
    K-->>A: NONE
    A->>K: JoinGroup(g, member_id=a)
    Note over K: all members rejoined (or rebalance timeout reached)
    K->>K: generation 2, choose assignor, compute assignment
    K-->>A: generation=2, member a, assignment orders-0..2
    K-->>B: generation=2, member b, assignment orders-3..5
    A->>K: SyncGroup(g, a, generation=2)
    K-->>A: assignment orders-0..2
    B->>K: SyncGroup(g, b, generation=2)
    K-->>B: assignment orders-3..5
    B->>K: FetchOffset(g, orders-3..5)
    K-->>B: committed offsets (else auto offset reset)
    loop every heartbeat interval
        A->>K: Heartbeat(g, a, generation=2)
        B->>K: Heartbeat(g, b, generation=2)
    end
    Note over A,B: fetch from partition leaders, auto-commit every 5 s
    A->>K: CommitOffset(g, a, generation=1, ...) from a stale generation
    K-->>A: ILLEGAL_GENERATION
```

- A member whose heartbeats stop for longer than its session timeout, or that sends
  `LeaveGroup`, triggers the next rebalance.
- A new group waits `group_initial_rebalance_delay_ms` (3 s) for more members before its first
  rebalance.
- Membership is held in memory on the coordinator; committed offsets are in the metadata store.
  After a controller failover, members get `NOT_COORDINATOR` or `UNKNOWN_MEMBER_ID`, find the new
  coordinator and rejoin.

## Controlled shutdown

SPEC §4.6, NFR4; [D53](DECISIONS.md#d53-controlled-shutdown-m8). Broker 2 receives SIGTERM while
leading some partitions; broker 1 is the controller. Steps 2–6 share one deadline,
`controlled_shutdown_timeout_ms` (default 30 s).

```mermaid
sequenceDiagram
    autonumber
    participant OS as SIGTERM
    participant N as Broker 2 (Node.Close)
    participant C as Broker 1 (controller)
    participant R as Raft FSM (all brokers)
    participant O as Other replicas

    OS->>N: SIGTERM
    N->>N: stop the metrics server
    N->>N: stop and join the liveness loop (no more heartbeats)
    loop until it succeeds, retrying each 2 s attempt
        N->>C: ControlledShutdown(broker_id=2, broker_epoch)
    end
    C->>C: mark broker 2 shuttingDown (never unfence it)
    C->>R: Propose FenceBroker(2)
    R->>R: remove 2 from ISRs, elect new leaders, leader_epoch++
    C-->>N: NONE
    R-->>O: become leader or follow the new leaders
    R-->>N: change, no longer leader
    N->>N: wait until no local partition is led here
    opt broker 2 is the Raft leader
        N->>R: TransferLeadership
    end
    N->>N: stop accepting work, gRPC GracefulStop (up to 10 s)
    N->>N: close the group coordinator
    N->>N: broker Close: stop fetchers, producer snapshots, HW checkpoint
    N->>N: storage Close: flush logs, recovery-point checkpoint
    N->>N: close the metadata store
```

- The request is skipped once no controller has been visible for twice the Raft election
  timeout, because without a quorum nothing can move.
- Failures only log: shutdown never blocks beyond the deadline.
- A heartbeat already in flight to a newly elected controller can briefly unfence the stopping
  broker; it self-heals by the session timeout.
- In compose the budget is 10 s and `stop_grace_period` is 30 s
  ([D54](DECISIONS.md#d54-compose-topology-and-shutdown-budget-m8)).

## On-disk layout

SPEC §3.1; [D17](DECISIONS.md#d17-topic-ids-partitionmetadata-and-stray-dir-cleanup-m2-on-disk-addition-approved-by-the-plan-reviewer),
[D30](DECISIONS.md#d30-two-metadata-modes-no-migration-m5),
[D37](DECISIONS.md#d37-on-disk-additions-and-go-hclog-m5-approved-by-the-plan-reviewer),
[D50](DECISIONS.md#d50-producer-state-snapshots-m7).

```
<data_dir>/
├── recovery-point-offset-checkpoint     last flushed offset per partition
├── replication-offset-checkpoint        high watermark per partition (every 5 s and on shutdown)
├── metadata.json                        single-node mode only: local metadata store
├── raft/                                cluster mode only
│   ├── raft.db                          BoltDB Raft log and stable store
│   └── snapshots/                       FSM snapshots (versioned JSON)
└── orders-0/                            one directory per partition: <topic>-<partition>
    ├── partition.metadata               "version: 0" and the topic id
    ├── leader-epoch-checkpoint          "epoch startOffset" lines
    ├── 00000000000000000000.log         record batches; file name = base offset (20 digits)
    ├── 00000000000000000000.index       sparse offset index (relative offset, position)
    ├── 00000000000000000000.timeindex   time index (timestamp, relative offset)
    ├── 00000000000000052311.snapshot    producer state up to offset 52311 (written on roll)
    ├── 00000000000000052311.log         the active segment
    ├── 00000000000000052311.index
    └── 00000000000000052311.timeindex
```

- **Checkpoint files** use Kafka's text format: a version line (`0`), an entry count, then one
  entry per line (`topic partition offset`, or `epoch startOffset`). They are written to a
  `.tmp` file, fsynced, renamed over the old file, and the directory is fsynced.
- **Indexes** are memory-mapped, preallocated and trimmed on roll. Offset index entries are 8
  bytes (`uint32` relative offset, `uint32` position), so a segment is at most 1 GiB; time index
  entries are 12 bytes ([D3](DECISIONS.md#d3-index-file-format-and-validity-m1)). Indexes of
  segments at or after the recovery point are rebuilt from the log on startup.
- **Producer snapshots** (`<offset>.snapshot`, a CRC32C plus a JSON body) cover every batch below
  their offset. They are written on segment roll and at the LEO on clean shutdown; recovery loads
  the newest valid one and replays later batches.
- **Deleted segments** are renamed with a `.deleted` suffix and unlinked after
  `file_delete_delay_ms` (60 s); leftovers are removed at startup
  ([D7](DECISIONS.md#d7-delayed-deletion-without-reader-refcounts-sticky-io-failure-m1)).
  Deleted topics and stray partition directories are renamed `<dir>.<id|stray>.<random>.delete`
  and removed in the background.
- **Mode guard:** a broker refuses to start in one metadata mode when the other mode's metadata
  (`metadata.json` or `raft/`) is present
  ([D30](DECISIONS.md#d30-two-metadata-modes-no-migration-m5)).
