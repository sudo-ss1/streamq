# Progress Log

## M0 — Scaffolding (2026-10-18)

**Built**
- `go.mod` (`github.com/sudo-ss1/streamq`, `go 1.26`), `.gitignore`.
- `Makefile`: `tools`, `gen`, `build`, `test`, `test-int`, `lint`, `fmt`, `clean`. `fuzz`, `bench`,
  `cluster-up/down` and `chaos` are stubs that say which milestone implements them.
- Pinned tools in `.tools/bin`: buf v1.73.0, golangci-lint v2.14.0, protoc-gen-go v1.36.12,
  protoc-gen-go-grpc v1.6.2.
- `.golangci.yml` (v2): standard linters plus contextcheck, copyloopvar, errorlint, forbidigo
  (bans `fmt.Print*` and `log.Print*/Fatal*/Panic*`), gochecknoglobals, gochecknoinits, gocritic,
  gosec (G115 excluded), misspell, noctx, nolintlint, revive (exported docs), sloglint,
  testifylint, thelper, unconvert, unparam. gofmt + goimports formatters. `gen/` excluded.
  `uniq-by-line: false`, so findings from different linters on one line all show up.
- `buf.yaml` / `buf.gen.yaml`: module `api`, output to `gen/` (see DECISIONS D1).
- Empty binaries `cmd/streamq-broker` and `cmd/streamqctl` (log "not implemented" via slog).
- `.github/workflows/ci.yml`: parallel lint (golangci-lint-action pinned to v2.14.0, plus a
  `make gen` + `git diff --exit-code gen/` check), test (`make test`) and build jobs.
- Renamed `claude.md` → `CLAUDE.md` and `docs/spec.md` → `docs/SPEC.md`.

**Decisions**: D1 (proto layout), D2 (project-local pinned tools).

**Verification**
- `make lint test build` passes (0 lint issues; no test files yet).
- In a throwaway copy: a sample proto ran through `make gen` into `gen/v1/` (package `streamqv1`)
  and compiled. A file with a global var, `init()`, `fmt.Println` and undocumented exports triggered
  gochecknoglobals, gochecknoinits, forbidigo and revive as expected.

**Known limitations**
- `make gen` is a no-op until M2 adds `.proto` files (buf fails on an empty module).
- The CI workflow has not run on GitHub yet (nothing has been pushed); the action tags are unverified.
- `make test-int` is a no-op until `test/integration/` exists (M2).
- Process switched to the autonomous plan → plan-reviewer → implement → code/failure-reviewer loop
  (CLAUDE.md). M0 plan approved by the user directly; see `docs/plans/M0.md`.
- No goleak `TestMain` yet; it arrives with the first package that has tests (M1).

## M1 — Storage engine (2026-10-18)

**Built**
- `internal/record`: SPEC §3.5 batch codec — `Batch` accessors, `ParseHeader`, `Validate`
  (canonical form only: minimal varints, zero attributes, exact framing, maxTimestamp check, CRC-32C),
  `SplitBatches`, `Records()` (aliases the buffer), `Builder`. Fuzz test `FuzzValidate` with a
  decode→rebuild round-trip property.
- `internal/storage`:
  - mmapped sparse offset and time indexes;
  - Kafka-format checkpoint files (offset and leader-epoch), written atomically;
  - segments with an atomically published size (lock-free readers);
  - `Log`: append with offset/epoch stamping, read bounded by maxOffset, roll by size, time
    (Kafka `timeWaitedForRoll`) or full index, flush policies with a sticky failure state,
    timestamp lookup, prefix-only time/size retention with `.deleted` renames;
  - recovery from the recovery point with CRC/contiguity validation, truncation and index rebuild;
  - `Manager`: recovery-point checkpoint, background flush/retention/checkpoint/delayed-delete
    passes, startup cleanup.
- `make fuzz` (30s FuzzValidate) and `make bench` (storage benchmarks).

**Decisions**: D3–D9.

**Process**: plan-reviewer REVISE → APPROVE (round 2); code-reviewer and failure-reviewer
CHANGES REQUIRED → APPROVE (round 2), with remaining MINORs fixed (see `docs/plans/M1.md`). Parallel split: record (implementer A),
indexes+checkpoints (implementer B), segment/log/recovery/manager (main session). Contract committed
first, A and B merged with `--no-ff`, C committed after tests passed on the merged tree.

**Verification**
- `make lint test` clean; `go test -race -count=3 ./internal/...` stable.
- `make fuzz`: ~1M execs in 30s, no failures.
- Benchmarks (6-core WSL2 machine, `t.TempDir()` on the WSL2 filesystem; indicative only):
  | benchmark | ns/op | throughput |
  |---|---|---|
  | Append 1×1 KiB, flush=never | 8.4 µs | 131 MB/s, 127k rec/s |
  | Append 1×1 KiB, flush=interval | 5.7 µs | 193 MB/s, 205k rec/s |
  | Append 100×1 KiB, flush=never | 687 µs | 151 MB/s, 166k rec/s |
  | Append 100×1 KiB, flush=interval | 940 µs | 110 MB/s, 120k rec/s |
  | Read (≈1 MiB) | 276 µs | 3.7 GB/s |

**Known limitations**
- `Validate` decodes every record on append (201 allocs per 100-record batch); optimise if M10
  benchmarks show it matters.
- Index contents below the recovery point are only sanity-checked (ordering, position < size), as in
  Kafka; a wrong-but-plausible entry would surface as a read error, not wrong data.
- Readers rely on the 60 s delete delay rather than refcounts (D7).
- MINOR review notes left for later:
  - `loadState` does not rebuild a missing or empty index for a trusted segment (reads then scan
    from position 0).
  - The active segment is fully CRC-validated on every start, even after a clean shutdown.
  - Sealed segments are fsynced again at startup.
  - `OffsetForTimestamp` skipping a segment retired mid-scan is not unit-tested (needs a race hook).
  - The parent-dir fsync on partition creation is not crash-testable in unit tests.
  - No fault-injection tests for an index-append failure or read I/O errors during recovery
    (both handled by code paths reviewed by hand).
  - `testHookAfterFsync` is a test-only field on `Log`.

## M2 — Single-node broker (2026-10-18)

**Built**
- `api/v1/streamq.proto` (`StreamQService`: CreateTopic, DeleteTopic, ListTopics, Metadata,
  Produce, Fetch, ListOffsets; `ErrorCode`, `Acks`) and `api/v1/internal.proto`
  (`MetadataCommand` with CreateTopic/DeleteTopic, versioned, with `topic_id`).
- `internal/config`:
  - SPEC §10 config with defaults → YAML (strict) → `STREAMQ_*` env → flags precedence;
  - validation;
  - per-topic config resolution;
  - `FlagKeys` (one broker flag per scalar key).
- `internal/metadata`: model, `Store` interface (atomic `Subscribe` handoff), deterministic
  `State.Apply`, `AssignReplicas`, topic-name rules. `internal/metadata/local` is a single-node
  store persisted as `metadata.json` (copy → persist → swap) with an owned notifier goroutine.
- `internal/partition`: leader replica — epoch checks (D11/D12), min.insync check for acks=all, HW
  (= LEO with ISR {self}), HW-change channel, ListOffsets (earliest/latest/timestamp).
- `internal/broker` (D14), the replica manager:
  - stray/topic-id directory cleanup (D17);
  - partitions opened from metadata;
  - topic create/delete handlers, with `WaitTopicReady`/`WaitTopicDeleted` (D18);
  - HW checkpoint (D15);
  - produce;
  - long-poll fetch (capture-then-read, `reflect.Select`, byte budget; D19);
  - ListOffsets.
- `internal/server`: thin gRPC handlers plus a single error-code mapping table.
- `internal/node`: process wiring with ordered shutdown. `cmd/streamq-broker`: cobra, signals,
  JSON logs. `deploy/single-node.yaml`.
- `internal/partitioner` (Kafka murmur2, verified against Kafka vectors). `cmd/streamqctl`: topics
  create/delete/list/describe, produce, consume.
- Storage: `Manager.DeleteLog` (checkpoint cleanup before rename, NAME_MAX-safe names) and
  `PartitionDirs`. `OffsetForTimestamp` returns the found record's timestamp.

**Decisions**: D10–D19. Lint config: G304/G204 path exclusions; `unparam` off in tests;
the `integration` build tag is linted.

**Process**:
- plan-reviewer REVISE → APPROVE (round 2);
- three implementers in parallel: config, metadata, partitioner + CLI;
- the main session did storage deletion, partition, broker, server, node, binaries and
  integration;
- code-reviewer CHANGES REQUIRED (2 MAJOR: checkpoint writer races; delete-dir names over
  NAME_MAX) → fixed (`41e531c`).

**Verification**
- `make lint test test-int` all green.
- `TestSingleNode100kKeyed` (§9): 100k records, 1000 keys, 6 partitions, batches of 500.
  Per-key order and partition placement verified, then the same data re-read after a graceful
  restart. Runs in about 3 s under `-race`.
- `TestKillAndRestartKeepsAcknowledgedRecords`: the real broker binary is SIGKILLed after 20k
  acked records and restarted. Every acked record is present and in order, and writes continue.

**Known limitations**
- go.mod says `go 1.26.0`: `go mod tidy` with the installed 1.27 toolchain rewrites `go 1.26` to
  that form. It is the same language version.
- `local.Store` detects re-entrant `Unsubscribe`/`Close` from a callback by parsing goroutine IDs
  from `runtime.Stack`. It works, but it's unusual.
- `TestShutdownWithIdleLongPollIsPrompt` can't prove the fetch was already waiting when Close
  started (no hook across packages); the broker-level `TestFetchWokenByClose` does prove it.
- Every topic create/delete wakes all parked fetches (correct, but extra reads). Revisit with
  leadership changes in M6.
- The checkpoint-vs-delete race tests depend on the scheduler; the lock fix is correct by
  inspection.

## M3 — Client library (2026-10-18)

**Built** (`pkg/client`)
- `Client`:
  - lazy gRPC connections;
  - metadata cache with age-based and error-driven invalidation;
  - coalesced refreshes in client-owned goroutines (immune to one caller's cancellation);
  - bootstrap failover;
  - sentinel errors per ErrorCode, with `IsRetriable` / `NeedsMetadataRefresh`;
  - full-jitter exponential backoff.
- `Producer` (main session):
  - per-partition accumulator with linger timers (injected clock);
  - murmur2 for keys, sticky partitioning with Kafka's `abortOnNewBatch`;
  - one in-flight batch per partition (D21);
  - retries with metadata refresh until `DeliveryTimeout`;
  - split and re-queue on MESSAGE_TOO_LARGE (D24), seal size capped by the topic's
    `max_message_bytes`;
  - `BufferMemory` backpressure, MaxBlock waits for unknown topics (D23);
  - `Flush`, and `Close` that drains or aborts.
- `Consumer` (implementer): manual assignment, auto offset reset, per-leader fetches, per-partition
  error backoff with rotation, Seek/SeekToBeginning/SeekToEnd/Position.
- `Admin` (implementer): create/delete/list/describe topics, ListOffsets, and transport-error retry
  semantics for create/delete.
- `record.Builder.AppendedSize`: exact size before append.
- `examples/{producer,consumer,admin}`, each with an end-to-end test against an in-process broker.
  `internal/testnode` is a test-only helper.
- `test/integration/client_test.go`: 10k keyed records over 4 partitions via Producer/Consumer.

**Decisions**: D20–D24.

**Process**:
- plan-reviewer REVISE → APPROVE (round 2); the split changed so the Producer is built by the main
  session;
- one implementer for Consumer+Admin;
- code-reviewer CHANGES REQUIRED (1 MAJOR: a coalesced refresh inherited the first caller's
  cancellation) → fixed (`f753ecf`).

**Verification**: `make lint test test-int` green; `pkg/client` stable over 6 race runs.

**Known limitations**
- Without idempotence (M7), a retry after REQUEST_TIMED_OUT, or after a transport error whose
  first attempt committed, can duplicate records (at-least-once).
- The consumer sends fetches to different leaders in parallel goroutines that are joined within
  Poll.
- `Client.Close` waits for any in-flight shared metadata refresh, up to RequestTimeout per
  candidate broker when brokers are unreachable.

## M4 — Consumer groups, single node (2026-10-18)

**Built**
- API: FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, CommitOffset (with
  per-partition results), FetchOffset, ListGroups, DescribeGroup. All additive; `buf breaking` is
  clean.
- Metadata FSM: the `CommitOffsetsCommand`, `State.Groups` persisted in `metadata.json` (D28), and
  topic deletion dropping its offsets (D27).
- `internal/group` (implementer):
  - coordinator with Kafka's state machine and server-side assignment (D25);
  - initial delay restarted per joiner, capped by the rebalance timeout;
  - parked members kept alive, session expiry, abandoned-join cleanup;
  - rebalances on topic create/delete;
  - commits accepted during PreparingRebalance;
  - range and roundrobin assignors.
- `pkg/client` GroupConsumer (implementer): join/sync/fetch-offset/seek, a heartbeat goroutine per
  generation, auto-commit (also before rejoin and on Close), coordinator rediscovery, and
  concurrency-safe accessors.
- `streamqctl groups list|describe|lag` (implementer).
- Server handlers, node wiring (the coordinator shuts down before GracefulStop), config key
  `group_initial_rebalance_delay_ms`.

**Decisions**: D25–D29.

**Process**:
- plan-reviewer REVISE ×2 → APPROVE (round 3);
- three implementers in parallel (coordinator, GroupConsumer, CLI);
- the main session did the FSM, server, node and integration.

**Verification**
- `make lint test test-int` green.
- `TestGroupsNoDoubleAssignmentAndKill`, `TestGroupsStaleGenerationCommitRejected` and
  `TestGroupsResumeFromCommittedOffsets` (§9) are stable over 3 race runs.

**Known limitations**
- `metadata.Snapshot()` deep-copies all committed offsets, and the coordinator calls it on every
  request for the controller check. Revisit in M5 with a single-group read if profiling shows a
  cost.
- Group operations call `store.Snapshot()` under the coordinator lock, and the local store holds
  its lock across `metadata.json` fsyncs in `Propose`. So every offset commit briefly stalls all
  group operations. Revisit with a cheaper controller-id/single-group read in M5.
- An empty group is dropped from coordinator memory, so its generation restarts at 1. Its
  committed offsets stay in metadata.
- `streamqctl groups lag` sends one ListOffsets to the bootstrap broker; with replication (M6)
  it must route by leader.

## M5 — Metadata Raft (2026-10-18)

**Built** (main session only; no implementer split, per CLAUDE.md and plan R8)
- FSM (`internal/metadata`):
  - broker registration (fenced until caught up; re-registration fences);
  - fence/unfence with broker epochs;
  - leader election (first unfenced ISR member in replica order; the last ISR member is kept;
    offline partitions are re-elected on unfence);
  - CreateTopic only over unfenced brokers;
  - term-bound leader commands (`controller_epoch` checked against the Raft log term).
- `metadata/raftstore`:
  - hashicorp/raft with raft-boltdb, file snapshots and a TCP transport over a pre-bound listener
    (accepted connections are tracked so Close is goroutine-clean);
  - static bootstrap;
  - deterministic FSM with versioned JSON snapshots and a `Restored` change carrying the snapshot;
  - follower→controller forwarding that retries only when the command was not appended and waits
    for local apply;
  - leader-only `ProposeAsLeader`/`ApplyForwarded`;
  - leadership fan-out through a Raft observer;
  - Barrier.
- `internal/controller`: activates on leadership after a Barrier, captures its term, gives
  untracked brokers a grace period, fences after the session timeout (once per epoch), and
  unfences when a heartbeat reports a processed index at or beyond the catch-up index.
- Internal gRPC API (`StreamQInternalService`: RegisterBroker, BrokerHeartbeat, ForwardMetadata)
  and a forwarder client.
- Broker:
  - monotonic `Partition.ApplyState` (leader, follower, and ISR-only updates);
  - `resync` on Restored;
  - a processed-index watermark reported in heartbeats.
- Coordinator: clears in-memory groups on a controller-epoch change; commits are leader-only and
  term-stamped.
- Node:
  - cluster mode: open Raft + controller + internal API, register, wait for catch-up, and only then
    open the broker (no stray-dir deletion on a stale FSM);
  - liveness loop with re-registration on STALE_BROKER_EPOCH;
  - refuses to mix metadata modes (D30).
- Server: CreateTopic over live brokers, forwarding-aware idempotence (same topic id), Metadata
  lists live brokers, `controller_id` hints.
- Client: Admin retries NOT_CONTROLLER.
- Config: Raft timing keys (D32). `testnode.StartCluster` starts in-process clusters on pre-bound
  ports.

**Decisions**: D30–D38.

**Process**:
- plan-reviewer REVISE ×2 → APPROVE (round 3);
- code-reviewer CHANGES REQUIRED (BLOCKER: the catch-up target used Raft's commit index while
  brokers report applied-change indexes, so brokers could stay fenced forever after a failover)
  and failure-reviewer CHANGES REQUIRED (same issue as MAJOR) → fixed in `bf279d0` → both APPROVE;
- remaining MINORs fixed.

**Verification**
- `make lint test test-int` green.
- `TestClusterMetadataRaft` (§9) is stable over 4 race runs. It checks:
  - topics created via each of 3 brokers are identical everywhere;
  - the controller is killed and a new one elected, with metadata intact;
  - the dead broker's partition is re-elected with a bumped epoch after the session timeout;
  - the dead broker restarts with its partition data intact.
- raftstore tests:
  - forwarding;
  - deterministic errors;
  - term-bound rejection after a leadership change;
  - failover and restart;
  - InstallSnapshot to a lagging follower;
  - FSM snapshot round trip.

**Known limitations**
- ~~No data replication yet~~: resolved in M6.
- The broker-side liveness loop uses `time.Ticker`, not an injected clock (integration-tested
  only; the override is recorded in `docs/plans/M5.md`).
- ~~Interim acks=all guard on RF>1~~: replaced in M6 by replication and the HW purgatory.
- The M6 plan draft landed inside the M5 fix commit `bf279d0` (no history rewrite; noted).
- The notifier is duplicated in `local` and `raftstore` (same design).

## M6 — Replication (2026-10-18)

**Built**
- `internal/storage`:
  - follower append (`AppendAsFollower`, `ErrOffsetMismatch`);
  - batch-wise `TruncateTo`, which deletes segments newest-first with a dir fsync after each, then
    cuts the containing segment and rescans it;
  - `TruncateFullyAndStartAt`;
  - a truncation generation, so a concurrent flush never raises the recovery point past a
    truncation;
  - retention is bounded by a per-partition limit (the HW).
- `internal/partition`:
  - a leader-epoch cache (`leader-epoch-checkpoint`);
  - leader side:
    - the HW is the minimum LEO over the **maximal ISR**, committed ∪ pending (KIP-497);
    - min.insync is checked against the committed ISR;
    - Kafka's caught-up rule;
    - expand needs LEO ≥ HW, LEO ≥ the start of the current epoch, and an unfenced broker;
    - shrink after `replica_lag_max_ms`;
    - one AlterISR in flight, with backoff; FENCED keeps the pending ISR until metadata arrives;
    - the epoch-tied acks=all purgatory (`WaitForCommit`): NOT_LEADER on leadership change,
      NOT_ENOUGH_REPLICAS after the append, REQUEST_TIMED_OUT;
    - replica fetch long-poll up to the LEO; `OffsetForLeaderEpoch`;
  - follower side: epoch-checked append, HW = min(leader HW, LEO), truncation lowers the HW;
  - the HW checkpoint is loaded on the first transition only.
- `internal/broker`:
  - transitions are broker-driven: Classify → stop the fetcher → become leader/follower → start
    the fetcher;
  - one fetcher goroutine per followed partition. It runs the KIP-279 truncation loop and makes the
    recovery-point and HW checkpoints durable before appending. It backs off on errors and never
    exits on its own;
  - the ISR loop (shrink, AlterISR sender, epoch-cache log-start sync);
  - `Produce` waits for acks=all (`timeout_ms`, default 30 s, capped below the RPC deadline);
  - leader-side `ReplicaFetch` with the broker-epoch check, and `OffsetForLeaderEpoch`;
  - fetchers are stopped before topic delete or Close;
  - `Partition.Remove` wakes waiters on delete and shutdown.
- `internal/server`:
  - internal handlers for ReplicaFetch, OffsetForLeaderEpoch and AlterISR;
  - `ReplicaClient`, which sends follower RPCs and AlterISR to the controller;
  - error codes for FENCED (metadata), INVALID_ISR, STALE_BROKER_EPOCH and REQUEST_TIMED_OUT.
- `internal/controller`: `AlterISR`, applied term-stamped.
- `internal/node`: wires the replica client, passes the broker epoch (R18), and attaches the broker
  to the internal API.

**Decisions**: D39–D47 (D46 refined after review).

**Process**
- plan-reviewer: REVISE ×2, then APPROVE.
- Round 1:
  - code-reviewer: CHANGES REQUIRED (BLOCKER: KIP-279 stall on an empty latest epoch).
  - failure-reviewer: APPROVE.
- Fixes, then round 2: both APPROVE.
- Details are in `docs/plans/M6.md`.

**Verification**: `make lint test test-int` green.
- `test/integration/replication_test.go` covers:
  - no acknowledged loss on leader kill: 550 acks, 150 of them after failover;
  - the old leader rejoining and all three replicas becoming byte-identical;
  - a restarted follower rejoining the ISR;
  - NOT_ENOUGH_REPLICAS with RF 3 / min.insync 3 when one follower is down, with nothing
    appended.
  - Stable over 4 race runs, together with `TestClusterMetadataRaft`.
- Unit tests:
  - divergent-follower truncation (follower epochs [(0,0),(2,10)] vs leader [(0,0),(1,8),(3,20)]);
  - an empty latest epoch;
  - NOT_LEADER retries;
  - a follower below the leader's log start;
  - goleak for delete-while-following and close-while-following;
  - maximal-ISR HW, AlterISR result handling, the caught-up rule and shrink, purgatory outcomes.

**Known limitations / deferred MINORs**
- `store.Snapshot()` (a full metadata clone) is called per replica fetch and per outgoing replica
  RPC. Needs a cheap broker lookup; M10 benchmarks will show the cost.
- A `BecomeLeader` disk failure (epoch checkpoint) leaves the partition offline until the next
  metadata change. Kafka would mark the log offline.
- The ISR loop ticks on a real `time.Ticker`; shrink timing uses the injected clock.
- The R2 purgatory test passes on the leadership check alone.
- No crash-injection tests for truncation; durability ordering is reasoned and reviewed.
- The leader is stopped gracefully in integration tests; hard kills and partitions come in M9.
- One RPC per partition per fetch (no per-broker fetch batching, D42).

## M7 — Idempotent producer (2026-10-18)

**Built**
- API:
  - public `InitProducer`;
  - internal `AllocateProducerIDs`;
  - `AllocateProducerIDsCommand`, which keeps `next_producer_id` and `producer_id_blocks` in FSM
    state (D51).
- `internal/controller`: `AllocateProducerIDs` under `allocMu`, with term re-checks (plan R13).
- `internal/broker`: the `ProducerIDs` allocator, which hands out IDs from 1000-ID blocks reserved
  from the controller, or from the local store in single-node mode.
- `internal/producerstate`:
  - per-partition state keeping the last 5 batches per producer;
  - `Check` (duplicate / in sequence / OUT_OF_ORDER_SEQUENCE / UNKNOWN_PRODUCER_ID, with
    wraparound);
  - CRC32C + JSON snapshots;
  - atomic `Rebuild`: prunes snapshots, skips corrupt ones, replays the rest, and retries once on
    a retention race;
  - expiry (D49).
- `internal/storage`: `ValidateForAppend` + `AppendValidated` (validate before any producer check),
  and `AppendInfo.RolledAt`.
- `internal/partition`:
  - **Leader:** check → append → update under `prodMu`. A duplicate returns the original offsets
    and is still subject to the acks=all wait.
  - **Followers:** update state as they replicate.
  - **Snapshots:** written on roll, only while the state is valid, and at the LEO on clean close.
  - **Rebuild:** after truncation, at open, and on retry. Invalid state refuses idempotent appends
    with NOT_LEADER.
- `pkg/client`:
  - idempotence is on by default with acks=all (`DisableIdempotence` turns it off);
  - the pid is fetched lazily under a leaf `pidMu`, with a generation counter;
  - the sequence is fixed on first send and kept for retries;
  - a split rolls back the sequence;
  - a final failure re-initializes the pid (D48).

**Decisions**: D48–D51.

**Process**
- plan-reviewer: REVISE, then APPROVE (round 2).
- Round 1:
  - code-reviewer: CHANGES REQUIRED (stale-valid state after a partial truncation; a flaky
    pre-existing admin test race);
  - failure-reviewer: CHANGES REQUIRED (invalid state persisted on roll).
- Fixes, then round 2: both APPROVE.
- Details are in `docs/plans/M7.md`.

**Verification**: `make lint test test-int` green. M7 + M6 integration tests passed 5/5 race runs.
- `test/integration/idempotent_test.go`:
  - **Lossy network:** 10% of responses lost after append and 5% of requests lost. All 3000
    records are acked and stored exactly once in per-key order. Verified to fail with idempotence
    disabled.
  - **Leader failover:** a deterministic duplicate probe hits the new leader, which returns the
    original offset with the LEO unchanged, while a lossy stream crosses the failover.
  - **Rolling restart:** the last 5 batches are still duplicates.
  - **SIGKILL:** recovery from a roll snapshot plus replay; duplicates are detected and the
    sequence continues.
- Unit tests: producerstate check/snapshot/rebuild tables; partition dedup, validation,
  truncation, invalid-state and recovery cases; FSM field propagation through the local store,
  raft FSM and snapshots; allocator concurrency; client lost-ack, re-init, split and
  stale-generation tests.

**Known limitations / deferred MINORs**
- One batch in flight per partition (D21/D48). Throughput will be measured in M10.
- Replay parses whole batches, not just headers. A long rebuild under `prodMu` stalls that
  partition, notably the first open of a pre-M7 data dir with no snapshots.
- Expiry uses client batch timestamps (Kafka parity, D49). Lowering `max_message_bytes` between a
  lost-response attempt and its retry makes the retry fail visibly instead of deduplicating it.
- No producer epoch fencing: the epoch is always 0, and a re-init gets a new pid instead (D48).
- No test interleaves two controllers' ID allocations; the term check is unit-tested.
- Fixed in passing: a data race in `pkg/client/admin_test.go` (an atomic counter).
