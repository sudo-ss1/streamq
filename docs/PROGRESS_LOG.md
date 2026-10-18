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
