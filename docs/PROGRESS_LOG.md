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
