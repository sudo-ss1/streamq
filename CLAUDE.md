# CLAUDE.md — streamq

streamq is a distributed, partitioned, replicated, log-based message queue in Go (a small Kafka).

**The full specification is in `docs/SPEC.md`.** Read the sections relevant to the current
milestone before writing code. This file holds only what you need in every session.

---

## Project Rules (always apply)

- Build **one milestone at a time** in the order listed in `docs/SPEC.md` §9. Do not start the
  next milestone until the current one's acceptance criteria pass.
- Before coding a milestone, present a short plan (files, types, key functions, test cases) and
  **wait for my approval**. After the milestone passes and is committed, stop. Never start the
  next milestone unless I ask.
- If the same failure persists after 3 genuinely different fix attempts, stop and explain the
  problem instead of continuing to guess.
- Keep `docs/PROGRESS_LOG.md` updated after every milestone (what was built, decisions, known
  limitations, test/bench results) so work can resume cleanly in a fresh session.
- Always run `make test` (includes `-race`) and `make lint` before saying a step is done.
  Never mark work done with failing or skipped tests.
- If the spec doesn't answer a design question, choose the Kafka-like behavior and add a
  one-paragraph entry to `docs/DECISIONS.md`.
- **Ask before:** adding a dependency not listed below, changing the on-disk format after M1,
  making a breaking change to `api/v1/*.proto`, or deleting/rewriting existing tests.
- At the end of each milestone, tick it in the Progress list below.

## Git Rules (always apply)

- **Never run `git push`** (or anything that pushes: `gh pr create`, `gh repo sync`, force-push,
  pushing tags) until I explicitly say so in chat. Local commits are fine.
- Commit messages must **not** contain `Co-Authored-By: Claude`, "Generated with Claude Code", or
  any other AI attribution trailer or footer. Write plain commit messages only.
- Use Conventional Commits, e.g. `feat(storage): add sparse offset index`,
  `fix(partition): truncate follower log on epoch change`. One logical change per commit.
- Do not rewrite history (`rebase`, `commit --amend`, `reset --hard`) on commits I've already seen
  without asking.
- `GITHUB_PAT` is set in the shell environment. Never print, echo, or log it; never write it into any
  file, git remote URL, config, script, or commit; and don't use it unless I ask you to.

## Non-goals (do NOT build)

Cross-partition transactions, end-to-end exactly-once processing, authN/authZ, TLS, schema
registry, log compaction, tiered storage, compression codecs, dynamic cluster membership,
and implementing Raft from scratch.

---

## Tech Stack (approved dependencies)

- Go 1.23+, `log/slog`, `context` everywhere
- gRPC + Protocol Buffers, codegen with `buf`
- `github.com/hashicorp/raft` + `github.com/hashicorp/raft-boltdb/v2` (metadata only)
- `github.com/spf13/cobra` (CLI), `gopkg.in/yaml.v3` (config)
- `github.com/prometheus/client_golang`
- `golang.org/x/sync/errgroup`
- Tests: `github.com/stretchr/testify/require`, `go.uber.org/goleak`, `github.com/anishathalye/porcupine`
- Chaos: Docker Compose + Toxiproxy

---

## Repository Layout

```
api/v1/streamq.proto      public gRPC API
api/v1/internal.proto     broker-to-broker RPCs
gen/                      generated code — NEVER edit by hand
cmd/streamq-broker/       broker binary
cmd/streamqctl/           CLI
internal/record/          batch + record encoding, CRC, varints
internal/storage/         segment, indexes, log, retention, recovery, checkpoints
internal/partition/       replica state, LEO/HW, ISR, follower fetcher, leader-epoch cache
internal/metadata/        Store interface, local store, Raft store + FSM + snapshots
internal/controller/      broker liveness, partition leader election, ISR changes
internal/group/           consumer group coordinator + assignors
internal/producerstate/   idempotence tracking + snapshots
internal/server/          gRPC handlers (thin: validate → delegate → map errors)
internal/config/  internal/metrics/
pkg/client/               Go client: Producer, Consumer, GroupConsumer, Admin
examples/                 runnable client examples
test/integration/  test/linearizability/  test/chaos/
deploy/docker-compose.yml 3 brokers + toxiproxy + prometheus + grafana
bench/                    load generator
docs/SPEC.md  docs/DECISIONS.md
```

---

## Commands

```
make tools      # install pinned buf/golangci-lint/protoc plugins into .tools/bin
make fmt        # golangci-lint fmt (gofmt + goimports)
make gen        # buf generate
make build      # ./bin/streamq-broker, ./bin/streamqctl
make test       # go test -race -count=1 ./...
make test-int   # go test -race -tags=integration ./test/integration/...
make lint       # golangci-lint run
make fuzz       # 30s fuzz on record decoding
make bench      # storage microbenchmarks + bench/ tool
make cluster-up / make cluster-down
make chaos      # chaos scripts against the compose cluster
```

---

## Coding Conventions

- `ctx context.Context` is the first parameter of anything that does I/O or may block.
- Accept interfaces, return concrete types; define small interfaces where they are consumed.
- Wrap errors with `fmt.Errorf("...: %w", err)`. Each package exposes sentinel errors
  (e.g. `storage.ErrOffsetOutOfRange`). Map them to API `ErrorCode`s **only** in `internal/server`.
- No global mutable state; no `init()` side effects.
- Prometheus metrics live in a struct that owns its own `prometheus.Registry` and is passed to
  the components that need it — never package-level metric vars or the default registry.
- Every goroutine has an owner and a shutdown path (context cancel + `errgroup`/`WaitGroup`).
  Tests use `goleak.VerifyTestMain`.
- Narrowest possible locks. Any file taking more than one lock documents lock ordering at the top.
- Logging via `slog` with fields `broker_id`, `topic`, `partition` where relevant. No `fmt.Println`.
- Tests are table-driven, use `t.TempDir()` for disk, and never use `time.Sleep` to synchronize —
  use `require.Eventually` or explicit signals. Inject a clock interface where timing matters.
- Exported identifiers have doc comments.

## Gotchas

- Consumers may only read up to the **high watermark**; followers fetching for replication may
  read up to the leader's **LEO**. Don't mix these paths up.
- The index stores `uint32` file positions, so `segment.bytes` must be ≤ 1 GiB (validate config).
- Never delete a segment a reader may hold open — use the delayed-delete scheme in SPEC §3.3.
- Every request that targets a partition carries a `leader_epoch`; always check it.

---

## Progress

- [x] M0 Scaffolding
- [ ] M1 Storage engine
- [ ] M2 Single-node broker
- [ ] M3 Client library
- [ ] M4 Consumer groups (single node)
- [ ] M5 Metadata Raft
- [ ] M6 Replication
- [ ] M7 Idempotent producer
- [ ] M8 Observability & ops
- [ ] M9 Correctness & chaos
- [ ] M10 Benchmarks & docs