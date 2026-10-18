# CLAUDE.md — streamq

streamq is a distributed, partitioned, replicated, log-based message queue in Go (a small Kafka).

**The full specification is in `docs/SPEC.md`.** Read the sections relevant to the current
milestone before writing code. This file holds only what you need in every session.

---

## Project Rules (always apply)

- **Run autonomously.** I am not in the loop. The reviewer subagents are your approvers: their
  APPROVE replaces my approval. Do not stop to ask me questions or wait for confirmation.
- Build milestones **in order** (`docs/SPEC.md` §9). Every milestone follows this loop, then you
  continue straight to the next milestone until M10 is done:
  1. Draft a plan (files, types, key functions, test cases).
  2. Invoke the **plan-reviewer** subagent with the plan and milestone number. Fix every BLOCKER and
     MAJOR finding and re-run it until it returns APPROVE (max 3 rounds). If it lists questions
     for the user, answer them yourself using SPEC.md and Kafka's behavior as the tie-breaker,
     and record each answer in `docs/DECISIONS.md`.
  3. Save the approved plan to `docs/plans/M<N>.md`, including the reviewer's final verdict and
     any findings you overrode with your reasons.
  4. Implement, in parallel where the plan allows (see "Parallel Work" below). Run `make lint test`
     (plus `make test-int` from M2 onward) until green.
  5. Review. Invoke the **code-reviewer** subagent, and for M1, M5, M6, M7 and M9 invoke the
     **failure-reviewer** at the same time, in parallel. Fix every BLOCKER and MAJOR finding from
     both and re-run them until both APPROVE (max 3 rounds). Record MINOR findings left for later
     in `docs/PROGRESS_LOG.md`.
  6. Commit locally, tick Progress, update `docs/PROGRESS_LOG.md`, and move on to the next milestone.
- Decisions that would normally need my sign-off (a dependency not in the approved list, changing
  the on-disk format after M1, a breaking change to `api/v1/*.proto`, deleting or rewriting
  existing tests) must instead be approved by the plan-reviewer (or code-reviewer, if it arises
  during implementation) and logged in `docs/DECISIONS.md` with the justification.
- Reviewer subagents never edit files. You may override a reviewer finding only if you record the
  reason in the milestone's plan file or `docs/DECISIONS.md`.
- If a reviewer still says REVISE / CHANGES REQUIRED after 3 rounds, or the same failure persists
  after 3 genuinely different fix attempts, do not loop forever: write the problem, what you
  tried, and the reviewer's outstanding findings to `docs/BLOCKED.md`, commit, and **stop**.
  This is the only reason to stop before M10 (besides being blocked by something outside the
  repo, like a missing tool or no Docker).
- Never mark work done with failing or skipped tests.
- If the spec doesn't answer a design question, choose the Kafka-like behavior and add a
  one-paragraph entry to `docs/DECISIONS.md`.
- Keep `docs/PROGRESS_LOG.md` current (what was built, decisions, known limitations, test/bench
  results) so a fresh session can resume from the next unchecked milestone.

## Parallel Work

Use parallel subagents when it saves real time without risking correctness:

- **Milestones stay sequential.** Parallelism is only *within* a milestone, never across milestones.
- **Reviews:** run the reviewers named in step 5 in parallel. Independent read-only research (e.g.
  checking how Kafka handles a case while also auditing existing code) may also run in parallel.
- **Implementation:** split a milestone into parallel work packages only when the packages touch
  **disjoint files**. The plan must list, per package: owned paths, the contract it depends on,
  and its done criteria. The plan-reviewer checks this split.
- **Contract first.** Before spawning implementers, the main session writes the shared interfaces,
  types, proto messages, and stub constructors, makes `go build ./...` pass, and commits on the main
  branch. Implementer worktrees branch from the default branch, so anything uncommitted is invisible
  to them.
- Spawn one **implementer** subagent per package (each runs in its own git worktree). Max 4 at once.
- **Integrate:** merge each implementer branch with `git merge --no-ff` (no rebase), resolve any
  conflict, then run `make lint test` (and `make test-int`) on the merged result before reviewing.
  If an implementer reports a CONTRACT ISSUE, fix the contract on main and re-run the affected
  packages rather than patching around it. Delete merged worktrees and branches afterwards.
- **Do not parallelize** tightly coupled correctness-critical code: the core of M6 (replication,
  HW, ISR, truncation), the M5 FSM, and the M7 sequence logic are built by the main session alone.
- Good candidates: M1 `record` vs index/checkpoint helpers (after the record contract); M3
  Producer / Consumer / Admin; M8 metrics / docker-compose + Grafana / `streamqctl cluster status`;
  M9 porcupine model / chaos scripts; M10 bench tool / README and diagrams.

## Git Rules (always apply)

- **Never run `git push`** (or anything that pushes: `gh pr create`, `gh repo sync`, force-push,
  pushing tags) until I explicitly say so in chat. Local commits are fine.
- Commit messages must **not** contain `Co-Authored-By: Claude`, "Generated with Claude Code", or
  any other AI attribution trailer or footer. Write plain commit messages only.
- Use Conventional Commits, e.g. `feat(storage): add sparse offset index`,
  `fix(partition): truncate follower log on epoch change`. One logical change per commit.
- Never rewrite history (`rebase`, `commit --amend`, `reset --hard`) on existing commits. Fix
  mistakes with a new commit.
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
make test-int   # go test -race -count=1 -tags=integration ./test/integration/...
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
  the components that need it — never package-level metric vars or the default registry
  (`gochecknoglobals` enforces this).
- No `fmt.Print*` anywhere (`forbidigo`). CLI output uses `fmt.Fprint*(cmd.OutOrStdout(), ...)`;
  everything else logs through `slog`.
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
- [x] M1 Storage engine
- [x] M2 Single-node broker
- [x] M3 Client library
- [x] M4 Consumer groups (single node)
- [x] M5 Metadata Raft
- [x] M6 Replication
- [x] M7 Idempotent producer
- [ ] M8 Observability & ops
- [ ] M9 Correctness & chaos
- [ ] M10 Benchmarks & docs