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
- The CI workflow has not run on GitHub yet (nothing has been pushed).
- No goleak `TestMain` yet; it arrives with the first package that has tests (M1).
