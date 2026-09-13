# Repository Guidelines

## Project Structure & Module Organization

rcscheduler is a single-host rclone migration controller for Linux and macOS, with local JSON persistence and no database dependency.

- `cmd/rcscheduler/main.go`: executable entry point.
- `internal/cli` and `internal/httpapi`: command parsing, client connections, and HTTP handlers.
- `internal/service`: batch imports, scheduling, execution, and recovery.
- `internal/model`, `manifest`, `ledger`, `runner`, and `store`: shared types, manifest validation, object accounting, rclone processes, and atomic persistence.
- Tests live beside implementation in `*_test.go`; `deploy/rcscheduler.service` provides the systemd template.
- `bin/` holds generated binaries; `data/` holds local runtime state. Both are ignored by Git.

## Build, Test, and Development Commands

Use the Go version declared in `go.mod` (currently `1.27.0`).

- `make build`: build `bin/rcscheduler` with CGO disabled.
- `make test`: run all Go tests with the race detector.
- `make vet`: run `go vet ./...`.
- `make fmt`: format `cmd` and `internal` with `gofmt`.
- `RCSCHEDULER_TEST_RCLONE=/path/to/rclone-v1.75.1 make integration`: run uncached tests, including real rclone contracts.
- `make linux`: build Linux amd64 and arm64 binaries.

Start locally after building:

```sh
./bin/rcscheduler --data-dir ./data serve \
  --rclone /path/to/rclone --rclone-config /path/to/rclone.conf
```

Global flags precede the subcommand; HTTP defaults to `127.0.0.1:8787`.

## Coding Style & Naming Conventions

Use `gofmt` formatting and tabs. Keep package names lowercase, exported identifiers PascalCase, and unexported identifiers camelCase. Write comments in Chinese. Keep scheduling in `service`, process control in `runner`, and persistence in `store`. Preserve CLI/API compatibility and versioned JSON records.

## Testing Guidelines

Tests use Go's standard `testing` package and `net/http/httptest`. Name tests `Test<Behavior>` and isolate files with `t.TempDir()`. No numeric coverage threshold is configured. Add regression tests for changed behavior, especially validation barriers, object counts, retries, and crash recovery. Run a focused test with `go test ./internal/service -run TestAutomaticRetryLimit`.

Ordinary tests need no rclone binary; real contracts skip unless `RCSCHEDULER_TEST_RCLONE` is set. Use isolated local fixtures, never production buckets.

## Commit & Pull Request Guidelines

No consistent commit convention is established yet. Use imperative subjects, such as `Fix retry accounting`. PRs should describe the problem, behavior changes, validation results, and compatibility implications; link related issues and update `README.md` for user-facing changes.

## Security & Persistence

Never commit credentials or private environment details. Use generic examples without personal infrastructure names or topology. Use one controller per local data directory; shared writers and NFS are unsupported. Preserve atomic writes and fail-closed scheduling on persistence errors. Stop the service before backing up runtime data; do not manually edit live records.
