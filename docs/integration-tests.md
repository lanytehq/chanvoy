# chanvoy integration tests

Integration tests live under `tests/` at the workspace root and exercise the
actual `chanvoy` binary via `tokio::process::Command`. They are separate from
the unit test suite that runs under `make check`.

## Running

```sh
make test-integration       # just the integration tests
make pr-final               # full merge-gate (includes integration tests)
```

Integration tests are marked `#[ignore]` so `cargo test` / `make check` skip
them by default — the fast loop stays fast. `--ignored` turns them on.

## What's covered

**PER-008C restart harness** (`tests/restart_harness.rs`):

Phase 1 (landed):
- Harness scaffold — `TestEnv`, daemon spawn/stop helpers, mock Mattermost
  baseline, snapshot reader
- Smoke test: daemon spawns against mock, comes healthy, shuts down cleanly

Phase 2 (next): core brief ACs
- post_cursor survives clean restart
- post_cursor survives SIGKILL restart
- notifications_cursor survives restart
- stale_cursor path preserved across restart

Phase 3 (next): PER-009 lifecycle-primitive coverage (driven via real
`chanvoy auto-setup` invocations)
- `stop_daemon_if_present` across its three presence states
- `ensure_daemon_running` zombie-stop path
- Reuse→Refreshed bot_username promotion

## Isolation

Each test owns independent `TempDir` instances for config and runtime paths,
passed to the child process via `CHANVOY_CONFIG_DIR` and `CHANVOY_RUNTIME_DIR`
(explicit overrides recently added to `chanvoy-core`, usable in production for
non-standard deployments). Parent-process env is never mutated, so tests are
safe to run in parallel. Token env vars are also passed child-only.

Per-test `--profile <slug>` ensures socket, pid, and state filenames cannot
collide across tests.

## Mock Mattermost

A long-lived `wiremock::MockServer` is bound per test. Tests call
`TestEnv::reset_mocks` between restart phases so phase-1 responders cannot
silently satisfy phase-2 assertions.
