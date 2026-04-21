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

Tests are parallel-safe: each test owns independent `TempDir` instances for
config and runtime paths, passed to the child process via `CHANVOY_CONFIG_DIR`
and `CHANVOY_RUNTIME_DIR` (explicit overrides in `chanvoy-core` — these exist
both for test isolation and for production use on non-standard layouts). Each
test uses a unique `--profile <slug>` so socket/pid/state filenames never
collide. Parent-process env is never mutated.

## What's covered

**PER-008C restart harness** (`tests/restart_harness.rs`):

### Phase 1 — scaffold + smoke

- `harness_smoke_daemon_spawns_and_stops_cleanly` — daemon comes up against
  the mock, responds to `daemon status` RPC, shuts down cleanly. No state
  assertions — validates the harness itself.

### Phase 2 — brief AC #1–4 (cursor discriminators across restart)

- `post_cursor_survives_clean_restart` — `chanvoy post` records a cursor;
  daemon stops cleanly; daemon restarts; asserts cursor preserved.
- `post_cursor_survives_sigkill_restart` — same flow, but SIGKILL via
  `sysprims_signal::force_kill` instead of graceful shutdown. Catches any
  half-flushed state writes.
- `notifications_cursor_survives_clean_restart` — full `notifications`
  sweep records the mention cursor; restart preserves it.
- `stale_cursor_path_preserved_across_restart` — post establishes cursor;
  mock flips `/posts/<id>` to 404 post-restart; `check` correctly degrades
  to `anchor_source=stale_cursor`.

### Phase 3 — PER-009 lifecycle primitives (via real `chanvoy auto-setup`)

Driven through the real `chanvoy auto-setup` CLI path rather than direct
helper calls, so regressions at the dispatch boundary (where F5/F6/F7
actually landed during PER-009) are caught.

- `auto_setup_recovers_from_stale_socket` — F5 stale-socket subcase.
  Plants a stale socket file (bind + drop); auto-setup must recover via
  `stop_daemon_if_present`'s NotRunning-on-shutdown fast path and spawn
  cleanly.
- `auto_setup_stops_zombie_and_respawns` — F6 zombie-stop path.
  SIGSTOPs daemon1 to model a wedged daemon (alive + holding socket +
  unresponsive). Second auto-setup must detect via socket-presence,
  force-kill, and spawn fresh. Exercises the ping/shutdown timeout
  fallbacks in `chanvoy-cli`.
- `auto_setup_promotes_reuse_to_refreshed_on_bot_username_drift` — F7.
  Whoami flips between invocations; second auto-setup routes Reuse →
  Refreshed because the env-current credential authenticates as a
  different bot. Asserts persisted profile update, daemon restart, and
  JSON report structure (`profile_state=refreshed` + diff entry).

## Harness conventions

- **Readiness**: `spawn_daemon` polls `chanvoy daemon status` (real RPC
  round-trip) rather than socket-file presence. Socket existence alone
  can be a false-positive during daemon startup windows.
- **Cleanup**: `stop_daemon_cleanly` keys off `child.try_wait()` with
  `SHUTDOWN_TIMEOUT`, not unbounded `child.wait().await`. Socket absence
  is ambiguous as an exit gate — a never-yet-serving daemon also has no
  socket.
- **SIGKILL delivery**: `sysprims_signal::force_kill` is used instead of
  `tokio::process::Child::start_kill`, which has observed delivery gaps
  on macOS in test contexts. Falls back to `kill -9` shell-out in
  chanvoy-cli production code for the same reason when sysprims isn't
  a prod dep.
- **Mock server state**: `TestEnv::reset_mocks()` + explicit re-mount
  between phases; phase-1 responders cannot silently satisfy phase-2
  assertions.
- **Teardown**: Phase 3 tests spawn detached daemons (via `auto-setup`),
  so each explicitly calls `teardown_auto_setup_daemon` to reap any
  leftover pid via `sysprims_signal::force_kill` on the pid-file contents.

## Adding a test

1. Start in `#[tokio::test] #[ignore = "integration: run via make test-integration"]`.
2. Create a `TestEnv` with a unique profile slug.
3. Mount baseline mocks (`mock_baseline`) plus whatever the test flow
   touches (`mock_channel_lookup`, `mock_post_create`, `mock_channel_posts`,
   `mock_post_lookup`, `mock_empty_memberships`, etc. — add helpers on
   `TestEnv` rather than inlining `Mock::given` when patterns repeat).
4. Exercise the flow using either `spawn_daemon` + `run_chanvoy` (for
   RPC-driven scenarios) or `auto_setup_command` (for dispatch-driven
   scenarios).
5. Read state via direct file access (`read_attention_state`,
   `read_persisted_profile`, `read_daemon_pid`) — avoid in-process
   helpers that depend on parent-process env.
6. Clean up with `stop_daemon_cleanly` for tests that own a `Child` or
   `teardown_auto_setup_daemon` for detached-daemon cases.
