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

Tests are parallel-safe **within one cargo process**: each test owns
independent `TempDir` instances for config and runtime paths, passed to the
child process via `CHANVOY_CONFIG_DIR` and `CHANVOY_RUNTIME_DIR` (explicit
overrides in `chanvoy-core` — these exist both for test isolation and for
production use on non-standard layouts). Callers supply the `--profile` slug
(unique per test within a suite). Concurrent multi-seat runs on one machine may
reuse the same fixed slugs; process-table checks therefore scope to each
invocation's runtime dir via `lsof`. Parent-process env is never mutated.

### Multi-seat host SOP

**Primary (this repo — self-contained).** Acceptance for this product does
**not** depend on a mutable shared checkout of agent-support docs.

On a **shared** agent host, concurrent multi-seat panels that each run
`make pr-final` (or `make test-integration`) used to false-fail
`daemon_start_sweeps_residue_when_child_exits_after_binding` when process
counts matched only on fixed profile slugs. Isolation now scopes counts to
each invocation's runtime dir via `lsof` (see `tests/restart_harness.rs`).

**Residual serialization (still required):** only **one seat at a time**
should run chanvoy `make pr-final` / `make test-integration`
(`restart_harness`). Other panel seats use `make check` (or wait their turn).
Why: integration tests share fixed profile slugs across seats while each has
its own runtime dir; harness process-count isolation helps, but concurrent
full restart suites still thrash the host (CPU / mock-server noise).

**Channel deadman is separate:** one seat owns `chanvoy wait` per channel —
that rule is chat control-plane discipline, not a substitute for the product
serialization above.

**Secondary (estate echo, optional):** when present and durable on your host's
support pin, the same one-liner may also appear under
`$LANYTE_AGENT_SUPPORT_ROOT/multi-harness-recipes.md` (Card D — shared-host
multi-seat gates). That card is **not** required for scoring this repo tip;
do not treat a missing or branch-raced support checkout as a chanvoy code defect.

Shared harness primitives live in `tests/common/mod.rs` (mounted via
`mod common;` from each integration test file). Test-specific helpers
stay in the file that uses them.

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

**PER-008B attention-state inspection** (`tests/attention_inspection.rs`):

Exercises the `chanvoy attention list` / `chanvoy attention show`
commands end-to-end. The central reviewer ask (devrev, 2026-04-21) is
the **non-mutation invariant** — adding these commands leaves daemon
state identical before and after. Asserted by byte-comparing the
persisted state file across CLI invocations, not by code-reading the
read-only claim.

- `attention_list_cold_state_is_empty` — daemon with no tracked
  channels returns empty channels list + `no_anchor` mentions.
- `attention_list_and_show_after_post` — post establishes a
  `post_cursor`; both list and show surface the channel with the
  expected source + newest_seen.
- `attention_show_untracked_channel_is_no_anchor` — show on an
  untracked channel returns a `no_anchor` entry (not an error).
- `attention_list_shows_stale_cursor_after_check` — post establishes
  cursor; mock flips `/posts/<id>` to 404; `check` detects staleness
  and caches the verdict on `ChannelCursorState`; subsequent `list`
  shows `source=stale_cursor` with populated `last_checked_at`. This
  is the D1 cached-staleness shape aligned with cxotech + devrev.
- `attention_commands_do_not_mutate_state_file` — snapshots state-file
  bytes before + after list/show invocations (both JSON and text
  paths), asserts byte-equal. devrev's explicit non-mutation ask.
- `attention_list_text_output_renders` — smoke test for the text
  output contract: header row, channel row, source label, mentions
  sibling all present.

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
- **Teardown**: Phase 3 / 4 tests spawn detached daemons (via
  `auto-setup`), so each explicitly calls `teardown_auto_setup_daemon`
  to reap any leftover pid via `sysprims_signal::force_kill` on the
  pid-file contents. **Plus** an `AutoSetupDaemonGuard` (`let _guard =
  env.daemon_guard();`) RAII guard with a sync `Drop` that re-runs the
  pid-file kill on test panic — important since PER-008D's setsid
  detachment means the daemon really does survive the test process.
  Without the guard, a panicking test would leak a real backgrounded
  daemon.

## Daemon lifecycle model (PER-008D detachment)

Both background entry points — `chanvoy auto-setup` and
`chanvoy daemon start` — spawn the daemon through one shared primitive,
`spawn_durable_daemon` (`crates/chanvoy-cli/src/lib.rs`), which calls
`libc::setsid()` via `pre_exec`, making the daemon the leader of a new
session and process group with no controlling terminal. Operational
consequence: the daemon survives the spawning shell's exit, including
the controlling terminal closing (no `SIGHUP` propagation), and the end
of an agent tool invocation. Operators returning to the same machine in
a fresh session find the daemon still running on its profile socket.

Direct `chanvoy daemon serve` (the foreground / debug path) is **not**
detached — it intentionally stays attached to the spawning shell so
operators can `Ctrl-C` it and follow logs.

Test-design consequence, learned the hard way: **detachment coverage
must name the entry point it exercises.** PER-008D narrowed detachment
to the auto-setup path and the Phase 4 tests below were scoped to
match, so the suite stayed green for months while `daemon start` — a
documented lifecycle verb with its own spawn implementation — was
non-durable. When a contract has two entry points, test both, and make
the binding assertion cross-invocation (a *fresh process* reaching the
daemon after its starter exited), not chained commands inside one
shell.

## PER-008D detachment tests (Phase 4)

- `auto_setup_daemon_detaches_into_new_session` — spawns auto-setup as
  an intermediate process via `Command::spawn() + wait()`, then
  asserts:
  - `libc::getsid(daemon_pid) == daemon_pid` — daemon is its own
    session leader (load-bearing setsid contract; uniform across
    Linux init / systemd-user / macOS launchd)
  - `sysprims_proc::get_process(daemon_pid).ppid != intermediate_pid`
    — daemon was reparented away from the intermediate (corroborating
    evidence that the auto-setup CLI subprocess exited cleanly)
  - `chanvoy daemon status` answers — detachment did not break socket
    / RPC machinery
- `auto_setup_detached_daemon_state_survives_session_transition` —
  end-to-end model of "Session A spawns daemon, Session B uses it":
  posts via the detached daemon in Session A, then a fresh CLI
  invocation (Session B) inspects attention state and observes
  Session A's cursor.

## `daemon start` lifecycle gate (CHAN-TASK-001, Phase 5)

The `daemon start` counterpart to the Phase 4 gate. Every case runs the
CLI as a real subprocess and asserts on process/session state or fresh
invocations — none of them sleep for a fixed duration to let things
settle.

- `daemon_start_detaches_into_new_session` — the binding gate. Spawns
  `daemon start` as an intermediate process, waits for it to exit, then
  asserts:
  - `libc::getsid(daemon_pid) == daemon_pid` — own session leader
  - daemon reparented away from the intermediate CLI pid
  - the bootstrap handoff was consumed — proof the child bound on the
    parent-validated identity rather than making its own network call
  - `daemon status` answers from a fresh CLI process
  - `whoami` (network-backed, through the daemon to the mock) succeeds
    from a fresh CLI process and returns the validated identity — this
    is the operation that failed in the field, and a socket-only probe
    would not catch it
- `daemon_start_recovers_from_stale_socket_and_dead_pid` — plants a
  bound-then-dropped socket inode plus a pid file naming a reaped
  process; a plain `daemon start` must recover with no manual file
  movement
- `daemon_start_refuses_on_bot_identity_mismatch` — live credential
  resolves to a different bot than the profile records: nonzero exit,
  both names in the diagnostic, and nothing spawned or mutated (no
  socket, no pid file, no handoff, `bot_username` intact, no
  `active_profile` marker)
- `daemon_start_classifies_child_startup_failure` — forces a
  child-only failure (reduce policy naming a nonexistent family
  profile, which the daemon refuses at startup while the parent's own
  validation passes); asserts the error is a startup-failure
  classification naming the stage, not a bare `NotRunning`, and that
  the unconsumed handoff is cleaned up
- `daemon_start_timeout_leaves_no_live_child_and_retry_yields_one_daemon`
  — a start reported as failed must be terminal. Forces a deterministic
  hang (reduce policy whose family profile lives on a second mock server
  that delays `whoami` past the startup budget, so the child blocks in
  `build_reduce_writer` while the parent's own validation succeeds), then
  asserts no live `daemon serve` process for the profile — counted from
  the process table, not the pid file, because the failure mode is a
  child alive *without* having written one — plus no pid file, socket, or
  orphaned handoff, and that a retry yields exactly one daemon
- `daemon_start_requires_explicit_profile_selection` — the
  command-to-policy mapping, which core resolver unit tests do not cover:
  bare `daemon start` refuses both the `active_profile` marker and the
  single-running-daemon fallback, while `--profile` and env-exact
  identity succeed. Also pins the scope of the widening by asserting a
  read verb (`whoami`) still resolves via fallback
- `daemon_serve_remains_attached_to_invoking_session` — the negative
  control: `daemon serve` must *not* become a session leader and must
  stay in the invoking process's session

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
