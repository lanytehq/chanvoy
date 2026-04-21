//! PER-008C: daemon-restart attention-state harness.
//!
//! Exercises the chanvoy daemon binary (workspace-root `chanvoy`) through real
//! `tokio::process::Command` spawns against a long-lived `wiremock` Mattermost
//! mock, asserting that attention state survives restarts across the three
//! cursor discriminators (post_cursor, notifications_cursor, stale_cursor) and
//! that the PER-009 lifecycle primitives (`stop_daemon_if_present`,
//! `ensure_daemon_running` zombie-stop, Reuse→Refreshed bot_username
//! promotion) behave correctly end-to-end.
//!
//! Per-test isolation:
//! - `CHANVOY_CONFIG_DIR` and `CHANVOY_RUNTIME_DIR` (newly added as explicit
//!   cross-platform overrides in `chanvoy-core`) are passed to the child
//!   process only; parent-process env is never touched, so tests run in
//!   parallel without the `CONFIG_ENV_LOCK` serialization that the in-process
//!   tests use. Using these overrides rather than `XDG_*` is deliberate —
//!   `dirs::config_dir()` does not honor `XDG_CONFIG_HOME` on macOS.
//! - Each test uses a unique `--profile` slug so socket, pid, and state
//!   filenames cannot collide across tests.
//! - `LANYTE_MM_TOKEN` (or the profile-configured env var) is also passed
//!   child-only.
//!
//! Docs: see `docs/integration-tests.md` for how to run this harness and
//! which cases it covers.
//!
//! Phase 1 scaffold: harness utilities + one smoke test. Phase 2 wires the
//! core brief AC tests across the three cursor discriminators; Phase 3 folds
//! in the PER-009 lifecycle-primitive coverage (F5/F6/F7) via real
//! `chanvoy auto-setup` invocations. Helpers that only Phase 2/3 consume
//! are `#[allow(dead_code)]` at scaffold time to keep clippy happy without
//! bloating the smoke test.

#![allow(dead_code)]

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chanvoy_core::{AttentionState, Profile};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Path to the workspace-root `chanvoy` binary under test. Cargo sets this env
/// var for integration tests.
const CHANVOY_BIN: &str = env!("CARGO_BIN_EXE_chanvoy");

/// How long to wait for the daemon socket to appear after spawn.
const SPAWN_READY_TIMEOUT: Duration = Duration::from_secs(8);
/// How long to wait for the socket to disappear after a clean shutdown.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Isolated filesystem + mock-server environment for one daemon-restart test.
struct TestEnv {
    /// `CHANVOY_CONFIG_DIR` — holds `profiles/<name>.toml`, `state-<name>.json`,
    /// and `active_profile`. Passed child-only; replaces the entire base path.
    config_dir: TempDir,
    /// `CHANVOY_RUNTIME_DIR` — holds `<profile>.sock` and `<profile>.pid`.
    /// Passed child-only; replaces the entire base path.
    runtime_dir: TempDir,
    /// Long-lived wiremock server; individual tests call `reset_mocks` between
    /// restart phases so stale responders cannot satisfy later assertions.
    mock: MockServer,
    profile_name: String,
    token_env_name: String,
    token_value: String,
}

impl TestEnv {
    async fn new(profile_name: &str) -> Self {
        Self {
            config_dir: tempfile::tempdir().expect("tempdir config"),
            runtime_dir: tempfile::tempdir().expect("tempdir runtime"),
            mock: MockServer::start().await,
            profile_name: profile_name.to_string(),
            token_env_name: "LANYTE_MM_TOKEN".to_string(),
            token_value: "test-token-value".to_string(),
        }
    }

    fn server_url(&self) -> String {
        self.mock.uri()
    }

    fn config_dir(&self) -> &Path {
        self.config_dir.path()
    }

    fn runtime_dir(&self) -> &Path {
        self.runtime_dir.path()
    }

    /// Effective chanvoy config dir under isolation. The daemon resolves this
    /// via `CHANVOY_CONFIG_DIR` (checked before the platform-conventional
    /// fallback), so paths below stay stable across Linux and macOS.
    fn chanvoy_config_dir(&self) -> PathBuf {
        self.config_dir().to_path_buf()
    }

    /// Effective chanvoy runtime dir under isolation. The daemon resolves this
    /// via `CHANVOY_RUNTIME_DIR`. Files live directly under this path (no
    /// nested `chanvoy/` subdir — the override replaces the whole base).
    fn chanvoy_runtime_dir(&self) -> PathBuf {
        self.runtime_dir().to_path_buf()
    }

    fn profile_path(&self) -> PathBuf {
        self.chanvoy_config_dir()
            .join("profiles")
            .join(format!("{}.toml", self.profile_name))
    }

    fn state_path(&self) -> PathBuf {
        self.chanvoy_config_dir()
            .join(format!("state-{}.json", self.profile_name))
    }

    fn socket_path(&self) -> PathBuf {
        self.chanvoy_runtime_dir()
            .join(format!("{}.sock", self.profile_name))
    }

    /// Write a default profile pointing at the mock server.
    fn write_default_profile(&self, bot_username: &str, team_name: &str) {
        let profile = Profile {
            name: self.profile_name.clone(),
            role: "bravo-devlead".to_string(),
            scope: "lanytehq".to_string(),
            provider: chanvoy_core::Provider::Mattermost,
            bot_username: bot_username.to_string(),
            team_name: team_name.to_string(),
            server_url: self.server_url(),
            env_name: self.token_env_name.clone(),
            env_file: None,
            credential_mode: chanvoy_core::CredentialMode::EnvName,
            capability_class: chanvoy_core::CapabilityClass::Standard,
            monitored_channels: Vec::new(),
            ipc: None,
        };
        let dir = self.profile_path().parent().unwrap().to_path_buf();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut file = std::fs::File::create(self.profile_path()).unwrap();
        file.write_all(toml::to_string_pretty(&profile).unwrap().as_bytes())
            .unwrap();
    }

    /// Install the baseline Mattermost mocks needed for daemon startup
    /// (whoami, team lookup). Individual tests add channel / post mocks on top.
    async fn mock_baseline(&self, bot_id: &str, bot_username: &str, team_id: &str) {
        Mock::given(method("GET"))
            .and(path("/api/v4/users/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": bot_id,
                "username": bot_username,
                "is_bot": true,
                "nickname": null,
                "email": null,
            })))
            .mount(&self.mock)
            .await;
        Mock::given(method("GET"))
            .and(path(format!(
                "/api/v4/teams/name/{}",
                // team_name from the profile
                "org-lanytehq"
            )))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"id": team_id, "name": "org-lanytehq"})),
            )
            .mount(&self.mock)
            .await;
    }

    /// Reset all installed mocks. Tests call this between restart phases so
    /// phase-1 responders cannot silently satisfy phase-2 assertions.
    async fn reset_mocks(&self) {
        self.mock.reset().await;
    }

    /// Mount a channel-by-name lookup for the default team id used in the
    /// baseline (`team-id-456`).
    async fn mock_channel_lookup(&self, channel_name: &str, channel_id: &str) {
        Mock::given(method("GET"))
            .and(path(format!(
                "/api/v4/teams/team-id-456/channels/name/{channel_name}"
            )))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"id": channel_id, "name": channel_name})),
            )
            .mount(&self.mock)
            .await;
    }

    /// Mount a successful `POST /posts` that returns the given post id.
    /// Daemon's `post_message` hits this after resolving the channel id.
    async fn mock_post_create(&self, post_id: &str) {
        Mock::given(method("POST"))
            .and(path("/api/v4/posts"))
            .respond_with(
                ResponseTemplate::new(201).set_body_json(serde_json::json!({"id": post_id})),
            )
            .mount(&self.mock)
            .await;
    }

    /// Mount `GET /channels/{channel_id}/posts` (used by `notifications()` via
    /// `read_channel`) with the given messages. Query-string params are not
    /// constrained — the same mock satisfies any `?since=...&per_page=...`.
    /// `posts` is a list of `(id, user_id, username, message_body, create_at)`.
    async fn mock_channel_posts(&self, channel_id: &str, posts: &[(&str, &str, &str, &str, i64)]) {
        let body = serde_json::json!({
            "posts": posts.iter().map(|(id, user_id, username, message, create_at)| {
                (
                    (*id).to_string(),
                    serde_json::json!({
                        "id": id,
                        "user_id": user_id,
                        "username": username,
                        "message": message,
                        "create_at": create_at,
                    }),
                )
            }).collect::<serde_json::Map<_, _>>()
        });
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{channel_id}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&self.mock)
            .await;
    }

    /// Mount `GET /posts/{post_id}` as either a 200 (post exists in the given
    /// channel) or a 404 (post absent — triggers `CoreError::AnchorNotFound`
    /// in `assert_post_in_channel`). Used for the stale-cursor AC.
    async fn mock_post_lookup(&self, post_id: &str, channel_id: &str, exists: bool) {
        let template = if exists {
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"id": post_id, "channel_id": channel_id}))
        } else {
            ResponseTemplate::new(404)
                .set_body_json(serde_json::json!({"status_code": 404, "message": "not found"}))
        };
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/posts/{post_id}")))
            .respond_with(template)
            .mount(&self.mock)
            .await;
    }

    /// Build a `Command` that invokes the chanvoy binary with this env's
    /// isolation. Parent-process env is untouched; all path overrides are
    /// passed child-only. Uses the `CHANVOY_CONFIG_DIR` / `CHANVOY_RUNTIME_DIR`
    /// env overrides (not `XDG_*`) so isolation works on macOS as well — the
    /// `dirs` crate does not honor `XDG_CONFIG_HOME` there.
    fn chanvoy_command(&self) -> Command {
        let mut cmd = Command::new(CHANVOY_BIN);
        cmd.env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("CHANVOY_CONFIG_DIR", self.chanvoy_config_dir())
            .env("CHANVOY_RUNTIME_DIR", self.chanvoy_runtime_dir())
            .env(&self.token_env_name, &self.token_value);
        cmd
    }
}

/// Run a `chanvoy` CLI subcommand and return the full output. The command is
/// addressed to the daemon over the socket in the test env's runtime dir.
async fn run_chanvoy(env: &TestEnv, args: &[&str]) -> std::process::Output {
    env.chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(args)
        .output()
        .await
        .expect("spawn chanvoy cli")
}

/// Read the daemon-persisted attention state file for `env.profile_name`.
/// Returns `None` if the file does not exist (daemon never persisted any
/// cursor). Returns the parsed `AttentionState` otherwise.
fn read_attention_state(env: &TestEnv) -> Option<AttentionState> {
    let path = env.state_path();
    if !path.exists() {
        return None;
    }
    let contents = std::fs::read_to_string(&path).expect("read state file");
    Some(serde_json::from_str(&contents).expect("parse state file"))
}

/// Spawn the daemon via `chanvoy --profile <name> daemon serve` and wait
/// until it is **actually serving** (not just until a socket file exists).
/// Readiness is probed via `chanvoy daemon status` — an RPC round-trip that
/// only succeeds when the daemon has bound its listener and accepts calls.
///
/// Socket existence alone is not a readiness signal: after a SIGKILL of a
/// prior daemon the stale socket persists, and the next `daemon::start()`
/// removes that stale socket mid-startup before binding, so a
/// socket-presence check can return during the window where the socket
/// file is there but no daemon is actually listening yet. That window
/// plus an unbounded cleanup wait downstream is the PER-008C parallel
/// hang root cause (identified jointly by devrev + entarch on 2026-04-21).
///
/// Also fails fast if the child exits before becoming ready — prevents
/// callers from waiting on a dead child's socket that will never appear.
async fn spawn_daemon(env: &TestEnv) -> Child {
    let mut child = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .arg("daemon")
        .arg("serve")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn chanvoy daemon");

    let deadline = std::time::Instant::now() + SPAWN_READY_TIMEOUT;
    while std::time::Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            panic!(
                "daemon child exited before reaching readiness (status={status:?}); \
                 profile={}",
                env.profile_name
            );
        }
        if daemon_serving(env).await {
            return child;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = child.start_kill();
    panic!(
        "daemon did not accept daemon-status RPC at {} within {:?} (socket_present={})",
        env.socket_path().display(),
        SPAWN_READY_TIMEOUT,
        env.socket_path().exists(),
    );
}

/// Probe whether the daemon is actually serving by driving
/// `chanvoy daemon status` (which internally calls the `daemon_status` RPC
/// over the UDS). Returns true on exit-status 0. Runs a short-lived
/// subprocess per poll, which is fine at the poll cadence used by callers.
async fn daemon_serving(env: &TestEnv) -> bool {
    let out = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .arg("daemon")
        .arg("status")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await;
    matches!(out, Ok(status) if status.success())
}

/// Issue a clean shutdown via `chanvoy --profile <name> daemon stop` and wait
/// for the **child process** to actually exit. Returns true on clean exit
/// within SHUTDOWN_TIMEOUT, false otherwise.
///
/// Process exit is observed via `child.try_wait()` (non-blocking), not via
/// socket absence. Socket absence alone is unsafe as the exit gate: the
/// daemon removes its socket as part of graceful shutdown cleanup, but a
/// daemon that is still in startup (never yet served) also has no socket
/// — observing `!socket.exists()` in that case and then blocking on
/// `child.wait()` results in an unbounded hang. Keying off `try_wait`
/// makes the exit condition authoritative and lets the SHUTDOWN_TIMEOUT
/// bound a misbehaving shutdown rather than waiting forever.
///
/// If the child is still alive past the timeout, force-kill via sysprims
/// and return false. Emits a diagnostic on non-zero shutdown-subprocess
/// exit (common when the daemon was never serving) so misuse surfaces
/// in the test log rather than silently persisting a live child.
async fn stop_daemon_cleanly(env: &TestEnv, mut child: Child) -> bool {
    let shutdown_out = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .arg("daemon")
        .arg("stop")
        .output()
        .await
        .expect("chanvoy daemon stop");
    if !shutdown_out.status.success() {
        eprintln!(
            "daemon stop exited non-zero: {}\nstderr={}",
            shutdown_out.status,
            String::from_utf8_lossy(&shutdown_out.stderr)
        );
    }
    let deadline = std::time::Instant::now() + SHUTDOWN_TIMEOUT;
    while std::time::Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_status)) => return true,
            Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(err) => {
                eprintln!("stop_daemon_cleanly: try_wait errored: {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    // Fallback: child did not exit on its own within SHUTDOWN_TIMEOUT.
    // Force-kill so the harness does not leak a live daemon across tests.
    if let Some(pid) = child.id() {
        let _ = sysprims_signal::force_kill(pid);
    }
    let _ = child.wait().await;
    false
}

/// SIGKILL the daemon and wait for the process to exit. Socket may be left
/// orphaned; `daemon::start()` cleans up stale sockets on subsequent spawn.
///
/// Uses `sysprims_signal::force_kill` rather than `tokio::process::Child::start_kill`
/// — the tokio path has observed delivery gaps on macOS in test contexts
/// (signal sent but pid kept running, `wait()` blocks indefinitely). sysprims
/// is the 3leaps primitive for process management; wraps a reliable
/// `libc::kill(pid, SIGKILL)` with cross-platform behavior.
///
/// Bounds the `wait` with a deadline so any remaining reap issue surfaces
/// as a test failure instead of a hang.
async fn kill_daemon(mut child: Child) {
    let pid = child.id().expect("child pid present before kill");
    let t0 = std::time::Instant::now();
    let force_kill_result = sysprims_signal::force_kill(pid);
    let t_force_kill = t0.elapsed();
    if let Err(err) = force_kill_result {
        panic!("kill_daemon: sysprims force_kill({pid}) failed: {err}");
    }
    // Probe OS-visible liveness BEFORE tokio's wait() so we can separate
    // "SIGKILL not delivered / daemon alive" from "daemon dead but wait()
    // not waking". Poll every 50ms for up to 2s.
    let liveness_deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut observed_dead: Option<Duration> = None;
    while std::time::Instant::now() < liveness_deadline {
        if sysprims_proc::get_process(pid).is_err() {
            observed_dead = Some(t0.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let t_before_wait = t0.elapsed();
    let wait_outcome = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
    let t_after_wait = t0.elapsed();
    if wait_outcome.is_err() || observed_dead.is_none() {
        eprintln!(
            "\nkill_daemon diagnostic pid={pid}:\n\
             - force_kill returned Ok after {t_force_kill:?}\n\
             - os-liveness observed_dead: {observed_dead:?}\n\
             - wait() start={t_before_wait:?} end={t_after_wait:?} outcome={wait_outcome:?}\n"
        );
    }
    if observed_dead.is_none() {
        panic!("kill_daemon: pid {pid} still alive 2s after force_kill → SIGNAL DELIVERY FAILURE");
    }
    match wait_outcome {
        Ok(Ok(_)) => {}
        Ok(Err(err)) => panic!("kill_daemon: wait errored: {err}"),
        Err(_) => panic!(
            "kill_daemon: pid {pid} died at {observed_dead:?} but tokio wait() timed out after 5s → REACTOR ISSUE"
        ),
    }
}

/// Smoke test for Phase 1: the harness compiles, the daemon spawns against
/// the mock, comes healthy (socket appears), and shuts down cleanly. No
/// state assertions yet — those land in Phase 2 across the four cursor
/// discriminators.
///
/// All tests in this file are `#[ignore]` so they stay out of the default
/// `cargo test` / `make check` fast loop. Run via `make test-integration`,
/// which passes `--ignored`. `make pr-final` runs both, matching CI.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn harness_smoke_daemon_spawns_and_stops_cleanly() {
    let env = TestEnv::new("per-008c-smoke-bravo").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-123", "agent-bravo-devlead", "team-id-456")
        .await;

    let child = spawn_daemon(&env).await;
    assert!(
        env.socket_path().exists(),
        "socket must be present after spawn_daemon returns"
    );

    let clean = stop_daemon_cleanly(&env, child).await;
    assert!(clean, "daemon should shut down cleanly within timeout");
    assert!(
        !env.socket_path().exists(),
        "socket must be gone after clean shutdown"
    );
}

// ---- Phase 2: core brief ACs across the three cursor discriminators ----
//
// Each test exercises the real daemon binary through its socket RPC (no
// in-process shortcuts), drives state changes via the CLI, reads the
// persisted attention-state file directly, and asserts survival across a
// restart boundary. Mocks are reset between pre-restart and post-restart
// phases so phase-1 responders cannot silently satisfy phase-2 assertions.

/// AC #1: post_cursor persists across a clean daemon shutdown + restart.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_cursor_survives_clean_restart() {
    let env = TestEnv::new("per-008c-ac1-post-clean").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ac1", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("test-channel", "chan-id-ac1").await;
    env.mock_post_create("post-id-ac1-phase1").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["post", "test-channel", "hello phase 1"]).await;
    assert!(
        out.status.success(),
        "post must succeed, stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let state_before = read_attention_state(&env).expect("state file after post");
    let cursor_before = state_before
        .channels
        .get("test-channel")
        .expect("channel cursor present");
    assert_eq!(
        cursor_before.last_seen_post_id.as_deref(),
        Some("post-id-ac1-phase1"),
        "post_message should record the returned post id as the channel cursor"
    );

    assert!(
        stop_daemon_cleanly(&env, daemon).await,
        "daemon should shut down cleanly"
    );

    // Phase 2: re-mount only the baseline. If the daemon somehow re-hits the
    // post / channel-lookup endpoints during boot, the test fails (unmocked
    // path → 404) — which is the contract we want.
    env.reset_mocks().await;
    env.mock_baseline("bot-id-ac1", "agent-bravo-devlead", "team-id-456")
        .await;

    let daemon2 = spawn_daemon(&env).await;
    let state_after = read_attention_state(&env).expect("state file survives restart");
    assert_eq!(
        state_after, state_before,
        "attention state must be byte-for-byte preserved across a clean restart"
    );
    let _ = stop_daemon_cleanly(&env, daemon2).await;
}

/// AC #2: post_cursor persists across SIGKILL + restart. Same shape as AC #1
/// but the pre-restart phase terminates with `SIGKILL` (no shutdown RPC,
/// no cleanup hook). Catches any half-flushed state writes.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_cursor_survives_sigkill_restart() {
    let env = TestEnv::new("per-008c-ac2-post-kill").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ac2", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("test-channel", "chan-id-ac2").await;
    env.mock_post_create("post-id-ac2-phase1").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["post", "test-channel", "hello kill-me"]).await;
    assert!(
        out.status.success(),
        "post must succeed, stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let state_before = read_attention_state(&env).expect("state file after post");
    let cursor_before = state_before
        .channels
        .get("test-channel")
        .expect("channel cursor present");
    assert_eq!(
        cursor_before.last_seen_post_id.as_deref(),
        Some("post-id-ac2-phase1"),
    );

    // SIGKILL. `daemon::start` cleans up stale socket on next boot, so no
    // pre-boot cleanup is needed here.
    kill_daemon(daemon).await;

    env.reset_mocks().await;
    env.mock_baseline("bot-id-ac2", "agent-bravo-devlead", "team-id-456")
        .await;

    let daemon2 = spawn_daemon(&env).await;
    let state_after = read_attention_state(&env).expect("state file survives sigkill");
    assert_eq!(
        state_after, state_before,
        "attention state must survive SIGKILL — writes are synchronous fs::write"
    );
    let _ = stop_daemon_cleanly(&env, daemon2).await;
}

/// AC #3: notifications_cursor persists across clean restart. Exercises the
/// full `notifications` sweep path that writes the mention cursor
/// (`record_notifications_cursor` inside the daemon handler).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn notifications_cursor_survives_clean_restart() {
    let env = TestEnv::new("per-008c-ac3-notif").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ac3", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("agent-notifications", "chan-id-notif")
        .await;
    env.mock_channel_posts(
        "chan-id-notif",
        &[(
            "post-mention-phase1",
            "someone-id",
            "someone",
            "hey @agent-bravo-devlead ping",
            1_776_000_000_000,
        )],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["notifications"]).await;
    assert!(
        out.status.success(),
        "notifications must succeed, stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let state_before = read_attention_state(&env).expect("state file after notifications sweep");
    assert_eq!(
        state_before.mentions.last_seen_post_id.as_deref(),
        Some("post-mention-phase1"),
        "full notifications sweep should record the last mention's post id"
    );

    assert!(
        stop_daemon_cleanly(&env, daemon).await,
        "daemon should shut down cleanly"
    );

    env.reset_mocks().await;
    env.mock_baseline("bot-id-ac3", "agent-bravo-devlead", "team-id-456")
        .await;

    let daemon2 = spawn_daemon(&env).await;
    let state_after = read_attention_state(&env).expect("state file survives restart");
    assert_eq!(
        state_after, state_before,
        "mention cursor must persist across clean restart"
    );
    let _ = stop_daemon_cleanly(&env, daemon2).await;
}

/// AC #4: stale_cursor path survives restart. After a post cursor is
/// established, the anchor post is "deleted" externally (mock `/posts/{id}`
/// flips to 404). On restart, `check_channel` should detect the stale
/// anchor and degrade to `anchor_source=stale_cursor` rather than erroring.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn stale_cursor_path_preserved_across_restart() {
    let env = TestEnv::new("per-008c-ac4-stale").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ac4", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("test-channel", "chan-id-ac4").await;
    env.mock_post_create("post-id-ac4-phase1").await;

    // Phase 1: establish cursor via post.
    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["post", "test-channel", "anchor me"]).await;
    assert!(
        out.status.success(),
        "post must succeed, stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let state_before = read_attention_state(&env).expect("state file after post");
    assert_eq!(
        state_before
            .channels
            .get("test-channel")
            .and_then(|c| c.last_seen_post_id.as_deref()),
        Some("post-id-ac4-phase1"),
    );
    assert!(stop_daemon_cleanly(&env, daemon).await);

    // Phase 2: anchor post is now "gone". /posts/<post_id> returns 404, which
    // `assert_post_in_channel` maps to `CoreError::AnchorNotFound`, which
    // `check_channel` in turn catches (while anchor_source == daemon_cursor)
    // and degrades to `stale_cursor_check_result`.
    env.reset_mocks().await;
    env.mock_baseline("bot-id-ac4", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("test-channel", "chan-id-ac4").await;
    env.mock_post_lookup("post-id-ac4-phase1", "chan-id-ac4", false)
        .await;

    let daemon2 = spawn_daemon(&env).await;
    let state_after = read_attention_state(&env).expect("state file survives restart");
    assert_eq!(
        state_after, state_before,
        "cursor file contents must be preserved across the restart"
    );

    // `check_channel` with a stale anchor + daemon_cursor source returns
    // has_new_messages=false, which the CLI wrapper translates to exit 1.
    // That exit code is load-bearing for ops — the test asserts it as well
    // as the structured anchor_source.
    let out = run_chanvoy(&env, &["--json", "check", "test-channel"]).await;
    assert_eq!(
        out.status.code(),
        Some(1),
        "check returns exit 1 on no-new-messages; stale_cursor falls in that bucket",
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("parse check json: {e}\n{stdout}"));
    assert_eq!(
        parsed["anchor_source"].as_str(),
        Some("stale_cursor"),
        "stale anchor must surface as anchor_source=stale_cursor (full json: {parsed})",
    );
    assert_eq!(parsed["has_new_messages"].as_bool(), Some(false));
    assert_eq!(parsed["count"].as_i64(), Some(0));

    let _ = stop_daemon_cleanly(&env, daemon2).await;
}
