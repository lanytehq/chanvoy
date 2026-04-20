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
/// until its socket appears. Returns the `Child` so tests can clean-shutdown
/// or SIGKILL.
async fn spawn_daemon(env: &TestEnv) -> Child {
    let child = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .arg("daemon")
        .arg("serve")
        .stdin(std::process::Stdio::null())
        // Inherit stdout/stderr so daemon-side panics/errors surface in the
        // test log. The daemon itself uses tracing without `without_time()`;
        // volume is modest.
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("spawn chanvoy daemon");

    let deadline = std::time::Instant::now() + SPAWN_READY_TIMEOUT;
    while std::time::Instant::now() < deadline {
        if env.socket_path().exists() {
            return child;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "daemon did not create socket at {} within {:?}",
        env.socket_path().display(),
        SPAWN_READY_TIMEOUT
    );
}

/// Issue a clean shutdown via `chanvoy --profile <name> daemon stop` and wait
/// for the socket file to disappear. Returns true on clean exit within
/// SHUTDOWN_TIMEOUT, false otherwise.
async fn stop_daemon_cleanly(env: &TestEnv, mut child: Child) -> bool {
    let status = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .arg("daemon")
        .arg("stop")
        .output()
        .await
        .expect("chanvoy daemon stop");
    if !status.status.success() {
        eprintln!(
            "daemon stop exited non-zero: {}\nstderr={}",
            status.status,
            String::from_utf8_lossy(&status.stderr)
        );
    }
    let deadline = std::time::Instant::now() + SHUTDOWN_TIMEOUT;
    while std::time::Instant::now() < deadline {
        if !env.socket_path().exists() {
            let _ = child.wait().await;
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = child.kill().await;
    false
}

/// SIGKILL the daemon and wait for the process to exit. Socket may be left
/// orphaned; `daemon::start()` cleans up stale sockets on subsequent spawn.
async fn kill_daemon(mut child: Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
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
