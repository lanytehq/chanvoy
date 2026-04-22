//! Shared integration-test harness primitives for chanvoy.
//!
//! Both `restart_harness.rs` (PER-008C) and `attention_inspection.rs`
//! (PER-008B) mount this via `mod common;` to avoid duplicating the
//! daemon-spawn / mock-server / env-isolation scaffolding. Consumer
//! files use `#![allow(dead_code)]` at the file level (integration
//! tests compile each file as a separate binary; unused helpers in one
//! file trigger dead-code lints even when consumed by another).
//!
//! Isolation model:
//! - `CHANVOY_CONFIG_DIR` / `CHANVOY_RUNTIME_DIR` overrides passed
//!   child-only (chanvoy-core honors them before the platform default;
//!   macOS `dirs::config_dir()` does not respect `XDG_CONFIG_HOME`)
//! - Parent-process env is never mutated
//! - Unique per-test `--profile` slug prevents socket / pid / state
//!   filename collisions under parallel execution
//! - Long-lived `wiremock::MockServer` per test; `reset_mocks()` between
//!   phases prevents phase-1 responders from satisfying phase-2 asserts
//!
//! SIGKILL delivery uses `sysprims_signal::force_kill` — the tokio
//! `Child::start_kill` path has observed delivery gaps on macOS in test
//! contexts. Child reap uses `child.wait()` / `try_wait()` as the
//! authoritative platform-agnostic exit signal (Linux zombie semantics
//! fool `sysprims_proc::get_process` as a liveness probe — see
//! `lanytehq/.plans/memos/20260421-zombie-liveness-predicate-gap.md`).

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chanvoy_core::{AttentionState, Profile};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Path to the workspace-root `chanvoy` binary under test. Cargo sets
/// this env var for integration tests.
pub const CHANVOY_BIN: &str = env!("CARGO_BIN_EXE_chanvoy");

pub const SPAWN_READY_TIMEOUT: Duration = Duration::from_secs(8);
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Isolated filesystem + mock-server environment for one integration test.
pub struct TestEnv {
    config_dir: TempDir,
    runtime_dir: TempDir,
    pub mock: MockServer,
    pub profile_name: String,
    pub token_env_name: String,
    pub token_value: String,
}

impl TestEnv {
    pub async fn new(profile_name: &str) -> Self {
        Self {
            config_dir: tempfile::tempdir().expect("tempdir config"),
            runtime_dir: tempfile::tempdir().expect("tempdir runtime"),
            mock: MockServer::start().await,
            profile_name: profile_name.to_string(),
            token_env_name: "LANYTE_MM_TOKEN".to_string(),
            token_value: "test-token-value".to_string(),
        }
    }

    pub fn server_url(&self) -> String {
        self.mock.uri()
    }

    pub fn config_dir(&self) -> &Path {
        self.config_dir.path()
    }

    pub fn runtime_dir(&self) -> &Path {
        self.runtime_dir.path()
    }

    pub fn chanvoy_config_dir(&self) -> PathBuf {
        self.config_dir().to_path_buf()
    }

    pub fn chanvoy_runtime_dir(&self) -> PathBuf {
        self.runtime_dir().to_path_buf()
    }

    pub fn profile_path(&self) -> PathBuf {
        self.chanvoy_config_dir()
            .join("profiles")
            .join(format!("{}.toml", self.profile_name))
    }

    pub fn state_path(&self) -> PathBuf {
        self.chanvoy_config_dir()
            .join(format!("state-{}.json", self.profile_name))
    }

    pub fn socket_path(&self) -> PathBuf {
        self.chanvoy_runtime_dir()
            .join(format!("{}.sock", self.profile_name))
    }

    /// Write a default profile TOML pointing at the mock server.
    pub fn write_default_profile(&self, bot_username: &str, team_name: &str) {
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

    /// Baseline Mattermost mocks for daemon startup (whoami + team lookup).
    /// Tests add channel / post mocks on top.
    pub async fn mock_baseline(&self, bot_id: &str, bot_username: &str, team_id: &str) {
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
            .and(path("/api/v4/teams/name/org-lanytehq"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"id": team_id, "name": "org-lanytehq"})),
            )
            .mount(&self.mock)
            .await;
    }

    pub async fn reset_mocks(&self) {
        self.mock.reset().await;
    }

    /// Channel-by-name lookup for the default team id used by
    /// `mock_baseline` (`team-id-456`). Override `team_id` in the
    /// baseline call if tests need a different team id.
    pub async fn mock_channel_lookup(&self, channel_name: &str, channel_id: &str) {
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

    /// Channel-by-name lookup for an explicit team id (useful when
    /// different tests use different team ids).
    pub async fn mock_channel_lookup_for_team(
        &self,
        team_id: &str,
        channel_name: &str,
        channel_id: &str,
    ) {
        Mock::given(method("GET"))
            .and(path(format!(
                "/api/v4/teams/{team_id}/channels/name/{channel_name}"
            )))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"id": channel_id, "name": channel_name})),
            )
            .mount(&self.mock)
            .await;
    }

    /// `POST /posts` returning the given post id (201 Created).
    pub async fn mock_post_create(&self, post_id: &str) {
        Mock::given(method("POST"))
            .and(path("/api/v4/posts"))
            .respond_with(
                ResponseTemplate::new(201).set_body_json(serde_json::json!({"id": post_id})),
            )
            .mount(&self.mock)
            .await;
    }

    /// `GET /channels/{channel_id}/posts` returning the given messages.
    /// `posts`: list of `(id, user_id, username, message, create_at)`.
    pub async fn mock_channel_posts(
        &self,
        channel_id: &str,
        posts: &[(&str, &str, &str, &str, i64)],
    ) {
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

    /// `GET /posts/{post_id}` — exists=true returns 200, exists=false
    /// returns 404 (triggers `CoreError::AnchorNotFound` in
    /// `assert_post_in_channel` / stale-cursor detection).
    pub async fn mock_post_lookup(&self, post_id: &str, channel_id: &str, exists: bool) {
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

    /// `GET /users/me/teams/{team_id}/channels` returning an empty
    /// membership list. `seed_cursors` with empty memberships produces
    /// no outcomes, so auto-setup exits 0 cleanly without needing
    /// per-channel HEAD mocks.
    pub async fn mock_empty_memberships(&self, team_id: &str) {
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/me/teams/{team_id}/channels")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&self.mock)
            .await;
    }

    /// Build a `chanvoy` command with this env's isolation. Parent env
    /// is untouched; all path overrides + token go child-only.
    pub fn chanvoy_command(&self) -> Command {
        let mut cmd = Command::new(CHANVOY_BIN);
        cmd.env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("CHANVOY_CONFIG_DIR", self.chanvoy_config_dir())
            .env("CHANVOY_RUNTIME_DIR", self.chanvoy_runtime_dir())
            .env(&self.token_env_name, &self.token_value);
        cmd
    }
}

/// Run a `chanvoy` CLI subcommand and return the full output.
pub async fn run_chanvoy(env: &TestEnv, args: &[&str]) -> std::process::Output {
    env.chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(args)
        .output()
        .await
        .expect("spawn chanvoy cli")
}

/// Read the daemon-persisted attention state file. Returns None if
/// absent. Uses direct file read so assertions don't depend on
/// parent-process env or chanvoy-core's dir resolution.
pub fn read_attention_state(env: &TestEnv) -> Option<AttentionState> {
    let path = env.state_path();
    if !path.exists() {
        return None;
    }
    let contents = std::fs::read_to_string(&path).expect("read state file");
    Some(serde_json::from_str(&contents).expect("parse state file"))
}

/// Read the raw bytes of the attention-state file. Used by PER-008B's
/// non-mutation invariant tests (snapshot-compare across a read-only
/// CLI call). Returns None if the file does not exist.
pub fn read_attention_state_bytes(env: &TestEnv) -> Option<Vec<u8>> {
    let path = env.state_path();
    if !path.exists() {
        return None;
    }
    Some(std::fs::read(&path).expect("read state file bytes"))
}

/// Spawn the daemon via `chanvoy daemon serve` and wait until it is
/// actually serving (probed via `chanvoy daemon status` RPC, not
/// socket-file presence). Fails fast if the child exits before
/// readiness.
pub async fn spawn_daemon(env: &TestEnv) -> Child {
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
                "daemon child exited before readiness (status={status:?}); profile={}",
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

/// Probe daemon readiness via `chanvoy daemon status`. Exit 0 = serving.
pub async fn daemon_serving(env: &TestEnv) -> bool {
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

/// Clean shutdown via `chanvoy daemon stop` with bounded `try_wait`
/// polling. Force-kills on timeout as safety net.
pub async fn stop_daemon_cleanly(env: &TestEnv, mut child: Child) -> bool {
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
    if let Some(pid) = child.id() {
        let _ = sysprims_signal::force_kill(pid);
    }
    let _ = child.wait().await;
    false
}

/// SIGKILL the daemon via `sysprims_signal::force_kill` and reap via
/// `child.wait()` with timeout. Platform-agnostic exit signal (Linux
/// zombie semantics make `sysprims_proc::get_process` unreliable as a
/// pre-wait liveness probe).
pub async fn kill_daemon(mut child: Child) {
    let pid = child.id().expect("child pid present before kill");
    if let Err(err) = sysprims_signal::force_kill(pid) {
        panic!("kill_daemon: sysprims force_kill({pid}) failed: {err}");
    }
    match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(_status)) => {}
        Ok(Err(err)) => panic!("kill_daemon: wait errored for pid {pid}: {err}"),
        Err(_) => panic!(
            "kill_daemon: pid {pid} not reaped within 5s of force_kill → signal-delivery or reactor failure"
        ),
    }
}
