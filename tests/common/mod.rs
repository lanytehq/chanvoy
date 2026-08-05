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

use chanvoy_core::{AttentionState, Profile, ReducePolicy};
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
    /// PER-035: extra env vars injected child-only into every
    /// `chanvoy_command`. Used to supply a *second* bot token (the
    /// family identity a stream profile reduces to) so the daemon can
    /// build its reduce writer at startup. Empty by default.
    pub extra_env: Vec<(String, String)>,
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
            extra_env: Vec::new(),
        }
    }

    /// PER-035: register an additional env var (e.g. a family-bot token)
    /// injected child-only into every subsequent `chanvoy_command`.
    pub fn set_extra_env(&mut self, name: &str, value: &str) {
        self.extra_env.push((name.to_string(), value.to_string()));
    }

    /// PER-035: write an arbitrarily-named profile (the default
    /// `write_default_profile` always uses `self.profile_name`). Used to
    /// materialize the *family* profile a stream profile reduces to, and
    /// to set a `[reduce]` policy on the stream profile. `env_name` lets
    /// the family profile read a different token env than the stream's.
    pub fn write_named_profile(
        &self,
        name: &str,
        bot_username: &str,
        team_name: &str,
        env_name: &str,
        reduce_use_profile: Option<&str>,
    ) {
        let profile = Profile {
            name: name.to_string(),
            role: "dataeng".to_string(),
            scope: "galaxy".to_string(),
            provider: chanvoy_core::Provider::Mattermost,
            bot_username: bot_username.to_string(),
            team_name: team_name.to_string(),
            server_url: self.server_url(),
            env_name: env_name.to_string(),
            env_file: None,
            credential_mode: chanvoy_core::CredentialMode::EnvName,
            capability_class: chanvoy_core::CapabilityClass::Standard,
            monitored_channels: Vec::new(),
            ipc: None,
            reduce: reduce_use_profile.map(|p| ReducePolicy {
                use_profile: p.to_string(),
            }),
        };
        let dir = self.chanvoy_config_dir().join("profiles");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = dir.join(format!("{name}.toml"));
        let mut file = std::fs::File::create(path).unwrap();
        file.write_all(toml::to_string_pretty(&profile).unwrap().as_bytes())
            .unwrap();
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
    /// `monitored_channels` defaults to empty.
    pub fn write_default_profile(&self, bot_username: &str, team_name: &str) {
        self.write_profile_with_monitored(bot_username, team_name, &[]);
    }

    /// Write a profile with explicit `monitored_channels`. Used by tests
    /// that need to exercise the tracked-but-uncursored path (PER-008B
    /// `attention list` union semantics — devrev finding 2026-04-22).
    pub fn write_profile_with_monitored(
        &self,
        bot_username: &str,
        team_name: &str,
        monitored_channels: &[&str],
    ) {
        self.write_profile_against(
            bot_username,
            team_name,
            monitored_channels,
            &self.server_url(),
        );
    }

    /// Write the default profile pointed at an arbitrary provider URL
    /// instead of the harness's mock server directly.
    ///
    /// Used by tests that stand a fault-injecting front end in front of
    /// the mock so a specific request can be made to fail at the
    /// transport layer — something wiremock, which always answers with
    /// a status, cannot express.
    pub fn write_profile_against_server(
        &self,
        bot_username: &str,
        team_name: &str,
        server_url: &str,
    ) {
        self.write_profile_against(bot_username, team_name, &[], server_url);
    }

    fn write_profile_against(
        &self,
        bot_username: &str,
        team_name: &str,
        monitored_channels: &[&str],
        server_url: &str,
    ) {
        let profile = Profile {
            name: self.profile_name.clone(),
            role: "bravo-devlead".to_string(),
            scope: "lanytehq".to_string(),
            provider: chanvoy_core::Provider::Mattermost,
            bot_username: bot_username.to_string(),
            team_name: team_name.to_string(),
            server_url: server_url.to_string(),
            env_name: self.token_env_name.clone(),
            env_file: None,
            credential_mode: chanvoy_core::CredentialMode::EnvName,
            capability_class: chanvoy_core::CapabilityClass::Standard,
            monitored_channels: monitored_channels.iter().map(|s| s.to_string()).collect(),
            ipc: None,
            reduce: None,
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
    ///
    /// Live-shaped: a real Mattermost post object carries `user_id` and
    /// no author name at all, so the mocked post JSON omits `username`
    /// and the name is served from a `GET /users/{user_id}` mount
    /// instead — the same two-step chanvoy has to perform against a
    /// real server. Injecting the name straight into the post body (as
    /// this helper used to) let author-resolution bugs pass the suite.
    pub async fn mock_channel_posts(
        &self,
        channel_id: &str,
        posts: &[(&str, &str, &str, &str, i64)],
    ) {
        let body = serde_json::json!({
            "posts": posts.iter().map(|(id, user_id, _username, message, create_at)| {
                (
                    (*id).to_string(),
                    serde_json::json!({
                        "id": id,
                        "channel_id": channel_id,
                        "user_id": user_id,
                        "message": message,
                        "create_at": create_at,
                        "root_id": "",
                    }),
                )
            }).collect::<serde_json::Map<_, _>>()
        });
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{channel_id}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&self.mock)
            .await;
        let mut mounted: Vec<&str> = Vec::new();
        for (_, user_id, username, _, _) in posts {
            if mounted.contains(user_id) {
                continue;
            }
            mounted.push(user_id);
            self.mock_user_lookup(user_id, username).await;
        }
    }

    /// `GET /users/{user_id}` returning that user's name — the lookup
    /// chanvoy makes to put an author name on a post.
    pub async fn mock_user_lookup(&self, user_id: &str, username: &str) {
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{user_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": user_id,
                "username": username,
            })))
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

    /// PER-034: like `mock_post_lookup` (exists=true) but also sets the
    /// `is_pinned` field so `fetch_post_pinned_state` returns the
    /// expected pre-call pin state. Used by pin/unpin idempotency
    /// tests where the `was_already_pinned` / `was_already_unpinned`
    /// JSON output reflects this value.
    pub async fn mock_post_lookup_pinned(&self, post_id: &str, channel_id: &str, is_pinned: bool) {
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/posts/{post_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": post_id,
                "channel_id": channel_id,
                "is_pinned": is_pinned,
            })))
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

    /// Construct an `AutoSetupDaemonGuard` for this env's profile. The
    /// returned guard's Drop reads the pid file and force-kills any
    /// surviving daemon process. Pair with auto-setup invocations so a
    /// panicking test doesn't leak a detached daemon onto the dev
    /// machine (load-bearing once PER-008D's setsid landed).
    pub fn daemon_guard(&self) -> AutoSetupDaemonGuard {
        AutoSetupDaemonGuard {
            pid_path: self
                .chanvoy_runtime_dir()
                .join(format!("{}.pid", self.profile_name)),
        }
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
        // PER-035: inject any extra tokens (e.g. the family-bot token a
        // stream profile reduces to) child-only.
        for (name, value) in &self.extra_env {
            cmd.env(name, value);
        }
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

/// PER-036: run a `chanvoy` CLI subcommand with `stdin_input` piped to
/// its stdin (for the `-` stdin-message convention). Writes the bytes,
/// closes stdin, then collects output.
pub async fn run_chanvoy_with_stdin(
    env: &TestEnv,
    args: &[&str],
    stdin_input: &[u8],
) -> std::process::Output {
    use tokio::io::AsyncWriteExt;
    let mut child = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn chanvoy cli with piped stdin");
    {
        let mut stdin = child.stdin.take().expect("child stdin piped");
        stdin.write_all(stdin_input).await.expect("write stdin");
        // Drop closes the pipe → EOF for the child's read_to_string.
    }
    child
        .wait_with_output()
        .await
        .expect("collect chanvoy output")
}

/// PER-036: the `message` field of the first `POST /api/v4/posts` the
/// mock recorded. Panics if no post was made.
pub async fn posted_message_body(env: &TestEnv) -> String {
    let requests = env.mock.received_requests().await.unwrap_or_default();
    let req = requests
        .iter()
        .find(|r| r.method.as_str().eq_ignore_ascii_case("POST") && r.url.path() == "/api/v4/posts")
        .expect("a POST /api/v4/posts request was recorded");
    let body: serde_json::Value = serde_json::from_slice(&req.body).expect("post body is JSON");
    body["message"]
        .as_str()
        .expect("post body has a string `message` field")
        .to_string()
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

/// Sync-Drop guard for daemons spawned by `chanvoy auto-setup`. Once
/// PER-008D's setsid detachment lands, an auto-setup-spawned daemon
/// truly survives its parent — including a panicking test process.
/// Without an RAII cleanup, a single panic between auto-setup and the
/// explicit `teardown_auto_setup_daemon` call leaks a real backgrounded
/// daemon onto the dev machine. The guard reads the pid file and
/// `libc::kill(pid, SIGKILL)`s on Drop. Sync-only because Drop cannot
/// be async; ESRCH (no such process — happy-path teardown beat us to
/// it) is silently swallowed.
///
/// Usage in tests:
/// ```ignore
/// let env = TestEnv::new("per-008d-...").await;
/// // ... auto-setup runs, spawns the detached daemon ...
/// let _guard = env.daemon_guard();
/// // ... rest of test; on panic the guard's Drop kills the daemon
/// ```
pub struct AutoSetupDaemonGuard {
    pid_path: PathBuf,
}

impl Drop for AutoSetupDaemonGuard {
    fn drop(&mut self) {
        let Ok(contents) = std::fs::read_to_string(&self.pid_path) else {
            return;
        };
        let Ok(pid) = contents.trim().parse::<i32>() else {
            return;
        };
        // SAFETY: `libc::kill` with SIGKILL is async-signal-safe and
        // takes only a pid + signal. We are NOT in a post-fork context
        // here — Drop runs in normal Rust code. The unsafe is required
        // only because libc::kill is an FFI call. SIGKILL is harmless
        // when the target pid no longer exists (ESRCH); we don't read
        // errno because the failure mode is acceptable on the happy
        // path (explicit teardown ran first).
        unsafe {
            let _ = libc::kill(pid, libc::SIGKILL);
        }
    }
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
