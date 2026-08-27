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

mod common;

use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use chanvoy_core::{rpc_result, JsonRpcRequest, Profile};
use common::{
    kill_daemon, read_attention_state, run_chanvoy, spawn_daemon, stop_daemon_cleanly,
    wait_for_ws_failure, TestEnv,
};
use tokio::process::Command;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
        .get("org-lanytehq/test-channel")
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
        .get("org-lanytehq/test-channel")
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
            .get("org-lanytehq/test-channel")
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

// ---- Phase 3: PER-009 lifecycle-primitive coverage via `chanvoy auto-setup` ----
//
// These tests drive the real `chanvoy auto-setup` CLI path (not direct
// helper calls into chanvoy-cli internals) so the F5/F6/F7 regressions
// from PER-009 review are covered end-to-end. Per devrev's nit on the
// assignment — the intent is catching bugs at the dispatch boundary, so
// the harness invokes `chanvoy auto-setup` rather than calling
// `handle_auto_setup` in-process.

/// Build a `chanvoy auto-setup` command with the minimum env that
/// `build_desired_profile_from_env` requires.
fn auto_setup_command(env: &TestEnv, scope: &str, role: &str) -> Command {
    let mut cmd = env.chanvoy_command();
    cmd.env("LANYTE_AGENT_ROLE", role)
        .env("LANYTE_AGENT_SCOPE", scope)
        .env("LANYTE_MM_URL", env.server_url())
        .env("LANYTE_MM_TEAM", "org-lanytehq")
        .env("CHANVOY_PROFILE", &env.profile_name)
        .arg("--json")
        .arg("auto-setup");
    cmd
}

/// Read the live daemon's pid from the runtime-dir pid file, if present.
fn read_daemon_pid(env: &TestEnv) -> Option<u32> {
    let pid_path = env
        .chanvoy_runtime_dir()
        .join(format!("{}.pid", env.profile_name));
    let contents = std::fs::read_to_string(&pid_path).ok()?;
    contents.trim().parse().ok()
}

/// Best-effort teardown for auto-setup-spawned daemons (which are detached
/// children, not tracked via a `Child` handle). Reads the pid file and
/// force-kills if the pid is still live.
async fn teardown_auto_setup_daemon(env: &TestEnv) {
    let Some(pid) = read_daemon_pid(env) else {
        return;
    };
    if sysprims_proc::get_process(pid).is_ok() {
        let _ = sysprims_signal::force_kill(pid);
        for _ in 0..10 {
            if sysprims_proc::get_process(pid).is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn clean_fake_daemon(env: &TestEnv) -> tokio::task::JoinHandle<()> {
    let socket_path = env.socket_path();
    let listener = UnixListener::bind(&socket_path).expect("bind clean fake daemon");
    let profile = env.profile_name.clone();
    let socket_json = socket_path.clone();
    tokio::spawn(async move {
        for expected in ["daemon_status", "seed_cursors"] {
            let (stream, _) = listener.accept().await.expect("accept fake daemon client");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .await
                .expect("read fake daemon request");
            let request: JsonRpcRequest =
                serde_json::from_str(line.trim_end()).expect("decode fake daemon request");
            assert_eq!(request.method, expected);
            let value = if expected == "daemon_status" {
                serde_json::json!({
                    "profile_name": profile,
                    "socket_path": socket_json,
                    "mattermost_username": "agent-bravo-devlead",
                    "mattermost_ok": true,
                    "ws_connection_state": "healthy",
                    "ws_last_error": null,
                    "ws_reconnect_count": 0,
                    "health": "healthy",
                    "mattermost_identity_drift": false,
                    "binary": chanvoy_core::resolve_host_build_info(),
                })
            } else {
                serde_json::json!({"outcomes": []})
            };
            let response = rpc_result(request.id, value);
            writer
                .write_all(
                    format!(
                        "{}\n",
                        serde_json::to_string(&response).expect("encode response")
                    )
                    .as_bytes(),
                )
                .await
                .expect("write fake daemon response");
        }
        let _ = std::fs::remove_file(socket_path);
    })
}

#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn auto_setup_reuses_clean_generation_matched_daemon_without_pid_change() {
    let env = TestEnv::new("auto-setup-ws-clean").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-clean", "agent-bravo-devlead", "team-id-clean")
        .await;
    env.mock_empty_memberships("team-id-clean").await;
    std::fs::create_dir_all(env.chanvoy_runtime_dir()).expect("runtime dir");
    let pid_path = env
        .chanvoy_runtime_dir()
        .join(format!("{}.pid", env.profile_name));
    std::fs::write(&pid_path, "424242").expect("sentinel pid");
    let server = clean_fake_daemon(&env).await;

    let output = auto_setup_command(&env, "lanytehq", "bravo-devlead")
        .output()
        .await
        .expect("auto-setup");
    assert!(
        output.status.success(),
        "clean daemon must reuse; stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("auto-setup JSON");
    assert_eq!(report["daemon_state"], "already_running");
    assert_eq!(
        std::fs::read_to_string(&pid_path).expect("sentinel pid retained"),
        "424242",
        "reuse must not cycle the daemon"
    );
    server.await.expect("fake daemon completed");
}

/// A generation-matched, identity-ownable daemon with a current websocket
/// failure must never be reported as reused/successful. The HTTP-only test
/// provider cannot establish a healthy replacement websocket, so this pins
/// the permitted refusal branch after auto-setup cycles the failed daemon.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn auto_setup_refuses_success_when_ownable_ws_repair_cannot_recover() {
    let env = TestEnv::new("auto-setup-ws-degraded").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ws", "agent-bravo-devlead", "team-id-ws")
        .await;
    env.mock_empty_memberships("team-id-ws").await;

    let mut daemon = spawn_daemon(&env).await;
    wait_for_ws_failure(&env).await;
    let old_pid = read_daemon_pid(&env).expect("failed daemon pid");

    let output = tokio::time::timeout(
        Duration::from_secs(20),
        auto_setup_command(&env, "lanytehq", "bravo-devlead").output(),
    )
    .await
    .expect("auto-setup repair remains bounded")
    .expect("auto-setup subprocess");
    assert_eq!(
        output.status.code(),
        Some(4),
        "failed WS repair must not report reuse/success; stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("auto-setup error JSON");
    assert_eq!(report["error_code"], "daemon_start");
    assert!(report["message"]
        .as_str()
        .is_some_and(|message| message.contains("did not restore healthy websocket")));
    tokio::time::timeout(Duration::from_secs(2), daemon.wait())
        .await
        .expect("the known-degraded daemon exits within the cleanup bound")
        .expect("wait for known-degraded daemon");
    assert!(
        sysprims_proc::get_process(old_pid).is_err(),
        "the known-degraded daemon must have been stopped and reaped"
    );
    assert!(
        !env.socket_path().exists(),
        "an unsuccessful replacement must be cleaned up"
    );
}

/// Read the persisted profile TOML for this env's profile.
fn read_persisted_profile(env: &TestEnv) -> Profile {
    let path = env
        .chanvoy_config_dir()
        .join("profiles")
        .join(format!("{}.toml", env.profile_name));
    let contents = std::fs::read_to_string(&path).expect("profile file present");
    toml::from_str(&contents).expect("profile parses")
}

/// F5 — `stop_daemon_if_present` stale-socket subcase.
///
/// Simulates a prior daemon that crashed leaving its socket file behind
/// (common on machine crash / OOM kill). `auto-setup` must recover without
/// hanging: `stop_daemon_if_present` sees the socket, issues a shutdown
/// RPC which fails with `NotRunning` (connect refused), treats that as
/// no-op, and proceeds. `ensure_daemon_running` then spawns a fresh
/// daemon which `daemon::start()` unblocks by removing the stale socket
/// before binding.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn auto_setup_recovers_from_stale_socket() {
    let env = TestEnv::new("per-008c-ph3-stale-socket").await;
    env.mock_baseline("bot-id-ph3a", "agent-bravo-devlead", "team-id-ph3a")
        .await;
    env.mock_empty_memberships("team-id-ph3a").await;

    // Plant a stale socket: bind + drop leaves the socket inode on disk
    // mirroring what a crashed-daemon orphan looks like.
    let socket = env.socket_path();
    std::fs::create_dir_all(socket.parent().unwrap()).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    drop(listener);
    assert!(
        socket.exists(),
        "stale socket must be planted before auto-setup"
    );

    let out = auto_setup_command(&env, "lanytehq", "bravo-devlead")
        .output()
        .await
        .expect("auto-setup subprocess");
    assert!(
        out.status.success(),
        "auto-setup must exit 0 through stale-socket recovery; \
         exit={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let _guard = env.daemon_guard();
    let pid = read_daemon_pid(&env).expect("fresh daemon pid file present");
    assert!(
        sysprims_proc::get_process(pid).is_ok(),
        "fresh daemon pid {pid} must be live post-recovery"
    );

    teardown_auto_setup_daemon(&env).await;
}

/// F6 — `ensure_daemon_running` zombie-stop path.
///
/// Scenario: daemon1 is healthy after the first auto-setup. We then
/// SIGSTOP daemon1 — process alive, socket file present, but no longer
/// accepting connections (modeling an auth-broken or hung daemon). The
/// second auto-setup's `ensure_daemon_running` must detect this via
/// socket-presence (not ping!) and stop daemon1 before spawning daemon2.
/// Without the PER-009 F5/F6 fix we'd spawn alongside the zombie —
/// two-daemons-one-profile.
///
/// Assertion anchor: pid in the pid file changes across the second
/// auto-setup invocation — zombie was stopped, replacement started.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn auto_setup_stops_zombie_and_respawns() {
    let env = TestEnv::new("per-008c-ph3-zombie").await;
    env.mock_baseline("bot-id-ph3b", "agent-bravo-devlead", "team-id-ph3b")
        .await;
    env.mock_empty_memberships("team-id-ph3b").await;

    let out1 = auto_setup_command(&env, "lanytehq", "bravo-devlead")
        .output()
        .await
        .expect("auto-setup #1");
    assert!(
        out1.status.success(),
        "first auto-setup must succeed; stderr={}",
        String::from_utf8_lossy(&out1.stderr)
    );
    let _guard = env.daemon_guard();
    let pid_before = read_daemon_pid(&env).expect("daemon pid after first auto-setup");
    assert!(sysprims_proc::get_process(pid_before).is_ok());

    // SIGSTOP daemon1: process alive, socket live, but cannot answer RPCs.
    // sysprims curates to a safe signal subset that excludes SIGSTOP
    // (reasonable — it's a test-only footgun), so we shell out via `kill`
    // for the stop/resume simulation.
    assert!(
        std::process::Command::new("kill")
            .args(["-STOP", &pid_before.to_string()])
            .status()
            .expect("kill -STOP spawn")
            .success(),
        "SIGSTOP daemon1 pid={pid_before}"
    );

    // Second auto-setup: detect stopped daemon via socket-presence path,
    // stop it (force-kill fallback if shutdown RPC stalls on the paused
    // process), and spawn a fresh daemon.
    let out2_result = tokio::time::timeout(
        Duration::from_secs(30),
        auto_setup_command(&env, "lanytehq", "bravo-devlead").output(),
    )
    .await;
    // Always resume daemon1 so teardown can reap it cleanly. On the happy
    // path the second auto-setup's force-kill fallback has already reaped
    // daemon1, so this CONT typically hits a nonexistent pid — that's
    // expected, and both stdout and stderr are silenced so the resulting
    // `kill: <pid>: No such process` doesn't clutter green test output.
    let _ = std::process::Command::new("kill")
        .args(["-CONT", &pid_before.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    let out2 = out2_result
        .expect("auto-setup #2 must complete within 30s")
        .expect("subprocess run");
    assert!(
        out2.status.success(),
        "second auto-setup must recover from zombie daemon; exit={} stderr={}",
        out2.status,
        String::from_utf8_lossy(&out2.stderr)
    );

    let pid_after = read_daemon_pid(&env).expect("daemon pid after zombie recovery");
    assert_ne!(
        pid_before, pid_after,
        "zombie was stopped and a fresh daemon started → pid must change"
    );
    assert!(
        sysprims_proc::get_process(pid_after).is_ok(),
        "replacement daemon pid {pid_after} must be live"
    );
    // Verify the zombie is actually reaped (not still running in parallel).
    let poll_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < poll_deadline {
        if sysprims_proc::get_process(pid_before).is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        sysprims_proc::get_process(pid_before).is_err(),
        "zombie daemon pid {pid_before} must be reaped after zombie-stop"
    );

    teardown_auto_setup_daemon(&env).await;
}

/// F7 — Reuse→Refreshed bot_username promotion.
///
/// auto-setup runs once with whoami=bot-alpha; profile persisted with
/// bot_username=bot-alpha. Then whoami flips to bot-beta (token rotated
/// to a different bot account). Second auto-setup's decide_profile_action
/// routes to Reuse (name/role/scope/server_url/team_name unchanged), but
/// validate_and_finalize_profile returns `bot_username=bot-beta`. The
/// Reuse arm detects the divergence, promotes to refresh semantics,
/// persists the updated profile, and restarts the daemon so it uses the
/// env-current credential.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn auto_setup_promotes_reuse_to_refreshed_on_bot_username_drift() {
    let env = TestEnv::new("per-008c-ph3-botdrift").await;
    env.mock_baseline("bot-id-alpha", "bot-alpha", "team-id-ph3c")
        .await;
    env.mock_empty_memberships("team-id-ph3c").await;

    let out1 = auto_setup_command(&env, "lanytehq", "bravo-devlead")
        .output()
        .await
        .expect("auto-setup #1");
    assert!(
        out1.status.success(),
        "first auto-setup must succeed; stderr={}",
        String::from_utf8_lossy(&out1.stderr)
    );
    let _guard = env.daemon_guard();
    let profile_before = read_persisted_profile(&env);
    assert_eq!(profile_before.bot_username, "bot-alpha");
    let pid_before = read_daemon_pid(&env).expect("pid after first auto-setup");

    // Flip whoami to bot-beta; team + memberships stay the same.
    env.reset_mocks().await;
    env.mock_baseline("bot-id-beta", "bot-beta", "team-id-ph3c")
        .await;
    env.mock_empty_memberships("team-id-ph3c").await;

    let out2 = auto_setup_command(&env, "lanytehq", "bravo-devlead")
        .output()
        .await
        .expect("auto-setup #2");
    assert!(
        out2.status.success(),
        "second auto-setup must succeed (reuse→refreshed promotion); \
         exit={} stderr={}",
        out2.status,
        String::from_utf8_lossy(&out2.stderr)
    );

    let profile_after = read_persisted_profile(&env);
    assert_eq!(
        profile_after.bot_username, "bot-beta",
        "bot_username drift must be persisted under the promotion path"
    );

    let pid_after = read_daemon_pid(&env).expect("pid after bot_username promotion");
    assert_ne!(
        pid_before, pid_after,
        "bot_username drift must restart the daemon so it uses the env-current credential"
    );
    assert!(
        sysprims_proc::get_process(pid_after).is_ok(),
        "post-promotion daemon pid {pid_after} must be live"
    );

    let report: serde_json::Value =
        serde_json::from_slice(&out2.stdout).expect("json report parses");
    assert_eq!(
        report["profile_state"].as_str(),
        Some("refreshed"),
        "reuse→refreshed promotion must surface profile_state=refreshed; report={report}"
    );
    let diff = report["refresh_diff"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        diff.iter()
            .any(|entry| entry["field"].as_str() == Some("bot_username")
                && entry["from"].as_str() == Some("bot-alpha")
                && entry["to"].as_str() == Some("bot-beta")),
        "refresh_diff must include the bot_username transition; report={report}"
    );

    teardown_auto_setup_daemon(&env).await;
}

// ---- Phase 4: PER-008D detachment ----
//
// Auto-setup must spawn a daemon that survives the spawning shell's
// termination. Implementation lands `setsid(2)` via `pre_exec` on the
// `Command` built in `ensure_daemon_running` (chanvoy-cli). These
// tests verify the structural detachment without requiring a pty:
//
// - the daemon is its own session leader (`getsid(daemon_pid) ==
//   daemon_pid`) — direct proof setsid took effect, uniform across
//   Linux init / systemd-user / macOS launchd
// - the daemon is reparented away from the intermediate spawning
//   process — corroborating evidence that auto-setup's CLI
//   subprocess exited and the daemon is no longer in its process tree
//
// PER-008C Phase 3 tests pass without detachment because the test
// harness has no controlling terminal (no TTY → no SIGHUP). Phase 4
// asserts the structural detachment that operators need in real
// shell sessions.

/// AC #1/#3 — daemon survives the auto-setup invocation that spawned
/// it AND is reachable from a fresh CLI invocation. Verifies the
/// load-bearing setsid contract via getsid + ppid checks.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn auto_setup_daemon_detaches_into_new_session() {
    let env = TestEnv::new("per-008d-detachment-newsession").await;
    env.mock_baseline("bot-id-d1", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_empty_memberships("team-id-456").await;

    // Spawn auto-setup as our intermediate process. We use spawn() +
    // wait() (rather than output().await) so we can capture the
    // intermediate's pid for the reparenting assertion below.
    let mut intermediate = env
        .chanvoy_command()
        .env("LANYTE_AGENT_ROLE", "bravo-devlead")
        .env("LANYTE_AGENT_SCOPE", "lanytehq")
        .env("LANYTE_MM_URL", env.server_url())
        .env("LANYTE_MM_TEAM", "org-lanytehq")
        .env("CHANVOY_PROFILE", &env.profile_name)
        .arg("--json")
        .arg("auto-setup")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn intermediate auto-setup");
    let intermediate_pid: u32 = intermediate.id().expect("intermediate pid");
    let exit = intermediate.wait().await.expect("intermediate wait");
    assert!(
        exit.success(),
        "intermediate auto-setup must exit 0 (daemon spawn happens during this run)"
    );

    let daemon_pid = read_daemon_pid(&env).expect("daemon pid file present after auto-setup");
    let _guard = env.daemon_guard();

    // SETSID PROOF: daemon must be its own session leader. setsid(2)
    // makes the calling process the leader of a new session whose
    // session id equals its pid. If detachment did not happen, the
    // daemon would be in the intermediate's session (sid = intermediate
    // ancestor's pid).
    //
    // SAFETY: libc::getsid is a normal syscall, not in a post-fork
    // context. The unsafe is purely the FFI requirement.
    let daemon_sid = unsafe { libc::getsid(daemon_pid as i32) };
    assert!(
        daemon_sid > 0,
        "getsid(daemon_pid={daemon_pid}) returned {daemon_sid}; errno may indicate the process is gone"
    );
    assert_eq!(
        daemon_sid as u32, daemon_pid,
        "daemon must be its own session leader (sid == pid); got sid={daemon_sid}, pid={daemon_pid}. \
         This is the load-bearing setsid contract — without it, SIGHUP from the spawning shell's \
         terminal close propagates to the daemon."
    );

    // REPARENTING HYGIENE: after intermediate exits, daemon's ppid must
    // not equal the intermediate's pid. Robust across init / systemd-
    // user-subreaper / launchd — we don't pin to ppid==1.
    let info =
        sysprims_proc::get_process(daemon_pid).expect("daemon must be alive after detachment");
    assert_ne!(
        info.ppid, intermediate_pid,
        "daemon ppid should be reparented away from intermediate auto-setup CLI \
         (intermediate exited, daemon should be owned by init / launchd / subreaper); \
         got ppid={} intermediate_pid={intermediate_pid}",
        info.ppid
    );
    assert_ne!(
        info.ppid, daemon_pid,
        "ppid sanity: daemon cannot be its own parent"
    );

    // Reachability proof: daemon answers RPC from a fresh CLI
    // invocation, confirming session-detachment did not break the
    // socket / RPC machinery.
    let status_out = run_chanvoy(&env, &["daemon", "status"]).await;
    assert!(
        status_out.status.success(),
        "daemon must answer status RPC after detachment; stderr={}",
        String::from_utf8_lossy(&status_out.stderr)
    );

    // Explicit teardown (the guard's sync Drop is the safety net for
    // panic paths only).
    teardown_auto_setup_daemon(&env).await;
}

// ---- Phase 5: CHAN-TASK-001 `daemon start` lifecycle parity ----
//
// The Phase 4 detachment gate above was scoped to `auto-setup`. That
// scoping was load-bearing in the wrong direction: `daemon start` had a
// second, older spawn implementation that wrote no PER-014 bootstrap
// handoff and never called `setsid()`, so the suite stayed green while
// the documented `daemon start` lifecycle was non-durable. Under Codex
// (and any sandbox that tears down the invocation's process group at the
// tool-execution boundary) an operator saw a successful start receipt,
// then `Daemon(NotRunning(...sock))` from the very next invocation.
//
// These tests hold both background entry points to the same contract.
// The binding assertion is cross-invocation: the daemon must answer a
// *fresh CLI process* after the process that started it has exited.
// Nothing here sleeps for a fixed duration to "let things settle" —
// readiness is proven by process/session state and real RPCs.

/// Path to the PER-014 bootstrap handoff for this env's profile. The
/// daemon consumes-and-deletes it during startup, so its absence after a
/// successful start is evidence the child took the validated-bootstrap
/// path rather than falling back to a child-side network `whoami`.
fn bootstrap_handoff_path(env: &TestEnv) -> std::path::PathBuf {
    env.chanvoy_runtime_dir()
        .join(format!("{}.bootstrap.json", env.profile_name))
}

/// Allocate a pid that is guaranteed dead: spawn a trivial process and
/// reap it. Models the crashed-daemon residue an operator finds on disk
/// (pid file naming a process that no longer exists).
async fn reaped_pid() -> u32 {
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg("exit 0")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn throwaway process");
    let pid = child.id().expect("throwaway pid");
    child.wait().await.expect("reap throwaway process");
    pid
}

/// CHAN-TASK-001 AC — `daemon start` produces a session leader that
/// reparents away from its CLI parent and answers a fresh invocation.
///
/// This is the `daemon start` sibling of
/// `auto_setup_daemon_detaches_into_new_session`, plus the two proofs
/// that were missing from the auto-setup gate and are the actual field
/// failure: a *network-backed* verb served after the spawning CLI exited,
/// and evidence that the parent (not the detached child) did the identity
/// validation.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn daemon_start_detaches_into_new_session() {
    let env = TestEnv::new("chan-task-001-start-detachment").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-cts1", "agent-bravo-devlead", "team-id-456")
        .await;

    // Intermediate process, spawned with an explicit pid capture so the
    // reparenting assertion has something to compare against.
    let mut intermediate = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .arg("--json")
        .arg("daemon")
        .arg("start")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn intermediate daemon start");
    let intermediate_pid: u32 = intermediate.id().expect("intermediate pid");
    let exit = intermediate.wait().await.expect("intermediate wait");
    assert!(
        exit.success(),
        "intermediate `daemon start` must exit 0 (daemon spawn happens during this run)"
    );

    let daemon_pid = read_daemon_pid(&env).expect("daemon pid file present after daemon start");
    let _guard = env.daemon_guard();

    // SETSID PROOF: identical contract to the auto-setup gate. Without
    // it the daemon stays in the spawning invocation's session and dies
    // with it — the exact Codex-visible failure.
    //
    // SAFETY: libc::getsid is a normal syscall, not in a post-fork
    // context. The unsafe is purely the FFI requirement.
    let daemon_sid = unsafe { libc::getsid(daemon_pid as i32) };
    assert!(
        daemon_sid > 0,
        "getsid(daemon_pid={daemon_pid}) returned {daemon_sid}; errno may indicate the process is gone"
    );
    assert_eq!(
        daemon_sid as u32, daemon_pid,
        "daemon started by `daemon start` must be its own session leader (sid == pid); \
         got sid={daemon_sid}, pid={daemon_pid}"
    );

    let info =
        sysprims_proc::get_process(daemon_pid).expect("daemon must be alive after detachment");
    assert_ne!(
        info.ppid, intermediate_pid,
        "daemon ppid should be reparented away from the intermediate `daemon start` CLI; \
         got ppid={} intermediate_pid={intermediate_pid}",
        info.ppid
    );
    assert_ne!(
        info.ppid, daemon_pid,
        "ppid sanity: daemon cannot be its own parent"
    );

    // PARENT-SIDE VALIDATION PROOF: the handoff exists only because the
    // spawning CLI validated identity itself and passed the result down.
    // The daemon consumes-and-deletes it, so an absent file after a
    // healthy start means the child bound on the pre-validated identity
    // instead of making its own (sandbox-blocked) `whoami` call.
    assert!(
        !bootstrap_handoff_path(&env).exists(),
        "bootstrap handoff must be consumed by the daemon during startup; \
         a surviving file means the child never took the validated path"
    );

    // CROSS-INVOCATION PROOF #1: local RPC from a fresh CLI process.
    let status_out = run_chanvoy(&env, &["daemon", "status"]).await;
    assert!(
        status_out.status.success(),
        "fresh CLI invocation must reach the daemon started by a now-exited CLI; stderr={}",
        String::from_utf8_lossy(&status_out.stderr)
    );

    // CROSS-INVOCATION PROOF #2: a network-backed verb. `daemon status`
    // alone would pass against a daemon that bound its socket but has no
    // working Mattermost client; `whoami` round-trips through the daemon
    // to the mock server. This is the operation that failed in the field.
    let whoami_out = run_chanvoy(&env, &["--json", "whoami"]).await;
    assert!(
        whoami_out.status.success(),
        "fresh network-backed verb must succeed against the detached daemon; stderr={}",
        String::from_utf8_lossy(&whoami_out.stderr)
    );
    let identity: serde_json::Value =
        serde_json::from_slice(&whoami_out.stdout).expect("whoami json parses");
    assert_eq!(
        identity["username"].as_str(),
        Some("agent-bravo-devlead"),
        "daemon must serve the parent-validated identity; got {identity}"
    );

    teardown_auto_setup_daemon(&env).await;
}

/// CHAN-TASK-001 AC — stale socket + dead pid recover with no manual
/// file movement.
///
/// The field trace shows an operator hand-moving pid and socket files to
/// try to make `daemon start` work. That is not a product workflow: a
/// documented lifecycle verb must clean up after a crashed predecessor
/// itself. Plants both pieces of residue and requires a plain
/// `daemon start` to recover.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn daemon_start_recovers_from_stale_socket_and_dead_pid() {
    let env = TestEnv::new("chan-task-001-start-stale-residue").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-cts2", "agent-bravo-devlead", "team-id-456")
        .await;

    // Residue from a crashed daemon: a bound-then-dropped socket inode
    // plus a pid file naming a process that has already been reaped.
    let socket = env.socket_path();
    std::fs::create_dir_all(socket.parent().unwrap()).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    drop(listener);
    assert!(socket.exists(), "stale socket must be planted");

    let dead_pid = reaped_pid().await;
    let pid_path = env
        .chanvoy_runtime_dir()
        .join(format!("{}.pid", env.profile_name));
    std::fs::write(&pid_path, dead_pid.to_string()).expect("plant stale pid file");

    let out = run_chanvoy(&env, &["daemon", "start"]).await;
    assert!(
        out.status.success(),
        "`daemon start` must recover from stale socket + dead pid without manual file surgery; \
         exit={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let _guard = env.daemon_guard();
    let daemon_pid = read_daemon_pid(&env).expect("fresh daemon pid file present");
    assert_ne!(
        daemon_pid, dead_pid,
        "pid file must name the freshly started daemon, not the planted corpse"
    );
    assert!(
        sysprims_proc::get_process(daemon_pid).is_ok(),
        "fresh daemon pid {daemon_pid} must be live after recovery"
    );
    assert!(
        run_chanvoy(&env, &["daemon", "status"])
            .await
            .status
            .success(),
        "recovered daemon must answer a fresh invocation"
    );

    teardown_auto_setup_daemon(&env).await;
}

/// CHAN-TASK-001 AC — `daemon start` fails closed on persisted
/// bot-identity mismatch, before spawning anything, and mutates nothing.
///
/// The credential in the environment authenticates as a different bot
/// than the profile records. Pre-convergence this check lived in the
/// detached child (if it ran at all); moving it into the parent means the
/// operator sees the refusal on the command they ran. The "mutates
/// nothing" half is the other side of the convergence: `daemon start`
/// shares spawn mechanics with `auto-setup` but explicitly not its
/// profile-management semantics, so it must not "helpfully" adopt the new
/// username or move the active marker.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn daemon_start_refuses_on_bot_identity_mismatch() {
    let env = TestEnv::new("chan-task-001-start-identity-mismatch").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    // The live credential resolves to a different bot than the profile.
    env.mock_baseline("bot-id-cts3", "agent-impostor", "team-id-456")
        .await;

    let out = run_chanvoy(&env, &["daemon", "start"]).await;
    assert!(
        !out.status.success(),
        "`daemon start` must fail closed on bot-identity mismatch; stdout={}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("agent-bravo-devlead") && stderr.contains("agent-impostor"),
        "refusal must name both the expected and actual identity; stderr={stderr}"
    );

    // Nothing was spawned: no daemon, and no handoff left on disk.
    assert!(
        !env.socket_path().exists(),
        "no socket may exist — refusal must precede the child spawn"
    );
    assert!(
        read_daemon_pid(&env).is_none(),
        "no pid file may exist — refusal must precede the child spawn"
    );
    assert!(
        !bootstrap_handoff_path(&env).exists(),
        "no bootstrap handoff may be written for a refused start"
    );

    // Nothing was mutated: profile identity intact, active marker unset.
    let persisted = read_persisted_profile(&env);
    assert_eq!(
        persisted.bot_username, "agent-bravo-devlead",
        "`daemon start` must not rewrite bot_username from a live whoami"
    );
    assert!(
        !env.chanvoy_config_dir().join("active_profile").exists(),
        "`daemon start` must not write the active_profile marker"
    );
}

/// CHAN-TASK-001 AC — a child that dies during startup is reported as a
/// startup failure, not as a bare "not running".
///
/// Pre-convergence, every early child exit collapsed into
/// `Daemon(NotRunning(<socket>))`, which reads as "nothing was ever
/// started" and sends operators looking for stale runtime files — the
/// wrong diagnosis, and the one the field trace acted on. The forced
/// failure here is child-side by construction: the profile carries a
/// reduce policy naming a family profile that does not exist, which the
/// daemon refuses at startup while the parent's own identity validation
/// passes cleanly.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn daemon_start_classifies_child_startup_failure() {
    let env = TestEnv::new("chan-task-001-start-child-failure").await;
    env.write_named_profile(
        &env.profile_name,
        "agent-bravo-devlead",
        "org-lanytehq",
        &env.token_env_name,
        Some("family-profile-that-does-not-exist"),
    );
    env.mock_baseline("bot-id-cts4", "agent-bravo-devlead", "team-id-456")
        .await;

    let out = run_chanvoy(&env, &["daemon", "start"]).await;
    assert!(
        !out.status.success(),
        "`daemon start` must exit nonzero when its child dies during startup; stdout={}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    // Assertions are against the text the operator actually sees. `main` now
    // renders errors with `Display`, so these match the `#[error(...)]`
    // messages rather than enum shapes.
    assert!(
        stderr.contains("daemon startup failed"),
        "failure must be classified as a startup failure, not a bare not-listening \
         report; stderr={stderr}"
    );
    assert!(
        !stderr.contains("no chanvoy daemon is listening"),
        "a child that started and died must not be reported as 'nothing is running'; \
         stderr={stderr}"
    );
    assert!(
        stderr.contains("before consuming the bootstrap handoff"),
        "classification must name the startup stage the child died in; stderr={stderr}"
    );
    assert!(
        stderr.contains("daemon serve"),
        "classification must point at the foreground diagnostic path; stderr={stderr}"
    );
    assert!(
        !bootstrap_handoff_path(&env).exists(),
        "an unconsumed handoff must be cleaned up so it cannot shadow the next spawn"
    );
    assert!(
        read_daemon_pid(&env).is_none(),
        "a daemon that never bound must not leave a pid file"
    );
}

/// Count live `chanvoy ... daemon serve` processes owned by this harness env.
///
/// The direct proof for "a failed start left nothing running" and "a retry
/// produced exactly one daemon". Deliberately process-table-based rather than
/// pid-file-based: the failure mode being tested is a child that is alive
/// *without* having written a pid file.
///
/// Process-table match on profile slug alone is **not** safe under concurrent
/// multi-seat `make pr-final` on a shared host — seats use the same fixed test
/// slugs with isolated `CHANVOY_RUNTIME_DIR` trees, so a foreign daemon with the
/// same `--profile` would false-count as a survivor. Scope candidates to
/// processes that hold open our runtime dir (or socket) via `lsof`.
fn count_daemon_serve_processes(env: &TestEnv) -> usize {
    let out = std::process::Command::new("ps")
        .args(["-ax", "-o", "pid=,command="])
        .output()
        .expect("ps -ax");
    let runtime = env.chanvoy_runtime_dir();
    let socket = env.socket_path();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let mut parts = line.splitn(2, char::is_whitespace);
            let pid: u32 = parts.next()?.trim().parse().ok()?;
            let cmd = parts.next().unwrap_or("");
            if !(cmd.contains("chanvoy")
                && cmd.contains("daemon")
                && cmd.contains("serve")
                && cmd.contains(&env.profile_name))
            {
                return None;
            }
            // Belong to this harness invocation: open file under our runtime
            // or our socket path. Foreign seats with the same slug fail this.
            if process_holds_path(pid, &socket) || process_holds_path(pid, &runtime) {
                Some(pid)
            } else {
                None
            }
        })
        .count()
}

/// Whether process `pid` has `path` open (socket, cwd, or any FD under a dir).
fn process_holds_path(pid: u32, path: &std::path::Path) -> bool {
    let Some(path_str) = path.to_str() else {
        return false;
    };
    let out = std::process::Command::new("lsof")
        .args(["-a", "-p", &pid.to_string(), "--", path_str])
        .output();
    match out {
        Ok(o) if o.status.success() => !o.stdout.is_empty(),
        _ => false,
    }
}

/// Write a `[reduce]`-configured stream profile plus its family profile, with
/// the family pointed at a separate server so the family `whoami` the daemon
/// child makes can be controlled independently of the stream identity the
/// parent CLI validates.
fn write_reduce_pair(env: &TestEnv, family_name: &str, family_server_url: &str) {
    let stream = Profile {
        name: env.profile_name.clone(),
        role: "bravo-devlead".to_string(),
        scope: "lanytehq".to_string(),
        provider: chanvoy_core::Provider::Mattermost,
        bot_username: "agent-bravo-devlead".to_string(),
        team_name: "org-lanytehq".to_string(),
        server_url: env.server_url(),
        env_name: env.token_env_name.clone(),
        env_file: None,
        credential_mode: chanvoy_core::CredentialMode::EnvName,
        capability_class: chanvoy_core::CapabilityClass::Standard,
        monitored_channels: Vec::new(),
        ipc: None,
        reduce: Some(chanvoy_core::ReducePolicy {
            use_profile: family_name.to_string(),
        }),
    };
    let family = Profile {
        name: family_name.to_string(),
        server_url: family_server_url.to_string(),
        bot_username: "agent-bravo-family".to_string(),
        reduce: None,
        ..stream.clone()
    };
    let dir = env.chanvoy_config_dir().join("profiles");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    for profile in [&stream, &family] {
        std::fs::write(
            dir.join(format!("{}.toml", profile.name)),
            toml::to_string_pretty(profile).unwrap(),
        )
        .unwrap();
    }
}

/// devrev P1 — a `daemon start` reported as failed must leave nothing alive.
///
/// The readiness-timeout path used to return an error while its child was
/// still starting. That child may not have written a pid or socket yet, so the
/// next `daemon start` finds nothing to stop and spawns a second one —
/// two-daemons-one-profile, the condition the lifecycle code exists to
/// prevent. A failed start must be terminal.
///
/// The hang is deterministic, not timing-luck: the stream profile carries a
/// reduce policy whose family profile lives on a second mock server that
/// delays `whoami` well past the startup budget. The daemon child blocks in
/// `build_reduce_writer` — before bind, before consuming the handoff — while
/// the parent's own stream-identity validation against the primary mock
/// succeeds normally.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn daemon_start_timeout_leaves_no_live_child_and_retry_yields_one_daemon() {
    let env = TestEnv::new("chan-task-001-start-hung-child").await;
    let family_mock = MockServer::start().await;
    let family_name = "chan-task-001-hung-family";
    write_reduce_pair(&env, family_name, &family_mock.uri());
    // Parent-side validation (stream identity) resolves promptly.
    env.mock_baseline("bot-id-cts6", "agent-bravo-devlead", "team-id-456")
        .await;
    // Child-side family identity hangs past the readiness budget.
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "id": "family-id-cts6",
                    "username": "agent-bravo-family",
                    "is_bot": true,
                    "nickname": null,
                    "email": null,
                }))
                .set_delay(Duration::from_secs(30)),
        )
        .mount(&family_mock)
        .await;

    let _guard = env.daemon_guard();
    let out = run_chanvoy(&env, &["daemon", "start"]).await;
    assert!(
        !out.status.success(),
        "a daemon that never becomes ready must fail the start; stdout={}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("daemon startup failed") && stderr.contains("startup budget"),
        "timeout must be classified as a startup failure; stderr={stderr}"
    );

    // TERMINAL-FAILURE PROOF: nothing from that spawn survives.
    assert_eq!(
        count_daemon_serve_processes(&env),
        0,
        "a failed start must leave no live daemon child — an unowned child would \
         make the next start spawn a second daemon for the same profile"
    );
    assert!(
        read_daemon_pid(&env).is_none(),
        "failed start must leave no pid file"
    );
    assert!(
        !env.socket_path().exists(),
        "failed start must leave no socket"
    );
    assert!(
        !bootstrap_handoff_path(&env).exists(),
        "failed start must leave no orphaned bootstrap handoff"
    );

    // RETRY PROOF: with the family identity answering promptly, the same
    // command succeeds and produces exactly one daemon.
    family_mock.reset().await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "family-id-cts6",
            "username": "agent-bravo-family",
            "is_bot": true,
            "nickname": null,
            "email": null,
        })))
        .mount(&family_mock)
        .await;

    let retry = run_chanvoy(&env, &["daemon", "start"]).await;
    assert!(
        retry.status.success(),
        "retry after a terminal failed start must succeed; stderr={}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert_eq!(
        count_daemon_serve_processes(&env),
        1,
        "retry must produce exactly one daemon for the profile"
    );
    assert!(
        run_chanvoy(&env, &["daemon", "status"])
            .await
            .status
            .success(),
        "the single retried daemon must answer a fresh invocation"
    );

    teardown_auto_setup_daemon(&env).await;
}

/// devrev P1 (rev 2) — residue must be swept when the child exits *after*
/// binding its socket and writing its pid file.
///
/// This is the branch the rev-2 fix could not reach. `finalize_failed_spawn`
/// read `child.id()` after `try_wait()` had already observed the exit, and
/// tokio returns `None` from `id()` once a child has been polled to completion
/// — so the pid-match guard never fired and the pid/socket files survived while
/// the error text claimed they had been cleared.
///
/// Forcing point is deterministic and post-bind by construction: the daemon
/// binds its socket and writes its pid file, and only *then* loads attention
/// state, which hard-errors on malformed JSON. So the child is guaranteed to
/// have created residue before dying.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn daemon_start_sweeps_residue_when_child_exits_after_binding() {
    let env = TestEnv::new("chan-task-001-start-post-bind-exit").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-cts8", "agent-bravo-devlead", "team-id-456")
        .await;
    // Malformed attention state: read after bind + pid write, and a hard error.
    std::fs::write(env.state_path(), "{ this is not valid json").expect("plant bad state");

    let _guard = env.daemon_guard();
    let out = run_chanvoy(&env, &["daemon", "start"]).await;
    assert!(
        !out.status.success(),
        "a child that dies during startup must fail the start; stdout={}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("exited on its own"),
        "the child died by itself here — the diagnostic must not claim we killed it; \
         stderr={stderr}"
    );

    // The claim the diagnostic makes must be true.
    assert_eq!(
        count_daemon_serve_processes(&env),
        0,
        "no daemon may survive a failed start"
    );
    assert!(
        read_daemon_pid(&env).is_none(),
        "pid file written by the dead child must be swept — this is the branch where \
         `child.id()` returns None if the pid is not captured at spawn time"
    );
    assert!(
        !env.socket_path().exists(),
        "socket bound by the dead child must be swept"
    );
    assert!(
        !bootstrap_handoff_path(&env).exists(),
        "handoff must not survive a failed start"
    );

    // And the profile must be startable again once the cause is fixed, with no
    // manual cleanup in between.
    std::fs::remove_file(env.state_path()).expect("remove bad state");
    let retry = run_chanvoy(&env, &["daemon", "start"]).await;
    assert!(
        retry.status.success(),
        "start must succeed after the cause is removed, with no manual cleanup; stderr={}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert_eq!(
        count_daemon_serve_processes(&env),
        1,
        "exactly one daemon after recovery"
    );

    teardown_auto_setup_daemon(&env).await;
}

/// Process-table counts must not see a foreign seat's daemon.
///
/// Two harness envs share the same fixed profile slug (as concurrent multi-seat
/// `make pr-final` runs do) but own distinct runtime roots. A live daemon on
/// env A must not inflate env B's survivor count after B's failed start.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn process_count_is_scoped_to_invocation_runtime() {
    // Fixed slug on purpose: models multi-seat collision of identical test names.
    // Synthetic bot/team identities — not live estate accounts.
    let foreign = TestEnv::new_fixed("shared-collide-slug").await;
    foreign.write_default_profile("agent-reviewer-bot", "org-example");
    foreign
        .mock_baseline_for_team(
            "bot-id-collide-a",
            "agent-reviewer-bot",
            "team-id-456",
            "org-example",
        )
        .await;
    let local = TestEnv::new_fixed("shared-collide-slug").await;
    local.write_default_profile("agent-reviewer-bot", "org-example");
    local
        .mock_baseline_for_team(
            "bot-id-collide-b",
            "agent-reviewer-bot",
            "team-id-456",
            "org-example",
        )
        .await;

    // Foreign seat keeps a healthy daemon for the shared slug.
    let _foreign_guard = foreign.daemon_guard();
    let start_foreign = run_chanvoy(&foreign, &["daemon", "start"]).await;
    assert!(
        start_foreign.status.success(),
        "foreign start must succeed; stderr={}",
        String::from_utf8_lossy(&start_foreign.stderr)
    );
    assert_eq!(
        count_daemon_serve_processes(&foreign),
        1,
        "foreign env owns its live daemon"
    );

    // Local seat fails post-bind (malformed attention state) for the same slug.
    std::fs::write(local.state_path(), "{ this is not valid json").expect("plant bad state");
    let _local_guard = local.daemon_guard();
    let start_local = run_chanvoy(&local, &["daemon", "start"]).await;
    assert!(
        !start_local.status.success(),
        "local failed start must fail; stderr={}",
        String::from_utf8_lossy(&start_local.stderr)
    );

    // The multi-seat failure mode: unscoped process-table match would count the
    // foreign daemon as a local survivor. Scoped count must stay 0.
    assert_eq!(
        count_daemon_serve_processes(&local),
        0,
        "local count must ignore a foreign daemon that shares the profile slug"
    );
    assert_eq!(
        count_daemon_serve_processes(&foreign),
        1,
        "foreign daemon must still be live after local failed start + cleanup"
    );
    assert!(
        foreign.socket_path().exists(),
        "foreign socket must not be swept by local failed-start cleanup"
    );

    teardown_auto_setup_daemon(&foreign).await;
    // local never left a live daemon; teardown is a no-op if pid absent
    teardown_auto_setup_daemon(&local).await;
}

/// devrev P2 — the command-to-policy mapping for `daemon start`, proven at the
/// CLI level.
///
/// Core resolver unit tests cover `ExplicitOnly` semantics but not which
/// commands are mapped to it, which is the part that changed. Also pins the
/// scope of the widening: read verbs must still resolve by fallback.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn daemon_start_requires_explicit_profile_selection() {
    // Profile name must be `<role>-<scope>` so the env-exact rule can match.
    let env = TestEnv::new("chan-task-001-policy-lanytehq").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-cts7", "agent-bravo-devlead", "team-id-456")
        .await;
    // An `active_profile` marker is the fallback that must NOT satisfy
    // `daemon start`.
    let marker = env.chanvoy_config_dir().join("active_profile");
    std::fs::write(&marker, &env.profile_name).expect("write active marker");
    std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o600)).unwrap();

    // `chanvoy_command` clears the environment, so this invocation has no
    // explicit source at all: no --profile, no CHANVOY_PROFILE, no role/scope.
    let implicit = env
        .chanvoy_command()
        .arg("daemon")
        .arg("start")
        .output()
        .await
        .expect("bare daemon start");
    assert!(
        !implicit.status.success(),
        "bare `daemon start` must refuse to resolve via the active_profile marker"
    );
    let stderr = String::from_utf8_lossy(&implicit.stderr);
    // The previously-documented gap here is now closed: `main` renders
    // `Display`, so the operator reads the typed message instead of the variant
    // name `DestructiveRequiresExplicit` — which was mis-describing a daemon
    // start as "destructive".
    assert!(
        stderr.contains("requires explicit profile selection"),
        "refusal must come from the explicit-source requirement; stderr={stderr}"
    );
    assert!(
        !stderr.to_lowercase().contains("destructive"),
        "`daemon start` is not destructive — no rendering of this refusal may say so; \
         stderr={stderr}"
    );
    assert!(
        stderr.contains(&env.profile_name),
        "refusal must list the available profiles so the operator can name one; \
         stderr={stderr}"
    );
    assert!(
        !env.socket_path().exists() && read_daemon_pid(&env).is_none(),
        "a refused resolution must not spawn anything"
    );

    // Env-exact identity (`<role>-<scope>` naming the existing profile) is an
    // explicit source and must succeed.
    let _guard = env.daemon_guard();
    let env_exact = env
        .chanvoy_command()
        .env("LANYTE_AGENT_ROLE", "chan-task-001-policy")
        .env("LANYTE_AGENT_SCOPE", "lanytehq")
        .arg("daemon")
        .arg("start")
        .output()
        .await
        .expect("env-exact daemon start");
    assert!(
        env_exact.status.success(),
        "sourced agent identity must satisfy the explicit-source requirement; stderr={}",
        String::from_utf8_lossy(&env_exact.stderr)
    );

    // Single-running-daemon fallback must not satisfy it either — exactly one
    // daemon is now running, which is what rule 4 would have resolved.
    let implicit_again = env
        .chanvoy_command()
        .arg("daemon")
        .arg("start")
        .output()
        .await
        .expect("bare daemon start with one daemon running");
    assert!(
        !implicit_again.status.success(),
        "bare `daemon start` must refuse the single-running-daemon fallback too"
    );

    // Scope check: the widening is per-command. A read verb still resolves via
    // fallback against that same running daemon.
    let read_verb = env
        .chanvoy_command()
        .arg("whoami")
        .output()
        .await
        .expect("bare whoami");
    assert!(
        read_verb.status.success(),
        "read verbs must keep the broader fallback chain; stderr={}",
        String::from_utf8_lossy(&read_verb.stderr)
    );

    teardown_auto_setup_daemon(&env).await;
}

/// CHAN-TASK-001 AC — `daemon serve` stays attached.
///
/// Convergence is about the *background* path. `daemon serve` is the
/// foreground diagnostic surface: it must remain in the invoking
/// process's session so logs land on the operator's terminal and `Ctrl-C`
/// works. A regression that detached it would silently remove the only
/// way to watch a failing daemon start.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn daemon_serve_remains_attached_to_invoking_session() {
    let env = TestEnv::new("chan-task-001-serve-attached").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-cts5", "agent-bravo-devlead", "team-id-456")
        .await;

    let child = spawn_daemon(&env).await;
    let child_pid = child.id().expect("serve child pid");

    // SAFETY: getsid/getpid are ordinary syscalls; unsafe is the FFI
    // requirement only.
    let (child_sid, own_sid) = unsafe {
        (
            libc::getsid(child_pid as i32),
            libc::getsid(std::process::id() as i32),
        )
    };
    assert!(child_sid > 0, "getsid(serve child) failed: {child_sid}");
    assert_ne!(
        child_sid as u32, child_pid,
        "`daemon serve` must NOT become a session leader — it is the attached \
         foreground diagnostic form; got sid={child_sid} pid={child_pid}"
    );
    assert_eq!(
        child_sid, own_sid,
        "`daemon serve` must stay in the invoking process's session"
    );

    let clean = stop_daemon_cleanly(&env, child).await;
    assert!(clean, "foreground daemon should stop cleanly");
}

/// AC #3 — attention state is intact when reached from a "fresh
/// session" (modeled here as a fresh CLI invocation against the
/// detached daemon). Cursor-write performed via `chanvoy post` is
/// observable via `attention list` even though the original auto-setup
/// CLI has exited.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn auto_setup_detached_daemon_state_survives_session_transition() {
    let env = TestEnv::new("per-008d-state-survives").await;
    env.mock_baseline("bot-id-d2", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_empty_memberships("team-id-456").await;
    env.mock_channel_lookup("bravo-team", "chan-id-d2").await;
    env.mock_post_create("post-id-d2").await;

    // Session A: auto-setup spawns the daemon, then the spawning CLI
    // exits. The daemon detaches and survives.
    let auto_setup_out = env
        .chanvoy_command()
        .env("LANYTE_AGENT_ROLE", "bravo-devlead")
        .env("LANYTE_AGENT_SCOPE", "lanytehq")
        .env("LANYTE_MM_URL", env.server_url())
        .env("LANYTE_MM_TEAM", "org-lanytehq")
        .env("CHANVOY_PROFILE", &env.profile_name)
        .arg("--json")
        .arg("auto-setup")
        .output()
        .await
        .expect("auto-setup");
    assert!(
        auto_setup_out.status.success(),
        "auto-setup must succeed; stderr={}",
        String::from_utf8_lossy(&auto_setup_out.stderr)
    );
    let _guard = env.daemon_guard();

    // Session A continues: post a message via the detached daemon.
    let post_out = run_chanvoy(&env, &["post", "bravo-team", "session-A"]).await;
    assert!(
        post_out.status.success(),
        "post must succeed against detached daemon; stderr={}",
        String::from_utf8_lossy(&post_out.stderr)
    );

    // "Session B" (modeled as a separate CLI invocation): inspect
    // attention state. The daemon is the same one from Session A —
    // detachment means the state from Session A's post is observable
    // here.
    let list_out = run_chanvoy(&env, &["--json", "attention", "list"]).await;
    assert!(list_out.status.success());
    let parsed: serde_json::Value =
        serde_json::from_slice(&list_out.stdout).expect("json list parses");
    let bravo_team = parsed["channels"]
        .as_array()
        .and_then(|arr| {
            arr.iter().find(|c| {
                // PER-019: attention list emits qualified
                // `<team>/<channel>` keys for the channel field.
                let label = c["channel"].as_str().unwrap_or("");
                label == "org-lanytehq/bravo-team" || label == "bravo-team"
            })
        })
        .expect("bravo-team entry present after Session A's post");
    assert_eq!(
        bravo_team["source"].as_str(),
        Some("post_cursor"),
        "Session A's cursor must be observable in Session B's inspection"
    );
    assert_eq!(
        bravo_team["newest_seen"].as_str(),
        Some("post-id-d2"),
        "cursor value preserved across session transition"
    );

    teardown_auto_setup_daemon(&env).await;
}
