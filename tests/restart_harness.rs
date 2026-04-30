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

use std::time::Duration;

use chanvoy_core::Profile;
use common::{
    kill_daemon, read_attention_state, run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv,
};
use tokio::process::Command;

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
