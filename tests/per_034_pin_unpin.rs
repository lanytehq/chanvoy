//! PER-034: pin / unpin write-verb integration tests.
//!
//! Covers the verb pair shipped in PER-034, end-to-end through the
//! real daemon binary against a wiremock Mattermost server:
//!
//! 1. `chanvoy pin <channel> <post_id> [--team <team>]`
//! 2. `chanvoy unpin <channel> <post_id> [--team <team>]`
//!
//! Pattern mirrors PER-024 react/unreact's integration tests exactly
//! (same mock baseline, same channel resolver shape, same daemon
//! lifecycle). PER-034 differences:
//!
//! - Two separate result types (`PinResult` / `UnpinResult`) because
//!   the brief's idempotency field names (`was_already_pinned` vs
//!   `was_already_unpinned`) are verb-specific
//! - Pre-write `is_pinned` lookup via `mock_post_lookup_pinned` so
//!   the `was_already_*` field carries meaningful state without an
//!   extra round-trip
//! - Validation order tested by leaving the POST /pin or /unpin mock
//!   unmounted on the wrong-channel path (any write would 404 and
//!   surface as failure)
//!
//! Idempotency contract per AC #3: re-pinning a pinned post or
//! re-unpinning an unpinned post exits 0 with the appropriate
//! `was_already_*` flag set. MM v4 returns 200 on either path; the
//! state-tracking comes from the pre-read.
//!
//! Shared harness primitives live in `tests/common/mod.rs`.

#![allow(dead_code)]

mod common;

use common::{run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use wiremock::http::Method;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

// ----------------------------------------------------------------------
// CLI-level rejection tests (no daemon needed)
// ----------------------------------------------------------------------

/// `chanvoy pin` requires two positional args (channel, post_id).
/// Missing either rejects at the clap layer.
#[tokio::test]
async fn pin_missing_args_rejected() {
    let env = TestEnv::new("per-034-pin-missing").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(&env, &["pin", "some-channel"]).await;
    assert!(
        !out.status.success(),
        "pin with one arg must reject (missing post_id)"
    );

    let out = run_chanvoy(&env, &["pin"]).await;
    assert!(!out.status.success(), "bare `pin` must reject");
}

/// `chanvoy unpin` mirrors `pin`'s positional arg requirements.
#[tokio::test]
async fn unpin_missing_args_rejected() {
    let env = TestEnv::new("per-034-unpin-missing").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(&env, &["unpin", "some-channel"]).await;
    assert!(
        !out.status.success(),
        "unpin with one arg must reject (missing post_id)"
    );

    let out = run_chanvoy(&env, &["unpin"]).await;
    assert!(!out.status.success(), "bare `unpin` must reject");
}

// ----------------------------------------------------------------------
// Daemon-driven happy + idempotency + validation paths
// ----------------------------------------------------------------------

/// `chanvoy pin` happy path on a not-yet-pinned post:
/// - resolver baseline + channel lookup + post lookup (`is_pinned: false`)
/// - POST /posts/{id}/pin returns 200
/// - --json output has `result: "pinned"`, `was_already_pinned: false`,
///   `ok: true`, and the resolved team/channel
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn pin_happy_path() {
    let env = TestEnv::new("per-034-pin-happy").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ph", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-ph").await;
    env.mock_post_lookup_pinned("post-ph", "chan-id-ph", false)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts/post-ph/pin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "OK"})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "pin", "bravo-team", "post-ph"]).await;
    assert!(
        out.status.success(),
        "pin must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).expect("pin --json parses");
    // Brief AC #5 field set: verb + channel + channel_id + post_id +
    // result + was_already_pinned (plus team + ok mirroring
    // ReactionResult per PER-024 pre-impl pin #2).
    assert_eq!(result["verb"].as_str(), Some("pin"));
    assert_eq!(result["ok"].as_bool(), Some(true));
    assert_eq!(result["team"].as_str(), Some("org-lanytehq"));
    assert_eq!(result["channel"].as_str(), Some("bravo-team"));
    assert_eq!(result["channel_id"].as_str(), Some("chan-id-ph"));
    assert_eq!(result["post_id"].as_str(), Some("post-ph"));
    assert_eq!(result["result"].as_str(), Some("pinned"));
    assert_eq!(result["was_already_pinned"].as_bool(), Some(false));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy pin` on an already-pinned post: exits 0 with
/// `was_already_pinned: true`. MM v4 returns 200 either way; the
/// state-tracking comes from the pre-read of `is_pinned`.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn pin_idempotent_on_already_pinned() {
    let env = TestEnv::new("per-034-pin-idem").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-pi", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-pi").await;
    env.mock_post_lookup_pinned("post-pi", "chan-id-pi", true)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts/post-pi/pin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "OK"})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "pin", "bravo-team", "post-pi"]).await;
    assert!(
        out.status.success(),
        "idempotent re-pin must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).expect("pin --json parses");
    assert_eq!(result["was_already_pinned"].as_bool(), Some(true));
    assert_eq!(result["ok"].as_bool(), Some(true));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy unpin` happy path on a currently-pinned post:
/// `was_already_unpinned: false`, `result: "unpinned"`.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn unpin_happy_path() {
    let env = TestEnv::new("per-034-unpin-happy").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-uh", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-uh").await;
    env.mock_post_lookup_pinned("post-uh", "chan-id-uh", true)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts/post-uh/unpin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "OK"})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "unpin", "bravo-team", "post-uh"]).await;
    assert!(
        out.status.success(),
        "unpin must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("unpin --json parses");
    assert_eq!(result["verb"].as_str(), Some("unpin"));
    assert_eq!(result["ok"].as_bool(), Some(true));
    assert_eq!(result["channel_id"].as_str(), Some("chan-id-uh"));
    assert_eq!(result["result"].as_str(), Some("unpinned"));
    assert_eq!(result["was_already_unpinned"].as_bool(), Some(false));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy unpin` on an already-unpinned post: exits 0 with
/// `was_already_unpinned: true`.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn unpin_idempotent_on_already_unpinned() {
    let env = TestEnv::new("per-034-unpin-idem").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ui", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-ui").await;
    env.mock_post_lookup_pinned("post-ui", "chan-id-ui", false)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts/post-ui/unpin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "OK"})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "unpin", "bravo-team", "post-ui"]).await;
    assert!(out.status.success(), "idempotent re-unpin must exit 0");
    let result: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("unpin --json parses");
    assert_eq!(result["was_already_unpinned"].as_bool(), Some(true));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy pin` on a post that doesn't exist on the resolved channel:
/// refuse before writing. Validation order matches PER-024 AC #5a;
/// no POST /posts/{id}/pin call.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn pin_wrong_channel_no_write() {
    let env = TestEnv::new("per-034-pin-wrong").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-pw", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-pw").await;
    env.mock_post_lookup_pinned("post-pw", "OTHER-CHANNEL", false)
        .await;
    // POST /posts/post-pw/pin intentionally NOT mounted.

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["pin", "bravo-team", "post-pw"]).await;
    assert!(
        !out.status.success(),
        "pin with wrong-channel post must exit non-zero"
    );

    let requests = env.mock.received_requests().await.unwrap_or_default();
    let pin_writes = requests
        .iter()
        .filter(|r| r.method == Method::POST && r.url.path() == "/api/v4/posts/post-pw/pin")
        .count();
    assert_eq!(
        pin_writes, 0,
        "wrong-channel pin must NOT issue POST /posts/.../pin; observed {pin_writes}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy pin` on a post that doesn't exist (MM 404): clear
/// diagnostic naming the post + channel; no write attempted.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn pin_post_not_found() {
    let env = TestEnv::new("per-034-pin-404").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-pn", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-pn").await;
    env.mock_post_lookup("missing-post", "chan-id-pn", false)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["pin", "bravo-team", "missing-post"]).await;
    assert!(
        !out.status.success(),
        "pin with missing post must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("missing-post") || stderr.contains("anchor"),
        "stderr should name the post id; got: {stderr}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy pin` on a post the bot lacks channel-admin to pin
/// (MM 403): chanvoy normalizes the 403 into a verb-specific
/// diagnostic naming the missing channel-admin permission per
/// AC #8 (devrev PR #36 F1). Per brief Open Question lean,
/// chanvoy doesn't pre-check — the normalization happens on the
/// MM error path so operators with no MM-API context see what to
/// ask their workspace admin for.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn pin_permission_error_diagnostic() {
    let env = TestEnv::new("per-034-pin-403").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-perr", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-perr").await;
    env.mock_post_lookup_pinned("post-perr", "chan-id-perr", false)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts/post-perr/pin"))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "status_code": 403,
            "message": "You do not have the appropriate permissions.",
        })))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["pin", "bravo-team", "post-perr"]).await;
    assert!(
        !out.status.success(),
        "pin without channel-admin must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    // Brief AC #8: the diagnostic must NAME the missing permission,
    // not just surface "api error 403: ...". Falsifiable assertions:
    assert!(
        stderr.contains("channel-admin"),
        "stderr must name the channel-admin permission; got: {stderr}"
    );
    assert!(
        stderr.contains("agent-bravo-devlead"),
        "stderr must name the bot username; got: {stderr}"
    );
    assert!(
        stderr.contains("bravo-team"),
        "stderr must name the channel; got: {stderr}"
    );
    assert!(
        stderr.contains("pin posts"),
        "stderr must distinguish pin vs unpin (verb-specific diagnostic); got: {stderr}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// Mount the additional mocks needed for cross-team `--team` /
/// `<team>/<channel>` resolution. Mirrors PER-024 / PER-023 pattern
/// (the alternate team surfaces in the bot's membership list so the γ
/// hybrid resolver can route explicit alternate-team requests).
async fn mount_alternate_team(
    env: &TestEnv,
    primary_team_id: &str,
    alt_team: &str,
    alt_team_id: &str,
) {
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": primary_team_id,
                "name": "org-lanytehq",
                "display_name": "lanytehq",
            },
            {
                "id": alt_team_id,
                "name": alt_team,
                "display_name": alt_team,
            },
        ])))
        .mount(&env.mock)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/teams/name/{alt_team}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": alt_team_id,
            "name": alt_team,
        })))
        .mount(&env.mock)
        .await;
}

/// PER-034 AC #9 — `--team` override flows through identically on
/// pin. Uses an explicit alternate team (not the profile's default)
/// to verify cross-team resolution paths the same way other γ hybrid
/// verbs do. Adds the new RPC path coverage devrev PR #36 F2
/// flagged was missing.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn pin_team_override() {
    let env = TestEnv::new("per-034-pin-team").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-pt", "agent-bravo-devlead", "team-id-456")
        .await;
    mount_alternate_team(&env, "team-id-456", "org-other", "team-id-other").await;
    env.mock_channel_lookup_for_team("team-id-other", "ops-updates", "chan-id-pt")
        .await;
    env.mock_post_lookup_pinned("post-pt", "chan-id-pt", false)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts/post-pt/pin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "OK"})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "--json",
            "pin",
            "ops-updates",
            "post-pt",
            "--team",
            "org-other",
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "pin with --team must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).expect("pin --json parses");
    assert_eq!(result["team"].as_str(), Some("org-other"));
    assert_eq!(result["channel"].as_str(), Some("ops-updates"));
    assert_eq!(result["channel_id"].as_str(), Some("chan-id-pt"));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// PER-034 AC #9 — `--team` override behaves identically on unpin.
/// Symmetric to `pin_team_override`.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn unpin_team_override() {
    let env = TestEnv::new("per-034-unpin-team").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ut", "agent-bravo-devlead", "team-id-456")
        .await;
    mount_alternate_team(&env, "team-id-456", "org-other", "team-id-other").await;
    env.mock_channel_lookup_for_team("team-id-other", "ops-updates", "chan-id-ut")
        .await;
    env.mock_post_lookup_pinned("post-ut", "chan-id-ut", true)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/posts/post-ut/unpin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "OK"})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "--json",
            "unpin",
            "ops-updates",
            "post-ut",
            "--team",
            "org-other",
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "unpin with --team must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("unpin --json parses");
    assert_eq!(result["verb"].as_str(), Some("unpin"));
    assert_eq!(result["team"].as_str(), Some("org-other"));
    assert_eq!(result["channel"].as_str(), Some("ops-updates"));
    assert_eq!(result["channel_id"].as_str(), Some("chan-id-ut"));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}
