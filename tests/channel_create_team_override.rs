//! v0.2.1 release-prep: `chanvoy channel create --team <slug>`
//! integration test. Closes the cross-team admin-verb gap that the
//! PER-019 γ resolver left on `channel create` (every other v0.2.1
//! verb routes cross-team correctly via the resolver, but
//! `create_channel` historically went straight through the profile's
//! primary `team_id()`).
//!
//! Test asserts:
//! - `chanvoy channel create --team org-3leaps some-name "Display" "Purpose"`
//!   posts to `POST /api/v4/channels` with `team_id: <alt-team-id>`,
//!   NOT the primary team's id, and preserves the positional purpose
//! - The team override does NOT change behavior when omitted (legacy
//!   default still lands on the primary team)
//!
//! Shared harness primitives live in `tests/common/mod.rs`.

#![allow(dead_code)]

mod common;

use common::{run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use wiremock::http::Method;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

/// Mount `/users/me/teams` + alt-team `/teams/name/<slug>` lookup so
/// `team_id_for_slug` can resolve the alternate team.
async fn mount_alternate_team(
    env: &TestEnv,
    primary_team_id: &str,
    alt_team: &str,
    alt_team_id: &str,
) {
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": primary_team_id, "name": "org-lanytehq", "display_name": "lanytehq"},
            {"id": alt_team_id, "name": alt_team, "display_name": alt_team},
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

/// `chanvoy channel create --team <alt-team> <name> <display> <purpose>` posts
/// to MM `/channels` with `team_id` set to the alt team's id, not the
/// primary team's, and passes the positional purpose through. This is
/// the exact argument shape used by the live release-smoke harness.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn channel_create_with_team_override_uses_alt_team_id() {
    let env = TestEnv::new("chan-create-alt-team").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-cco", "agent-bravo-devlead", "team-id-456")
        .await;
    mount_alternate_team(&env, "team-id-456", "org-3leaps", "team-id-3leaps").await;

    // Mock POST /channels returning a successful create response.
    Mock::given(method("POST"))
        .and(path("/api/v4/channels"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "new-chan-id",
            "name": "ops-discussions",
            "display_name": "Ops Discussions",
            "type": "O",
        })))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "channel",
            "create",
            "--team",
            "org-3leaps",
            "ops-discussions",
            "Ops Discussions",
            "Release smoke purpose",
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "channel create --team must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Read back the POST /channels body and assert team_id is the
    // alt team's, not the primary's.
    let requests = env.mock.received_requests().await.unwrap_or_default();
    let create_post = requests
        .iter()
        .find(|r| r.method == Method::POST && r.url.path() == "/api/v4/channels")
        .expect("POST /channels was made");
    let body: serde_json::Value =
        serde_json::from_slice(&create_post.body).expect("create body parses");
    assert_eq!(
        body["team_id"].as_str(),
        Some("team-id-3leaps"),
        "channel create --team must POST with the alt-team's team_id; got body: {}",
        serde_json::to_string(&body).unwrap()
    );
    assert_ne!(
        body["team_id"].as_str(),
        Some("team-id-456"),
        "channel create --team must NOT use primary-team id"
    );
    assert_eq!(
        body["purpose"].as_str(),
        Some("Release smoke purpose"),
        "channel create must preserve its optional positional purpose"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// Devrev PR #23 finding #1: `--team <slug>` must refuse with
/// `NotAMemberOfTeam` semantics when the bot is not a member of the
/// requested team — matching the PER-019 γ resolver's enforcement
/// posture for the rest of the cross-team verbs. Critically, the
/// refusal must happen BEFORE `POST /channels` is hit (no
/// silently-letting-MM-reject path).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn channel_create_team_not_member_refuses_before_post() {
    let env = TestEnv::new("chan-create-not-member").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ccnm", "agent-bravo-devlead", "team-id-456")
        .await;
    // Bot is a member of org-lanytehq ONLY. The operator's --team
    // value (`org-not-a-member`) is not in this list. The membership
    // helper must refuse before POST /channels is attempted.
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": "team-id-456", "name": "org-lanytehq", "display_name": "lanytehq"},
        ])))
        .mount(&env.mock)
        .await;
    // POST /channels intentionally NOT mounted — if create_channel
    // attempted to call it despite the membership refusal, wiremock
    // would 404 by default and the assertion below would still
    // catch the wrongful call via received_requests.

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "channel",
            "create",
            "ops-discussions",
            "Ops Discussions",
            "--team",
            "org-not-a-member",
        ],
    )
    .await;
    assert!(
        !out.status.success(),
        "channel create --team <not-a-member> must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    // Diagnostic should name the unfamiliar team. The exact
    // available-teams list shape is `CoreError::NotAMemberOfTeam`'s
    // surface; chanvoy's CLI error renderer shows it.
    assert!(
        stderr.contains("org-not-a-member") || stderr.to_lowercase().contains("not a member"),
        "diagnostic should name the bot-isn't-a-member condition; got: {stderr}"
    );

    // Critical: verify `POST /channels` was NOT called. Membership
    // refusal must short-circuit before any write attempt — devrev
    // pin's load-bearing assertion.
    let requests = env.mock.received_requests().await.unwrap_or_default();
    let create_writes = requests
        .iter()
        .filter(|r| r.method == Method::POST && r.url.path() == "/api/v4/channels")
        .count();
    assert_eq!(
        create_writes, 0,
        "membership refusal must short-circuit before POST /channels; observed {create_writes} write attempts"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// Legacy default preserved: `chanvoy channel create <name> <display>`
/// (no `--team`) still lands on the profile's primary team. Confirms
/// the v0.2.1 addition is fully additive and doesn't accidentally
/// change behavior for callers who don't opt into the override.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn channel_create_without_team_lands_on_primary() {
    let env = TestEnv::new("chan-create-primary").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ccp", "agent-bravo-devlead", "team-id-456")
        .await;

    Mock::given(method("POST"))
        .and(path("/api/v4/channels"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "new-chan-id",
            "name": "primary-chan",
            "display_name": "Primary Chan",
            "type": "O",
        })))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["channel", "create", "primary-chan", "Primary Chan"]).await;
    assert!(out.status.success());

    let requests = env.mock.received_requests().await.unwrap_or_default();
    let create_post = requests
        .iter()
        .find(|r| r.method == Method::POST && r.url.path() == "/api/v4/channels")
        .expect("POST /channels was made");
    let body: serde_json::Value = serde_json::from_slice(&create_post.body).expect("body parses");
    assert_eq!(
        body["team_id"].as_str(),
        Some("team-id-456"),
        "channel create without --team must use primary-team id (legacy default preserved)"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}
