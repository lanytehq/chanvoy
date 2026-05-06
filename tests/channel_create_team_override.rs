//! v0.2.1 release-prep: `chanvoy channel create --team <slug>`
//! integration test. Closes the cross-team admin-verb gap that the
//! PER-019 γ resolver left on `channel create` (every other v0.2.1
//! verb routes cross-team correctly via the resolver, but
//! `create_channel` historically went straight through the profile's
//! primary `team_id()`).
//!
//! Test asserts:
//! - `chanvoy channel create some-name "Display" --team org-3leaps`
//!   posts to `POST /api/v4/channels` with `team_id: <alt-team-id>`,
//!   NOT the primary team's id
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

/// `chanvoy channel create <name> <display> --team <alt-team>` posts
/// to MM `/channels` with `team_id` set to the alt team's id, not the
/// primary team's. Verified by reading back the wiremock POST body.
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
            "ops-discussions",
            "Ops Discussions",
            "--team",
            "org-3leaps",
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
