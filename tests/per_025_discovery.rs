//! PER-025: discovery primitives integration tests.
//!
//! Covers the two primitives shipped in PER-025 against the amended
//! brief at productbook PR #49:
//!
//! 1. `chanvoy search <channel> <query>` — channel-scoped search via
//!    MM `POST /api/v4/teams/{id}/posts/search`. Inline operator
//!    conflict refusal per AC #4a (broadened: `in:`, `from:`,
//!    `before:`/`after:` against chanvoy-owned scopes).
//! 2. `chanvoy channels` enriched output — `last_active` column
//!    (relative time / `—` for missing-activity), `--sort active`
//!    preserves PER-019 grouping (no flatten), `--primary-team --json`
//!    legacy preservation per AC #6a, `last_post_at: null` required
//!    on default `--json` for missing-activity per AC #6 cleanup.
//!
//! Cross-channel / team-wide search is explicitly DEFERRED from v1
//! per cross-reviewer alignment — no `--team-wide` etc. is exercised.
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

/// `chanvoy search` requires both positional args (channel + query).
#[tokio::test]
async fn search_missing_args_rejected() {
    let env = TestEnv::new("per-025-search-missing").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(&env, &["search", "some-channel"]).await;
    assert!(!out.status.success(), "search missing query must reject");

    let out = run_chanvoy(&env, &["search"]).await;
    assert!(!out.status.success(), "bare `search` must reject");
}

/// `chanvoy search <channel> "query in:other-channel"`: inline `in:`
/// conflicts with the channel arg per AC #4a. CLI-level rejection —
/// never reaches the daemon.
#[tokio::test]
async fn search_inline_in_rejects_against_channel_arg() {
    let env = TestEnv::new("per-025-search-in-conflict").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(
        &env,
        &["search", "some-channel", "parent_pid in:other-channel"],
    )
    .await;
    assert!(!out.status.success(), "in: conflict must reject");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("`in:`"),
        "diagnostic should name the in: conflict; got: {stderr}"
    );
    assert!(
        !stderr.contains("--team-wide"),
        "AC #4a: diagnostic must NOT name nonexistent --team-wide flag; got: {stderr}"
    );
}

/// `chanvoy search <channel> "query from:user" --from other`: inline
/// `from:` conflicts with `--from` flag.
#[tokio::test]
async fn search_inline_from_rejects_against_from_flag() {
    let env = TestEnv::new("per-025-search-from-conflict").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(
        &env,
        &[
            "search",
            "some-channel",
            "x from:entarch",
            "--from",
            "devrev",
        ],
    )
    .await;
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("`from:`") && stderr.contains("--from"),
        "diagnostic should name both sides of the conflict; got: {stderr}"
    );
}

/// `chanvoy search <channel> "query before:date" --since 5m`: inline
/// `before:` conflicts with `--since`.
#[tokio::test]
async fn search_inline_before_rejects_against_since_flag() {
    let env = TestEnv::new("per-025-search-before-conflict").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(
        &env,
        &[
            "search",
            "some-channel",
            "x before:2026-05-01",
            "--since",
            "5m",
        ],
    )
    .await;
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("`before:`") && stderr.contains("--since"),
        "got: {stderr}"
    );
}

// ----------------------------------------------------------------------
// Daemon-spawn integration tests
// ----------------------------------------------------------------------

/// Mock the MM search endpoint with a parametric response.
async fn mock_search_response(
    env: &TestEnv,
    team_id: &str,
    order: &[&str],
    posts: &[(&str, &str, &str, &str, i64)],
) {
    let posts_map: serde_json::Map<String, serde_json::Value> = posts
        .iter()
        .map(|(id, user_id, username, message, create_at)| {
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
        })
        .collect();
    Mock::given(method("POST"))
        .and(path(format!("/api/v4/teams/{team_id}/posts/search")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "order": order,
            "posts": posts_map,
        })))
        .mount(&env.mock)
        .await;
}

/// `chanvoy search <channel> <query>` happy path: returns matching
/// posts in MM's ranked order. Result `team` and `channel` are
/// surfaced for cross-team disambiguation per AC #6.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn search_happy_path() {
    let env = TestEnv::new("per-025-search-happy").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-sh", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-sh").await;
    mock_search_response(
        &env,
        "team-id-456",
        &["match-1", "match-2"],
        &[
            (
                "match-1",
                "user-1",
                "alice",
                "parent_pid validation here",
                1_700_000_000_000,
            ),
            (
                "match-2",
                "user-2",
                "bob",
                "parent_pid concern from earlier",
                1_700_000_001_000,
            ),
        ],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "search", "bravo-team", "parent_pid"]).await;
    assert!(
        out.status.success(),
        "search must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("search --json parses");
    assert_eq!(result["team"].as_str(), Some("org-lanytehq"));
    assert_eq!(result["channel"].as_str(), Some("bravo-team"));
    let posts = result["posts"].as_array().expect("posts is an array");
    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0]["id"].as_str(), Some("match-1"));
    assert_eq!(posts[1]["id"].as_str(), Some("match-2"));
    // AC #6: per-result `create_at` is i64 Unix epoch ms (matches
    // chanvoy-core post-timestamp convention).
    assert_eq!(posts[0]["create_at"].as_i64(), Some(1_700_000_000_000));

    // Verify chanvoy composed the chanvoy-owned scope into MM's
    // `terms` field — the request body should include
    // `in:bravo-team`.
    let requests = env.mock.received_requests().await.unwrap_or_default();
    let search_post = requests
        .iter()
        .find(|r| {
            r.method == Method::POST && r.url.path() == "/api/v4/teams/team-id-456/posts/search"
        })
        .expect("search request was made");
    let body: serde_json::Value =
        serde_json::from_slice(&search_post.body).expect("search body parses");
    let terms = body["terms"].as_str().expect("terms field");
    assert!(
        terms.contains("parent_pid"),
        "terms includes user query; got: {terms}"
    );
    assert!(
        terms.contains("in:bravo-team"),
        "terms includes auto-scope; got: {terms}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy search` returns empty result (exit 0; not an error).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn search_empty_result() {
    let env = TestEnv::new("per-025-search-empty").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-se", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-se").await;
    mock_search_response(&env, "team-id-456", &[], &[]).await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "search", "bravo-team", "nothing"]).await;
    assert!(out.status.success(), "empty result is not an error");
    let result: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("search --json parses");
    assert_eq!(
        result["posts"].as_array().map(|a| a.len()),
        Some(0),
        "empty result returns posts: []"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy search --limit N` caps the result count even when MM
/// returns more.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn search_limit_caps_results() {
    let env = TestEnv::new("per-025-search-limit").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-sl", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-sl").await;
    mock_search_response(
        &env,
        "team-id-456",
        &["m1", "m2", "m3", "m4", "m5"],
        &[
            ("m1", "u", "a", "x", 1_700_000_000_000),
            ("m2", "u", "a", "x", 1_700_000_000_001),
            ("m3", "u", "a", "x", 1_700_000_000_002),
            ("m4", "u", "a", "x", 1_700_000_000_003),
            ("m5", "u", "a", "x", 1_700_000_000_004),
        ],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &["--json", "search", "bravo-team", "x", "--limit", "2"],
    )
    .await;
    assert!(out.status.success());
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).expect("parses");
    assert_eq!(
        result["posts"].as_array().map(|a| a.len()),
        Some(2),
        "--limit 2 caps to 2 matches"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy search --from <author>` folds into the MM `terms` field as
/// `from:<author>` operator. AC #4a non-conflicting case.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn search_from_flag_folds_into_terms() {
    let env = TestEnv::new("per-025-search-from-flag").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-sf", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-sf").await;
    mock_search_response(&env, "team-id-456", &[], &[]).await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &["search", "bravo-team", "validation", "--from", "entarch"],
    )
    .await;
    assert!(out.status.success());

    let requests = env.mock.received_requests().await.unwrap_or_default();
    let search_post = requests
        .iter()
        .find(|r| r.method == Method::POST && r.url.path().ends_with("/posts/search"))
        .expect("search request");
    let body: serde_json::Value = serde_json::from_slice(&search_post.body).unwrap();
    let terms = body["terms"].as_str().unwrap();
    assert!(
        terms.contains("from:entarch"),
        "--from <author> folds into terms; got: {terms}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy search` non-conflicting inline operator passes through.
/// `before:` is fine when `--since` is unset — chanvoy doesn't claim
/// ownership of the time scope, so MM handles.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn search_non_conflicting_before_passes_through() {
    let env = TestEnv::new("per-025-search-passthrough").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-sp", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-sp").await;
    mock_search_response(&env, "team-id-456", &[], &[]).await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["search", "bravo-team", "x before:2026-05-01"]).await;
    assert!(
        out.status.success(),
        "non-conflicting before: must pass through; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let requests = env.mock.received_requests().await.unwrap_or_default();
    let search_post = requests
        .iter()
        .find(|r| r.method == Method::POST && r.url.path().ends_with("/posts/search"))
        .expect("search request");
    let body: serde_json::Value = serde_json::from_slice(&search_post.body).unwrap();
    let terms = body["terms"].as_str().unwrap();
    assert!(
        terms.contains("before:2026-05-01"),
        "non-conflicting before: passes through verbatim; got: {terms}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy channels --json` (default, no --primary-team) includes
/// `last_post_at` on each channel; missing-activity is required to
/// be `last_post_at: null` per AC #6 cleanup.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn channels_json_missing_activity_is_null() {
    let env = TestEnv::new("per-025-channels-null").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-cn", "agent-bravo-devlead", "team-id-456")
        .await;
    // Mock /users/me/teams returning the primary team only.
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": "team-id-456", "name": "org-lanytehq", "display_name": "lanytehq"}
        ])))
        .mount(&env.mock)
        .await;
    // Channel listing: one with activity, one without.
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams/team-id-456/channels"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": "chan-active",
                "name": "active-channel",
                "display_name": "Active",
                "type": "O",
                "last_post_at": 1_700_000_000_000i64,
            },
            {
                "id": "chan-quiet",
                "name": "quiet-channel",
                "display_name": "Quiet",
                "type": "O",
                "last_post_at": 0,
            }
        ])))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "channels"]).await;
    assert!(
        out.status.success(),
        "channels --json must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("channels --json parses");
    let teams = parsed["teams"].as_array().expect("teams array");
    let channels = teams[0]["channels"].as_array().expect("channels array");
    let active = channels
        .iter()
        .find(|c| c["name"] == "active-channel")
        .unwrap();
    let quiet = channels
        .iter()
        .find(|c| c["name"] == "quiet-channel")
        .unwrap();

    assert_eq!(
        active["last_post_at"].as_i64(),
        Some(1_700_000_000_000),
        "active channel surfaces last_post_at as i64 epoch ms"
    );
    assert!(
        quiet["last_post_at"].is_null(),
        "AC #6 cleanup: missing-activity REQUIRED to be `null`, NOT absent or 0; got: {}",
        serde_json::to_string(quiet).unwrap()
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy channels --primary-team --json` preserves the legacy JSON
/// field set exactly — no `last_post_at` field. Per AC #6a.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn channels_primary_team_json_legacy_preserved() {
    let env = TestEnv::new("per-025-channels-legacy").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-cl", "agent-bravo-devlead", "team-id-456")
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams/team-id-456/channels"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": "chan-1",
                "name": "primary-channel",
                "display_name": "Primary",
                "type": "O",
                "last_post_at": 1_700_000_000_000i64,
            }
        ])))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "channels", "--primary-team"]).await;
    assert!(
        out.status.success(),
        "channels --primary-team --json must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).expect("legacy --json parses as Vec<LegacyChannel>");
    assert_eq!(parsed.len(), 1);
    let ch = &parsed[0];
    // Legacy fields present: id, name, display_name, type.
    assert_eq!(ch["name"].as_str(), Some("primary-channel"));
    assert_eq!(ch["type"].as_str(), Some("O"));
    // AC #6a: last_post_at MUST NOT be in the legacy JSON shape.
    assert!(
        !ch.as_object().unwrap().contains_key("last_post_at"),
        "legacy --primary-team --json must NOT include last_post_at; got: {}",
        serde_json::to_string(ch).unwrap()
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy channels --sort active` preserves PER-019 grouping —
/// channels sort within each team group by recency, but the team
/// group order itself is NOT flattened. Per AC #5 (entarch pin #4).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn channels_sort_active_preserves_grouping() {
    let env = TestEnv::new("per-025-channels-sort").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-cs", "agent-bravo-devlead", "team-id-456")
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": "team-id-456", "name": "org-lanytehq", "display_name": "lanytehq"},
            {"id": "team-id-3l", "name": "org-3leaps", "display_name": "3leaps"}
        ])))
        .mount(&env.mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams/team-id-456/channels"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": "c1", "name": "old", "display_name": "Old", "type": "O", "last_post_at": 100i64},
            {"id": "c2", "name": "newer", "display_name": "Newer", "type": "O", "last_post_at": 500i64},
            {"id": "c3", "name": "never", "display_name": "Never", "type": "O", "last_post_at": 0}
        ])))
        .mount(&env.mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams/team-id-3l/channels"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": "c4", "name": "alt-only", "display_name": "Alt", "type": "O", "last_post_at": 300i64}
        ])))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "channels", "--sort", "active"]).await;
    assert!(out.status.success());
    let parsed: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("channels --json parses");
    let teams = parsed["teams"].as_array().expect("teams array");

    // Group order preserved: primary team first, then alt team. AC
    // #5: --sort active does NOT flatten globally.
    assert_eq!(teams[0]["team_name"].as_str(), Some("org-lanytehq"));
    assert_eq!(teams[1]["team_name"].as_str(), Some("org-3leaps"));

    // Within primary team: most-recent first; never-active last.
    let primary_channels = teams[0]["channels"].as_array().unwrap();
    let names: Vec<&str> = primary_channels
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["newer", "old", "never"],
        "within-group recency sort with never-active last"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}
