//! PER-035: profile identity-reduction integration tests.
//!
//! Covers the reduction policy end-to-end through the real daemon
//! binary against a wiremock Mattermost server. A stream-suffixed
//! profile (`dataeng-galaxy-s2`, team `org-codename`) carries a
//! `[reduce]` policy pointing at its bare family profile
//! (`dataeng-galaxy`, team `org-3leaps`). Two distinct bot tokens are
//! injected child-only so the daemon can build both clients; the
//! falsifiable assertion is **which bearer token the terminal write
//! carried**:
//!
//! - Post to a channel OUTSIDE the engagement team  → `POST /posts`
//!   carries the FAMILY token (identity reduced).
//! - Post to a channel INSIDE the engagement team   → `POST /posts`
//!   carries the STREAM token (no reduction).
//! - Channel resolution (the `GET /teams/...` lookups) always carries
//!   the STREAM token — resolution stays with the calling identity even
//!   when the write reduces (brief AC).
//! - `chanvoy whoami` returns the STREAM identity (not channel-targeted,
//!   never reduces).
//! - A `reduce.use_profile` that does not exist on disk refuses daemon
//!   start with a diagnostic naming the missing profile (brief AC:
//!   negative case — no silent fall-back to the bare daemon identity).
//!
//! Pattern mirrors PER-034's daemon-driven tests (same harness, same
//! mock baseline, same daemon lifecycle).
//!
//! Shared harness primitives live in `tests/common/mod.rs`.

#![allow(dead_code)]

mod common;

use std::time::Duration;

use common::{run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, ResponseTemplate};

const STREAM_PROFILE: &str = "dataeng-galaxy-s2";
const STREAM_BOT: &str = "agent-dataeng-blue-s2";
const STREAM_TEAM: &str = "org-codename";
const STREAM_TOKEN: &str = "test-token-value"; // TestEnv default (LANYTE_MM_TOKEN)

const FAMILY_PROFILE: &str = "dataeng-galaxy";
const FAMILY_BOT: &str = "agent-dataeng-blue";
const FAMILY_TEAM: &str = "org-3leaps";
const FAMILY_TOKEN_ENV: &str = "FAMILY_MM_TOKEN";
const FAMILY_TOKEN: &str = "family-token-value";

/// Build a TestEnv whose profile is the stream profile, with a second
/// (family) token injected and both profiles on disk. `reduce_target`
/// is the family profile name the stream reduces to (or a bogus name
/// for the negative test).
async fn stream_env(slug: &str, reduce_target: &str) -> TestEnv {
    let mut env = TestEnv::new(STREAM_PROFILE).await;
    // Use a unique config dir tag is unnecessary — TestEnv already
    // isolates per instance; `slug` only disambiguates intent in logs.
    let _ = slug;
    env.set_extra_env(FAMILY_TOKEN_ENV, FAMILY_TOKEN);
    // Stream profile: engagement team + reduction policy.
    env.write_named_profile(
        STREAM_PROFILE,
        STREAM_BOT,
        STREAM_TEAM,
        "LANYTE_MM_TOKEN",
        Some(reduce_target),
    );
    // Family profile: galaxy team, separate token env, no reduction.
    env.write_named_profile(
        FAMILY_PROFILE,
        FAMILY_BOT,
        FAMILY_TEAM,
        FAMILY_TOKEN_ENV,
        None,
    );
    env
}

/// Token-keyed `/users/me` mocks. Two distinct identities resolve by
/// bearer token so startup validation can tell the stream and family
/// clients apart:
/// - the STREAM token authenticates as the stream bot (primary
///   `daemon serve` whoami),
/// - the FAMILY token authenticates as the family bot (PER-035 reduce-
///   writer validation, devrev PR #37 P1).
///
/// This is what makes the leak detectable: if both profiles resolved
/// the same token, the family client's whoami would return the wrong
/// bot and startup would refuse.
async fn mock_identities(env: &TestEnv) {
    mock_whoami_for_token(env, STREAM_TOKEN, "stream-bot-id", STREAM_BOT).await;
    mock_whoami_for_token(env, FAMILY_TOKEN, "family-bot-id", FAMILY_BOT).await;
}

async fn mock_whoami_for_token(env: &TestEnv, token: &str, bot_id: &str, username: &str) {
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .and(header("authorization", format!("Bearer {token}").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": bot_id,
            "username": username,
            "is_bot": true,
            "nickname": null,
            "email": null,
        })))
        .mount(&env.mock)
        .await;
}

/// Bot team-membership list covering both the engagement team and the
/// family/galaxy team (so the γ resolver can route an explicit
/// `--team org-3leaps`). Mirrors PER-034 `mount_alternate_team`.
async fn mock_team_membership(env: &TestEnv) {
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": "team-codename-id", "name": STREAM_TEAM, "display_name": STREAM_TEAM},
            {"id": "team-3leaps-id", "name": FAMILY_TEAM, "display_name": FAMILY_TEAM},
        ])))
        .mount(&env.mock)
        .await;
}

async fn mock_team_by_name(env: &TestEnv, team_name: &str, team_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/teams/name/{team_name}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"id": team_id, "name": team_name})),
        )
        .mount(&env.mock)
        .await;
}

async fn mock_channel_for_team(env: &TestEnv, team_id: &str, channel: &str, channel_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/api/v4/teams/{team_id}/channels/name/{channel}"
        )))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"id": channel_id, "name": channel})),
        )
        .mount(&env.mock)
        .await;
}

async fn mock_post_create(env: &TestEnv, post_id: &str) {
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({"id": post_id})))
        .mount(&env.mock)
        .await;
}

/// Return the bearer token carried by the (first) request matching
/// `req_method` + exact path. Panics if no such request was recorded.
async fn bearer_for(env: &TestEnv, req_method: &str, req_path: &str) -> String {
    let requests = env.mock.received_requests().await.unwrap_or_default();
    let req = requests
        .iter()
        .find(|r| r.method.as_str().eq_ignore_ascii_case(req_method) && r.url.path() == req_path)
        .unwrap_or_else(|| panic!("no {req_method} {req_path} request recorded"));
    let header = req
        .headers
        .get("authorization")
        .unwrap_or_else(|| panic!("{req_method} {req_path} carried no Authorization header"));
    header
        .to_str()
        .unwrap()
        .strip_prefix("Bearer ")
        .unwrap_or(header.to_str().unwrap())
        .to_string()
}

// ----------------------------------------------------------------------
// Reduction: outside-team write posts under the family identity
// ----------------------------------------------------------------------

/// Brief AC: `chanvoy post <channel-outside-engagement>` from a
/// stream-profiled shell auto-reduces to the family identity. The
/// `POST /posts` write carries the FAMILY token; the channel-resolution
/// lookups carry the STREAM token (resolution stays with the caller).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_reduces_to_family_identity_outside_team() {
    let env = stream_env("reduce-outside", FAMILY_PROFILE).await;
    mock_identities(&env).await;
    // Explicit `--team` resolution routes through the bot's membership
    // list (`GET /users/me/teams`), not `GET /teams/name/...`.
    mock_team_membership(&env).await;
    mock_channel_for_team(&env, "team-3leaps-id", "tooling", "chan-tooling-id").await;
    mock_post_create(&env, "post-reduced").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "--json",
            "post",
            "tooling",
            "hello galaxy from a stream agent",
            "--team",
            FAMILY_TEAM,
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "post must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The terminal write reduced to the family identity.
    assert_eq!(
        bearer_for(&env, "POST", "/api/v4/posts").await,
        FAMILY_TOKEN,
        "outside-team post must carry the FAMILY bearer token (identity reduced)"
    );
    // Resolution stayed on the calling (stream) identity: the team
    // membership lookup that backs `--team` resolution carried the
    // stream token, not the family token.
    assert_eq!(
        bearer_for(&env, "GET", "/api/v4/users/me/teams").await,
        STREAM_TOKEN,
        "channel resolution must carry the STREAM bearer token (resolution does not reduce)"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// No reduction: in-engagement-team write keeps the stream identity
// ----------------------------------------------------------------------

/// Brief AC: `chanvoy post <channel-in-engagement>` from a
/// stream-profiled shell posts as the stream identity. The reduction
/// policy is present but does NOT fire for in-team channels.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_keeps_stream_identity_in_engagement_team() {
    let env = stream_env("no-reduce-in-team", FAMILY_PROFILE).await;
    mock_identities(&env).await;
    // Primary-team resolution: team-by-name for the engagement team +
    // channel lookup under it. No --team flag → primary path.
    mock_team_by_name(&env, STREAM_TEAM, "team-codename-id").await;
    mock_channel_for_team(&env, "team-codename-id", "s2-build", "chan-s2-id").await;
    mock_post_create(&env, "post-stream").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &["--json", "post", "s2-build", "stream-internal update"],
    )
    .await;
    assert!(
        out.status.success(),
        "in-team post must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    assert_eq!(
        bearer_for(&env, "POST", "/api/v4/posts").await,
        STREAM_TOKEN,
        "in-engagement-team post must carry the STREAM bearer token (no reduction)"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// whoami: self-query never reduces
// ----------------------------------------------------------------------

/// Brief AC: `chanvoy whoami` (no profile override) returns the stream
/// identity — it is a self-query, not a channel-targeted call, so the
/// reduction policy never applies.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn whoami_returns_stream_identity_no_reduction() {
    let env = stream_env("whoami", FAMILY_PROFILE).await;
    mock_identities(&env).await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "whoami"]).await;
    assert!(
        out.status.success(),
        "whoami must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let identity: serde_json::Value = serde_json::from_slice(&out.stdout).expect("whoami --json");
    assert_eq!(
        identity["username"].as_str(),
        Some(STREAM_BOT),
        "whoami must report the stream identity, never the reduced family identity"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// `profile show`: reports the reduction policy (pure disk read)
// ----------------------------------------------------------------------

/// Brief AC: `chanvoy profile show <stream-profile>` reports the
/// reduction policy. Pure read — no daemon, no network.
#[tokio::test]
async fn profile_show_reports_reduction_policy() {
    let env = stream_env("profile-show", FAMILY_PROFILE).await;

    let out = run_chanvoy(&env, &["--json", "profile", "show", STREAM_PROFILE]).await;
    assert!(
        out.status.success(),
        "profile show must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("profile show --json");
    assert_eq!(v["name"].as_str(), Some(STREAM_PROFILE));
    assert_eq!(v["team_name"].as_str(), Some(STREAM_TEAM));
    assert_eq!(v["reduce"]["use_profile"].as_str(), Some(FAMILY_PROFILE));
    // The family profile exists on disk in this env, so the diagnostic
    // confirms the target resolves.
    assert_eq!(v["reduce"]["use_profile_exists"].as_bool(), Some(true));
}

/// `profile show` on a non-reducing profile reports `reduce: null`.
#[tokio::test]
async fn profile_show_reports_no_reduction_for_family() {
    let env = stream_env("profile-show-family", FAMILY_PROFILE).await;

    let out = run_chanvoy(&env, &["--json", "profile", "show", FAMILY_PROFILE]).await;
    assert!(out.status.success(), "profile show must exit 0");
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("profile show --json");
    assert_eq!(v["name"].as_str(), Some(FAMILY_PROFILE));
    assert!(
        v["reduce"].is_null(),
        "family profile has no reduction policy; got {}",
        v["reduce"]
    );
}

/// `profile show` flags a dangling reduce target (the daemon would
/// refuse to start) so an operator can catch a misconfiguration before
/// bootstrapping.
#[tokio::test]
async fn profile_show_flags_dangling_reduce_target() {
    let env = TestEnv::new(STREAM_PROFILE).await;
    env.write_named_profile(
        STREAM_PROFILE,
        STREAM_BOT,
        STREAM_TEAM,
        "LANYTE_MM_TOKEN",
        Some("dataeng-galaxy-missing"),
    );

    let out = run_chanvoy(&env, &["--json", "profile", "show", STREAM_PROFILE]).await;
    assert!(
        out.status.success(),
        "profile show must exit 0 even when the target is missing"
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("profile show --json");
    assert_eq!(
        v["reduce"]["use_profile"].as_str(),
        Some("dataeng-galaxy-missing")
    );
    assert_eq!(
        v["reduce"]["use_profile_exists"].as_bool(),
        Some(false),
        "dangling reduce target must be reported as non-existent"
    );
}

// ----------------------------------------------------------------------
// Negative case: missing reduce target refuses daemon start
// ----------------------------------------------------------------------

/// Brief AC (negative case): a configured `reduce.use_profile` that does
/// not exist on disk must refuse daemon start with a clear diagnostic —
/// never a silent fall-back to the bare daemon identity. We point the
/// stream profile at a non-existent family profile and assert
/// `daemon serve` exits non-zero naming the missing profile.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn missing_reduce_target_refuses_daemon_start() {
    // reduce target deliberately NOT written to disk.
    let mut env = TestEnv::new(STREAM_PROFILE).await;
    env.set_extra_env(FAMILY_TOKEN_ENV, FAMILY_TOKEN);
    env.write_named_profile(
        STREAM_PROFILE,
        STREAM_BOT,
        STREAM_TEAM,
        "LANYTE_MM_TOKEN",
        Some("dataeng-galaxy-does-not-exist"),
    );
    mock_identities(&env).await;

    // `daemon serve` returns immediately on a start error (before
    // binding the socket); on success it would block, so bound the wait.
    let serve = env
        .chanvoy_command()
        .arg("--profile")
        .arg(STREAM_PROFILE)
        .arg("daemon")
        .arg("serve")
        .output();
    let out = tokio::time::timeout(Duration::from_secs(10), serve)
        .await
        .expect("daemon serve must fail fast on a missing reduce target, not block")
        .expect("spawn daemon serve");

    assert!(
        !out.status.success(),
        "daemon serve must exit non-zero when reduce.use_profile is missing"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("dataeng-galaxy-does-not-exist"),
        "diagnostic must name the missing reduce target; got: {stderr}"
    );
    assert!(
        stderr.contains("ReduceProfileNotFound"),
        "diagnostic must attribute the failure to the reduction policy; got: {stderr}"
    );
}

/// Brief AC negative case + devrev PR #37 P1: a reduce target whose
/// token authenticates as a *different* bot than the family profile
/// names must refuse daemon start — never silently post stream identity
/// under a false family attribution. We give the family profile the
/// SAME `env_name` (`LANYTE_MM_TOKEN`) as the stream, so in this shell
/// it resolves to the stream token and the family client's whoami
/// returns the stream bot. Startup must refuse with
/// `ReduceIdentityMismatch` naming the expected family bot.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn reduce_target_token_shadowed_by_stream_refuses_start() {
    let env = TestEnv::new(STREAM_PROFILE).await;
    // Stream profile reduces to the family profile.
    env.write_named_profile(
        STREAM_PROFILE,
        STREAM_BOT,
        STREAM_TEAM,
        "LANYTE_MM_TOKEN",
        Some(FAMILY_PROFILE),
    );
    // Family profile shares the stream's token env — the leak setup.
    // In this shell LANYTE_MM_TOKEN holds the STREAM token, so the
    // family client would authenticate as the stream bot.
    env.write_named_profile(
        FAMILY_PROFILE,
        FAMILY_BOT,
        FAMILY_TEAM,
        "LANYTE_MM_TOKEN",
        None,
    );
    mock_identities(&env).await;

    let serve = env
        .chanvoy_command()
        .arg("--profile")
        .arg(STREAM_PROFILE)
        .arg("daemon")
        .arg("serve")
        .output();
    let out = tokio::time::timeout(Duration::from_secs(10), serve)
        .await
        .expect("daemon serve must fail fast on a shadowed reduce identity, not block")
        .expect("spawn daemon serve");

    assert!(
        !out.status.success(),
        "daemon serve must exit non-zero when the reduce token authenticates as the wrong bot"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("ReduceIdentityMismatch"),
        "diagnostic must attribute the failure to identity-mismatch; got: {stderr}"
    );
    assert!(
        stderr.contains(FAMILY_BOT) && stderr.contains(STREAM_BOT),
        "diagnostic must name both the expected (family) and actual (stream) bot; got: {stderr}"
    );
}
