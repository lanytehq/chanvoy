//! PER-024: conversation-shape primitives integration tests.
//!
//! Covers the two primitives shipped in PER-024, end-to-end through the
//! real daemon binary against a wiremock Mattermost server:
//!
//! 1. `chanvoy post --reply-to <post_id>` — threaded replies; additive
//!    `PostReceipt` shape (`{ id, parent_id }` for threaded; `{ id }`
//!    only for non-threaded baseline preservation per AC #3a)
//! 2. `chanvoy react <channel> <post_id> <emoji>` /
//!    `chanvoy unreact <channel> <post_id> <emoji>` — emoji reactions;
//!    channel-required positional, idempotent on duplicate / missing,
//!    cursor-neutral
//!
//! Validation order (resolve → verify → write) per AC #3 + #5a is
//! tested via wiremock `received_requests()` assertions to confirm
//! the write endpoint is NOT hit on wrong-channel inputs (per
//! @agent-bravo-devrev's PER-024 pre-impl pin #5).
//!
//! Idempotency (per AC #5b + devrev pre-impl pin #3) is tested
//! deliberately at the chanvoy-core layer: re-react on MM 2xx happy
//! path, unreact-when-not-reacted via 404 normalization to success.
//!
//! Shared harness primitives live in `tests/common/mod.rs`.

#![allow(dead_code)]

mod common;

use common::{read_attention_state, run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use wiremock::http::Method;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

// ----------------------------------------------------------------------
// CLI-level rejection tests (no daemon needed)
// ----------------------------------------------------------------------

/// `chanvoy react` requires three positional args (channel, post_id,
/// emoji). Missing any rejects at the clap layer.
#[tokio::test]
async fn react_missing_args_rejected() {
    let env = TestEnv::new("per-024-react-missing").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    // Missing emoji.
    let out = run_chanvoy(&env, &["react", "some-channel", "some-post-id"]).await;
    assert!(
        !out.status.success(),
        "react with two args must reject (missing emoji)"
    );

    // Missing all positionals.
    let out = run_chanvoy(&env, &["react"]).await;
    assert!(!out.status.success(), "bare `react` must reject");
}

/// `chanvoy unreact` mirrors `react`'s positional arg requirements.
#[tokio::test]
async fn unreact_missing_args_rejected() {
    let env = TestEnv::new("per-024-unreact-missing").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(&env, &["unreact", "some-channel", "some-post-id"]).await;
    assert!(!out.status.success(), "unreact missing emoji must reject");
}

// ----------------------------------------------------------------------
// Daemon-spawn integration tests
// ----------------------------------------------------------------------

/// `chanvoy post <channel> <message>` (no --reply-to): existing
/// `PostReceipt { id }` JSON shape is preserved byte-for-byte; no
/// `parent_id` key appears. Per AC #3a additive-only contract.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_non_threaded_json_shape_preserved() {
    let env = TestEnv::new("per-024-post-non-threaded").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-pn", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-pn").await;
    env.mock_post_create("post-pn-1").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "post", "bravo-team", "hello"]).await;
    assert!(
        out.status.success(),
        "post must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("post --json parses");
    assert_eq!(parsed["id"].as_str(), Some("post-pn-1"), "id field present");
    // PER-024 AC #3a: non-threaded receipts have NO parent_id key
    // (skip_serializing_if = "Option::is_none" ensures absent vs null).
    assert!(
        !parsed
            .as_object()
            .expect("object")
            .contains_key("parent_id"),
        "non-threaded post --json must NOT contain parent_id key; got: {}",
        serde_json::to_string(&parsed).unwrap()
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy post --reply-to <parent>`: returns `{ id, parent_id }`
/// additive shape. Validation order succeeds; threaded write hits
/// `/posts` with `root_id` field.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_reply_to_threaded_json_includes_parent_id() {
    let env = TestEnv::new("per-024-post-threaded").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-pt", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-pt").await;
    // Parent post exists on the resolved channel — validation
    // succeeds.
    env.mock_post_lookup("parent-post-pt", "chan-id-pt", true)
        .await;
    env.mock_post_create("post-pt-reply").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "--json",
            "post",
            "bravo-team",
            "ack on finding",
            "--reply-to",
            "parent-post-pt",
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "threaded post must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("threaded post --json parses");
    assert_eq!(parsed["id"].as_str(), Some("post-pt-reply"));
    assert_eq!(
        parsed["parent_id"].as_str(),
        Some("parent-post-pt"),
        "threaded post --json must surface parent_id additively"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy post --reply-to <parent>` where parent is on a DIFFERENT
/// channel: refuse with diagnostic; NO write attempted. Per AC #3
/// validation order + devrev pin #5 (received_requests assertion).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_reply_to_wrong_channel_no_write() {
    let env = TestEnv::new("per-024-post-wrong-channel").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-pw", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-pw").await;
    // Parent post is on a DIFFERENT channel id — validation must
    // refuse before reaching the write.
    env.mock_post_lookup("parent-post-pw", "OTHER-CHANNEL", true)
        .await;
    // Intentionally NOT mounting POST /posts — if it gets called
    // wiremock returns 404 by default; the assertion below catches
    // any actual write attempt.

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "post",
            "bravo-team",
            "should not post",
            "--reply-to",
            "parent-post-pw",
        ],
    )
    .await;
    assert!(
        !out.status.success(),
        "wrong-channel post --reply-to must exit non-zero"
    );

    // Devrev pin #5: the validation order is "no write attempted on
    // mismatch." Confirm via wiremock's request log.
    let requests = env.mock.received_requests().await.unwrap_or_default();
    let post_writes = requests
        .iter()
        .filter(|r| r.method == Method::POST && r.url.path() == "/api/v4/posts")
        .count();
    assert_eq!(
        post_writes, 0,
        "wrong-channel reply-to must NOT issue POST /posts; observed {post_writes}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy react <channel> <post_id> <emoji>` happy path: emoji is
/// added under the bot identity (POST /reactions hit). Returns
/// `ReactionResult { team, channel, post_id, emoji, ok: true }`.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn react_happy_path() {
    let env = TestEnv::new("per-024-react-happy").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-rh", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-rh").await;
    env.mock_post_lookup("post-rh", "chan-id-rh", true).await;
    Mock::given(method("POST"))
        .and(path("/api/v4/reactions"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "user_id": "bot-id-rh",
            "post_id": "post-rh",
            "emoji_name": "+1",
            "create_at": 1_700_000_000_000i64,
        })))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "react", "bravo-team", "post-rh", "+1"]).await;
    assert!(
        out.status.success(),
        "react must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("react --json parses");
    assert_eq!(result["ok"].as_bool(), Some(true));
    assert_eq!(result["team"].as_str(), Some("org-lanytehq"));
    assert_eq!(result["channel"].as_str(), Some("bravo-team"));
    assert_eq!(result["post_id"].as_str(), Some("post-rh"));
    assert_eq!(result["emoji"].as_str(), Some("+1"));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy react :+1:` (colon-wrapped emoji): the colons are stripped
/// before the API call and the canonical bare form surfaces in `--json`.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn react_colon_wrapped_emoji_stripped() {
    let env = TestEnv::new("per-024-react-colon").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-rc", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-rc").await;
    env.mock_post_lookup("post-rc", "chan-id-rc", true).await;
    Mock::given(method("POST"))
        .and(path("/api/v4/reactions"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "react", "bravo-team", "post-rc", ":+1:"]).await;
    assert!(out.status.success(), "colon-wrapped react must succeed");
    let result: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("react --json parses");
    // Canonical stripped form in output.
    assert_eq!(result["emoji"].as_str(), Some("+1"));

    // Verify the body sent to MM contained the stripped form, not
    // `:+1:`.
    let requests = env.mock.received_requests().await.unwrap_or_default();
    let reaction_post = requests
        .iter()
        .find(|r| r.method == Method::POST && r.url.path() == "/api/v4/reactions")
        .expect("POST /reactions was made");
    let body: serde_json::Value =
        serde_json::from_slice(&reaction_post.body).expect("reaction body parses");
    assert_eq!(
        body["emoji_name"].as_str(),
        Some("+1"),
        "MM API receives the stripped emoji_name, not the colon-wrapped form"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy react` on a post that doesn't exist on the resolved channel:
/// refuse before writing. Validation order per AC #5a; NO call to
/// POST /reactions per devrev pin #5.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn react_wrong_channel_no_write() {
    let env = TestEnv::new("per-024-react-wrong").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-rw", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-rw").await;
    env.mock_post_lookup("post-rw", "OTHER-CHANNEL", true).await;
    // POST /reactions intentionally NOT mounted.

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["react", "bravo-team", "post-rw", "+1"]).await;
    assert!(
        !out.status.success(),
        "react with wrong-channel post must exit non-zero"
    );

    let requests = env.mock.received_requests().await.unwrap_or_default();
    let reaction_writes = requests
        .iter()
        .filter(|r| r.method == Method::POST && r.url.path() == "/api/v4/reactions")
        .count();
    assert_eq!(
        reaction_writes, 0,
        "wrong-channel react must NOT issue POST /reactions; observed {reaction_writes}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy react` does NOT advance the attention cursor (AC #5b
/// cursor-no-op invariant). Pre/post-react attention state must
/// match byte-for-byte.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn react_does_not_advance_cursor() {
    let env = TestEnv::new("per-024-react-cursor").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-rcu", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-rcu").await;
    env.mock_post_lookup("post-rcu", "chan-id-rcu", true).await;
    Mock::given(method("POST"))
        .and(path("/api/v4/reactions"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let pre_state = read_attention_state(&env);

    let out = run_chanvoy(&env, &["react", "bravo-team", "post-rcu", "+1"]).await;
    assert!(out.status.success());

    let post_state = read_attention_state(&env);
    assert_eq!(
        pre_state, post_state,
        "AC #5b: react must not mutate attention state"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy unreact` happy path: DELETE
/// /users/{user_id}/posts/{post_id}/reactions/{emoji} hit; result
/// indicates ok.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn unreact_happy_path() {
    let env = TestEnv::new("per-024-unreact-happy").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-uh", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-uh").await;
    env.mock_post_lookup("post-uh", "chan-id-uh", true).await;
    Mock::given(method("DELETE"))
        .and(path("/api/v4/users/bot-id-uh/posts/post-uh/reactions/+1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "OK"})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "unreact", "bravo-team", "post-uh", "+1"]).await;
    assert!(
        out.status.success(),
        "unreact must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("unreact --json parses");
    assert_eq!(result["ok"].as_bool(), Some(true));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy unreact` when the reaction doesn't exist: MM returns 404,
/// chanvoy-core normalizes to success per AC #5b + devrev pin #3
/// (idempotency is deliberate at the chanvoy-core layer).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn unreact_idempotent_on_missing_reaction() {
    let env = TestEnv::new("per-024-unreact-missing").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-um", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-um").await;
    env.mock_post_lookup("post-um", "chan-id-um", true).await;
    Mock::given(method("DELETE"))
        .and(path("/api/v4/users/bot-id-um/posts/post-um/reactions/+1"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(serde_json::json!({"status_code": 404, "message": "not found"})),
        )
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "unreact", "bravo-team", "post-um", "+1"]).await;
    assert!(
        out.status.success(),
        "unreact-when-not-reacted must succeed (idempotent); stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("unreact --json parses");
    assert_eq!(
        result["ok"].as_bool(),
        Some(true),
        "404 is normalized to success at chanvoy-core layer"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy unreact` does NOT advance the attention cursor.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn unreact_does_not_advance_cursor() {
    let env = TestEnv::new("per-024-unreact-cursor").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ucu", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-ucu").await;
    env.mock_post_lookup("post-ucu", "chan-id-ucu", true).await;
    Mock::given(method("DELETE"))
        .and(path("/api/v4/users/bot-id-ucu/posts/post-ucu/reactions/+1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "OK"})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let pre_state = read_attention_state(&env);

    let out = run_chanvoy(&env, &["unreact", "bravo-team", "post-ucu", "+1"]).await;
    assert!(out.status.success());

    let post_state = read_attention_state(&env);
    assert_eq!(
        pre_state, post_state,
        "AC #5b: unreact must not mutate attention state"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// Devrev PR #21 round-1 P2 follow-ups
// ----------------------------------------------------------------------

/// Mount the additional mocks needed for cross-team `<team>/<channel>`
/// resolution: `/users/me/teams` enumeration plus the by-name lookup
/// against the alternate team. Mirrors the PER-023 helper from
/// `per_023_session_start.rs` — the alternate team needs to surface in
/// the bot's membership list so the γ hybrid resolver can route
/// explicit `<other-team>/<channel>` invocations.
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

/// AC #10: re-react with the same emoji is a no-op success. Mattermost
/// returns 2xx on the duplicate-react path (it returns the existing
/// reaction object); chanvoy-core accepts any 2xx as success without
/// distinguishing "added" from "already present" on the operator
/// surface. Per @agent-bravo-devrev's PR #21 round-1 P2 finding #1
/// (2026-05-05): pin the idempotency deliberately with a focused
/// duplicate-react test rather than relying on the generic happy-path
/// test to imply it.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn react_idempotent_on_duplicate() {
    let env = TestEnv::new("per-024-react-duplicate").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-rd", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-rd").await;
    env.mock_post_lookup("post-rd", "chan-id-rd", true).await;
    // MM returns 200 with the existing reaction object on duplicate
    // (per MM API: POST /reactions is idempotent). Same response shape
    // serves both "added" and "already present" cases — chanvoy
    // doesn't distinguish.
    Mock::given(method("POST"))
        .and(path("/api/v4/reactions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "user_id": "bot-id-rd",
            "post_id": "post-rd",
            "emoji_name": "+1",
            "create_at": 1_700_000_000_000i64,
        })))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;

    // First react — establishes the reaction.
    let out1 = run_chanvoy(&env, &["--json", "react", "bravo-team", "post-rd", "+1"]).await;
    assert!(
        out1.status.success(),
        "first react must succeed; stderr={}",
        String::from_utf8_lossy(&out1.stderr)
    );

    // Second react with the same emoji on the same post — duplicate
    // path. Brief AC #10: must be no-op success (success exit, ok:
    // true).
    let out2 = run_chanvoy(&env, &["--json", "react", "bravo-team", "post-rd", "+1"]).await;
    assert!(
        out2.status.success(),
        "duplicate-react must succeed (idempotent); stderr={}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&out2.stdout).expect("duplicate-react --json parses");
    assert_eq!(
        result["ok"].as_bool(),
        Some(true),
        "duplicate-react must report ok: true"
    );

    // Sanity: both calls hit POST /reactions (chanvoy doesn't
    // short-circuit; MM's idempotent semantics are what we rely on).
    let requests = env.mock.received_requests().await.unwrap_or_default();
    let reaction_writes = requests
        .iter()
        .filter(|r| r.method == Method::POST && r.url.path() == "/api/v4/reactions")
        .count();
    assert_eq!(
        reaction_writes, 2,
        "both react calls hit POST /reactions; idempotency is server-side, not client-cached"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// AC #10: `<team>/<channel>` syntax works on `post --reply-to`. Per
/// @agent-bravo-devrev's PR #21 round-1 P2 finding #2 (2026-05-05):
/// explicit cross-team test confirms the alternate-team channel
/// lookup is exercised and the threaded write lands on the resolved
/// team's channel id.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_reply_to_cross_team_syntax() {
    let env = TestEnv::new("per-024-post-reply-cross").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-pc", "agent-bravo-devlead", "team-id-456")
        .await;
    mount_alternate_team(&env, "team-id-456", "org-3leaps", "team-id-3leaps").await;
    // Channel exists on the alternate team only — confirms the
    // resolver is routing on the explicit team slug, not falling
    // through to the primary.
    env.mock_channel_lookup_for_team("team-id-3leaps", "ops-tech", "chan-id-3l-pc")
        .await;
    env.mock_post_lookup("parent-3l", "chan-id-3l-pc", true)
        .await;
    env.mock_post_create("post-3l-reply").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "--json",
            "post",
            "org-3leaps/ops-tech",
            "ack on cross-team finding",
            "--reply-to",
            "parent-3l",
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "cross-team threaded post must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("threaded post --json parses");
    assert_eq!(parsed["id"].as_str(), Some("post-3l-reply"));
    assert_eq!(
        parsed["parent_id"].as_str(),
        Some("parent-3l"),
        "threaded receipt surfaces parent_id additively on cross-team path too"
    );

    // Confirm the write went to the resolved alternate-team channel
    // id, not the primary team's. The post body should include
    // channel_id = "chan-id-3l-pc" (the alt-team channel).
    let requests = env.mock.received_requests().await.unwrap_or_default();
    let post_create = requests
        .iter()
        .find(|r| r.method == Method::POST && r.url.path() == "/api/v4/posts")
        .expect("POST /posts was made");
    let body: serde_json::Value =
        serde_json::from_slice(&post_create.body).expect("post body parses");
    assert_eq!(
        body["channel_id"].as_str(),
        Some("chan-id-3l-pc"),
        "threaded write lands on alternate-team channel id, not primary team's"
    );
    assert_eq!(
        body["root_id"].as_str(),
        Some("parent-3l"),
        "threaded write carries the resolved parent post id"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// AC #10: `<team>/<channel>` syntax works on `react`/`unreact`. Per
/// @agent-bravo-devrev's PR #21 round-1 P2 finding #2 (2026-05-05):
/// covers both the cross-team channel resolution and the
/// `ReactionResult` cross-team disambiguation (resolved `team` field
/// reflects the explicitly-requested team, not the profile's primary).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn react_cross_team_syntax_records_resolved_team() {
    let env = TestEnv::new("per-024-react-cross").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-rc2", "agent-bravo-devlead", "team-id-456")
        .await;
    mount_alternate_team(&env, "team-id-456", "org-3leaps", "team-id-3leaps").await;
    env.mock_channel_lookup_for_team("team-id-3leaps", "ops-tech", "chan-id-3l-rc")
        .await;
    env.mock_post_lookup("post-3l-rc", "chan-id-3l-rc", true)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/reactions"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &["--json", "react", "org-3leaps/ops-tech", "post-3l-rc", "+1"],
    )
    .await;
    assert!(
        out.status.success(),
        "cross-team react must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("react --json parses");
    assert_eq!(
        result["team"].as_str(),
        Some("org-3leaps"),
        "ReactionResult.team reflects the resolved alternate-team slug, confirming γ resolver routed via explicit"
    );
    assert_eq!(result["channel"].as_str(), Some("ops-tech"));
    assert_eq!(result["post_id"].as_str(), Some("post-3l-rc"));
    assert_eq!(result["ok"].as_bool(), Some(true));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}
