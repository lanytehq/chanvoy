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
