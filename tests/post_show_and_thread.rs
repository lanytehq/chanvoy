//! `chanvoy show` and `chanvoy thread` — the two read verbs that take a
//! post id.
//!
//! The contract these tests hold down:
//!
//! - A post id alone is not authority to read a post. Both verbs bind
//!   the post to the channel the operator named, and the binding runs
//!   before any body is handed back or any thread is fetched.
//! - A thread can be named by its root or by any reply in it. Both read
//!   the same thread, because the canonical root comes off the anchor.
//! - `--latest` narrows a thread to its final message without changing
//!   the shape of the response: `--json` is an array either way.
//! - Neither verb touches attention state. They are reads.
//! - Human-readable output carries the post id, so an operator who did
//!   not ask for `--json` can still cite a post to these verbs.
//!
//! Where a test asserts a refusal it asserts on the *absence of the
//! downstream request*, not merely on the error: a bind that ran after
//! the fetch would still return an error while having already leaked
//! the read.

#![allow(dead_code)]

mod common;

use common::{
    read_attention_state, read_attention_state_bytes, run_chanvoy, spawn_daemon,
    stop_daemon_cleanly, TestEnv,
};
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

const CHANNEL: &str = "bravo-team";
const CHANNEL_ID: &str = "chan-id-show-thread";
const OTHER_CHANNEL_ID: &str = "chan-id-out-of-reach";
/// A second team the bot also belongs to, carrying a channel of the
/// same slug as `CHANNEL` under a different channel id. Cross-team
/// resolution is only testable against a team that is not the profile's
/// primary one.
const OTHER_TEAM_ID: &str = "team-id-999";
const OTHER_TEAM_SLUG: &str = "org-otherhq";
const OTHER_TEAM_CHANNEL_ID: &str = "chan-id-other-team";

/// A body marker distinctive enough that a leak assertion cannot pass
/// by accident.
fn body_of(id: &str) -> String {
    format!("BODY-MARKER-{id}")
}

/// One post in the shape Mattermost actually sends: `user_id` and no
/// author name anywhere on it.
fn wire_post(id: &str, channel_id: &str, user_id: &str, create_at: i64, root_id: &str) -> Value {
    json!({
        "id": id,
        "channel_id": channel_id,
        "user_id": user_id,
        "message": body_of(id),
        "create_at": create_at,
        "root_id": root_id,
    })
}

fn posts_envelope(posts: Vec<Value>) -> Value {
    let order: Vec<String> = posts
        .iter()
        .map(|p| p["id"].as_str().unwrap().to_string())
        .collect();
    let map: serde_json::Map<String, Value> = posts
        .into_iter()
        .map(|p| (p["id"].as_str().unwrap().to_string(), p))
        .collect();
    json!({ "order": order, "posts": map })
}

/// `GET /posts/{id}` returning the full post object. The harness's
/// `mock_post_lookup` serves only `{id, channel_id}`, which is enough
/// for an existence assertion but not for a verb that returns the post.
async fn mount_post(env: &TestEnv, post: Value) {
    let id = post["id"].as_str().unwrap().to_string();
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/posts/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(post))
        .mount(&env.mock)
        .await;
}

async fn mount_missing_post(env: &TestEnv, post_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/posts/{post_id}")))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(json!({"status_code": 404, "message": "no"})),
        )
        .mount(&env.mock)
        .await;
}

async fn mount_thread(env: &TestEnv, root_id: &str, posts: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/posts/{root_id}/thread")))
        .respond_with(ResponseTemplate::new(200).set_body_json(posts_envelope(posts)))
        .mount(&env.mock)
        .await;
}

async fn mount_user(env: &TestEnv, user_id: &str, username: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/users/{user_id}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"id": user_id, "username": username})),
        )
        .mount(&env.mock)
        .await;
}

/// `/users/me/teams`, needed by the explicit `<team>/<channel>` and
/// `--team` resolution paths. Takes the full membership list so a test
/// can express "member of these, not that one".
async fn mount_my_teams(env: &TestEnv, teams: &[(&str, &str)]) {
    let body: Vec<Value> = teams
        .iter()
        .map(|(id, name)| json!({"id": id, "name": name, "display_name": name}))
        .collect();
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&env.mock)
        .await;
}

/// How many times the provider's thread endpoint was asked for
/// anything at all.
async fn thread_requests(env: &TestEnv) -> usize {
    env.mock
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|req| req.url.path().ends_with("/thread"))
        .count()
}

fn stdout_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn combined_output(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

// ----------------------------------------------------------------------
// The point-fetch itself: the bind runs before the body is built
// ----------------------------------------------------------------------
//
// The integration tests below prove nothing leaks to the operator.
// These prove *why*: the channel comparison happens before the post is
// hydrated at all, so there is no window in which a body exists to
// leak. Author resolution is the observable proxy — it is the first
// thing hydration does, and it is a separate provider request.

mod point_fetch {
    use super::{body_of, wire_post, CHANNEL_ID, OTHER_CHANNEL_ID};
    use chanvoy_core::{
        CapabilityClass, CoreError, CredentialMode, MattermostClient, Profile, Provider,
    };
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn build_client(mock_url: &str) -> MattermostClient {
        let profile = Profile {
            name: "point-fetch".to_string(),
            role: "bravo-devlead".to_string(),
            scope: "lanytehq".to_string(),
            provider: Provider::Mattermost,
            bot_username: "bot-stable".to_string(),
            team_name: "org-lanytehq".to_string(),
            server_url: mock_url.to_string(),
            env_name: "LANYTE_MM_TOKEN".to_string(),
            env_file: None,
            credential_mode: CredentialMode::EnvName,
            capability_class: CapabilityClass::Standard,
            monitored_channels: vec![],
            ipc: None,
            reduce: None,
        };
        MattermostClient::new(&profile, "fixture-token".to_string()).expect("build client")
    }

    async fn mount(server: &MockServer, post: serde_json::Value) {
        let id = post["id"].as_str().unwrap().to_string();
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/posts/{id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(post))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v4/users/user-a"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"id": "user-a", "username": "alice"})),
            )
            .mount(server)
            .await;
    }

    async fn author_lookups(server: &MockServer) -> usize {
        server
            .received_requests()
            .await
            .expect("received_requests")
            .iter()
            .filter(|req| req.url.path().starts_with("/api/v4/users/"))
            .count()
    }

    /// A post in the expected channel comes back hydrated, with the
    /// author resolved and the thread root normalized.
    #[tokio::test]
    async fn a_post_in_the_expected_channel_comes_back_hydrated() {
        let server = MockServer::start().await;
        mount(
            &server,
            wire_post("post-1", CHANNEL_ID, "user-a", 1_700_000_000_000, ""),
        )
        .await;
        let client = build_client(&server.uri());

        let message = client
            .get_post_in_channel(CHANNEL_ID, "the-channel", "post-1")
            .await
            .expect("post in the expected channel");

        assert_eq!(message.id, "post-1");
        assert_eq!(message.username, "alice", "the author is resolved");
        assert_eq!(
            message.root_id, "post-1",
            "a top-level post names itself as its thread root"
        );
        assert_eq!(message.message, body_of("post-1"));
    }

    /// A post in a different channel is refused, and the refusal lands
    /// before hydration even begins — no author lookup is issued,
    /// because there is no body being assembled to put an author on.
    #[tokio::test]
    async fn a_post_in_another_channel_is_refused_before_it_is_hydrated() {
        let server = MockServer::start().await;
        mount(
            &server,
            wire_post("post-1", OTHER_CHANNEL_ID, "user-a", 1_700_000_000_000, ""),
        )
        .await;
        let client = build_client(&server.uri());

        let error = client
            .get_post_in_channel(CHANNEL_ID, "the-channel", "post-1")
            .await
            .expect_err("a post in another channel must not be returned");

        match &error {
            CoreError::AnchorChannelMismatch { post_id, channel } => {
                assert_eq!(post_id, "post-1");
                assert_eq!(channel, "the-channel");
            }
            other => panic!("expected AnchorChannelMismatch, got {other:?}"),
        }
        assert!(
            !error.to_string().contains(&body_of("post-1")),
            "the refusal must not quote the body it withheld: {error}"
        );
        assert_eq!(
            author_lookups(&server).await,
            0,
            "the channel check runs before hydration; an author lookup \
             would mean a body was already being built"
        );
    }

    /// A missing post is the same refusal the write verbs already give.
    #[tokio::test]
    async fn a_missing_post_is_refused_as_a_missing_anchor() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v4/posts/no-such-post"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"status_code": 404})))
            .mount(&server)
            .await;
        let client = build_client(&server.uri());

        let error = client
            .get_post_in_channel(CHANNEL_ID, "the-channel", "no-such-post")
            .await
            .expect_err("a missing post must not be returned");

        match error {
            CoreError::AnchorNotFound(id) => assert_eq!(id, "no-such-post"),
            other => panic!("expected AnchorNotFound, got {other:?}"),
        }
    }
}

// ----------------------------------------------------------------------
// show
// ----------------------------------------------------------------------

/// `show` on a root and on a reply: one JSON object each, with the
/// author resolved and the thread the post belongs to named.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn show_returns_one_object_for_a_root_and_for_a_reply() {
    let env = TestEnv::new("show-root-and-reply").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-sr", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    mount_post(
        &env,
        wire_post("root-1", CHANNEL_ID, "user-a", 1_700_000_000_000, ""),
    )
    .await;
    mount_post(
        &env,
        wire_post("reply-1", CHANNEL_ID, "user-b", 1_700_000_001_000, "root-1"),
    )
    .await;
    mount_user(&env, "user-a", "alice").await;
    mount_user(&env, "user-b", "bob").await;

    let daemon = spawn_daemon(&env).await;

    let out = run_chanvoy(&env, &["--json", "show", CHANNEL, "root-1"]).await;
    assert!(
        out.status.success(),
        "show on a root must exit 0; output={}",
        combined_output(&out)
    );
    let root: Value = serde_json::from_str(&stdout_of(&out)).expect("show --json parses");
    assert!(root.is_object(), "show emits one object, got {root}");
    assert_eq!(root["id"], "root-1");
    assert_eq!(root["username"], "alice", "the author is resolved by name");
    assert_eq!(
        root["root_id"], "root-1",
        "a root names itself as its thread"
    );
    assert_eq!(root["message"], body_of("root-1"));

    let out = run_chanvoy(&env, &["--json", "show", CHANNEL, "reply-1"]).await;
    assert!(
        out.status.success(),
        "show on a reply must exit 0; output={}",
        combined_output(&out)
    );
    let reply: Value = serde_json::from_str(&stdout_of(&out)).expect("show --json parses");
    assert!(reply.is_object(), "show emits one object, got {reply}");
    assert_eq!(reply["id"], "reply-1");
    assert_eq!(reply["username"], "bob");
    assert_eq!(
        reply["root_id"], "root-1",
        "a reply names the thread it belongs to, not itself"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// A post that lives in another channel is refused, and none of its
/// content reaches the operator. The refusal is what stands between a
/// bare post id and a read of any channel the bot can see.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn show_refuses_a_post_in_another_channel_without_leaking_it() {
    let env = TestEnv::new("show-cross-channel").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-sx", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    mount_post(
        &env,
        wire_post(
            "elsewhere-1",
            OTHER_CHANNEL_ID,
            "user-a",
            1_700_000_000_000,
            "",
        ),
    )
    .await;
    mount_user(&env, "user-a", "alice").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "show", CHANNEL, "elsewhere-1"]).await;

    assert!(
        !out.status.success(),
        "show on an out-of-channel post must exit non-zero"
    );
    let rendered = combined_output(&out);
    assert!(
        rendered.contains("elsewhere-1") && rendered.contains(CHANNEL),
        "the refusal names the post and the channel it is not in: {rendered}"
    );
    assert!(
        !rendered.contains(&body_of("elsewhere-1")),
        "no part of the post body may reach the operator: {rendered}"
    );
    assert!(
        !rendered.contains("alice"),
        "not even the author may be disclosed: {rendered}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// A post that does not exist refuses cleanly rather than rendering an
/// empty or half-built object.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn show_refuses_a_missing_post() {
    let env = TestEnv::new("show-missing").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-sm", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    mount_missing_post(&env, "no-such-post").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "show", CHANNEL, "no-such-post"]).await;

    assert!(
        !out.status.success(),
        "show on a missing post must exit non-zero"
    );
    assert!(
        stdout_of(&out).trim().is_empty(),
        "a refusal prints no result document: {}",
        stdout_of(&out)
    );
    let rendered = combined_output(&out);
    assert!(
        rendered.contains("no-such-post"),
        "the refusal names the post: {rendered}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// thread
// ----------------------------------------------------------------------

/// Naming a reply reads the same thread as naming the root. The
/// provider is deliberately given no answer for the reply's own thread
/// URL, so a call made against the id the operator typed — rather than
/// against the canonical root — fails outright.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn thread_by_reply_id_reads_the_same_thread_as_by_root_id() {
    let env = TestEnv::new("thread-root-vs-reply").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-trr", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    let root = wire_post("root-1", CHANNEL_ID, "user-a", 1_700_000_000_000, "");
    let reply = wire_post("reply-1", CHANNEL_ID, "user-b", 1_700_000_001_000, "root-1");
    mount_post(&env, root.clone()).await;
    mount_post(&env, reply.clone()).await;
    // Only the canonical root's thread URL is answerable.
    mount_thread(&env, "root-1", vec![root, reply]).await;
    mount_user(&env, "user-a", "alice").await;
    mount_user(&env, "user-b", "bob").await;

    let daemon = spawn_daemon(&env).await;

    let by_root = run_chanvoy(&env, &["--json", "thread", CHANNEL, "root-1"]).await;
    assert!(
        by_root.status.success(),
        "thread by root must exit 0; output={}",
        combined_output(&by_root)
    );
    let by_reply = run_chanvoy(&env, &["--json", "thread", CHANNEL, "reply-1"]).await;
    assert!(
        by_reply.status.success(),
        "thread by reply must exit 0; output={}",
        combined_output(&by_reply)
    );

    let from_root: Value = serde_json::from_str(&stdout_of(&by_root)).expect("parses");
    let from_reply: Value = serde_json::from_str(&stdout_of(&by_reply)).expect("parses");
    assert!(from_root.is_array(), "thread emits an array: {from_root}");
    assert_eq!(
        from_root, from_reply,
        "a reply id and its root id read the identical thread"
    );
    assert_eq!(
        from_root
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["root-1", "reply-1"],
        "the thread is the root plus every reply, root first"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `--latest` returns exactly the final message of the thread, still as
/// an array. The fixture's ids sort in the opposite order to its
/// timestamps, so "the last element of whatever the provider's map
/// yielded" is a different post from the right answer.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn thread_latest_is_the_final_message_as_a_one_element_array() {
    let env = TestEnv::new("thread-latest").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-tl", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    let root = wire_post("root-1", CHANNEL_ID, "user-a", 1_700_000_000_000, "");
    // Ids deliberately in the opposite order to timestamps: the newest
    // reply sorts first by id, so an unsorted implementation picks
    // "zz-reply" and fails here.
    let older = wire_post(
        "zz-reply",
        CHANNEL_ID,
        "user-b",
        1_700_000_001_000,
        "root-1",
    );
    let newest = wire_post(
        "aa-reply",
        CHANNEL_ID,
        "user-c",
        1_700_000_002_000,
        "root-1",
    );
    mount_post(&env, root.clone()).await;
    mount_thread(&env, "root-1", vec![root, older, newest]).await;
    mount_user(&env, "user-a", "alice").await;
    mount_user(&env, "user-b", "bob").await;
    mount_user(&env, "user-c", "carol").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "thread", CHANNEL, "root-1", "--latest"]).await;
    assert!(
        out.status.success(),
        "thread --latest must exit 0; output={}",
        combined_output(&out)
    );

    let value: Value = serde_json::from_str(&stdout_of(&out)).expect("parses");
    let array = value
        .as_array()
        .unwrap_or_else(|| panic!("--latest must not change the JSON type: {value}"));
    assert_eq!(array.len(), 1, "--latest is one element, got {value}");
    assert_eq!(
        array[0]["id"], "aa-reply",
        "the final message is the newest one, not the last by id"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// A thread anchored on a post in another channel is refused and the
/// provider's thread endpoint is never asked for anything. The endpoint
/// is mounted and would answer happily — the point is that the request
/// is not made.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn thread_with_a_mismatched_channel_makes_no_thread_request() {
    let env = TestEnv::new("thread-cross-channel").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-tx", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    let elsewhere = wire_post(
        "elsewhere-1",
        OTHER_CHANNEL_ID,
        "user-a",
        1_700_000_000_000,
        "",
    );
    mount_post(&env, elsewhere.clone()).await;
    mount_thread(&env, "elsewhere-1", vec![elsewhere]).await;
    mount_user(&env, "user-a", "alice").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "thread", CHANNEL, "elsewhere-1"]).await;

    assert!(
        !out.status.success(),
        "thread on an out-of-channel anchor must exit non-zero"
    );
    let rendered = combined_output(&out);
    assert!(
        !rendered.contains(&body_of("elsewhere-1")),
        "no part of the thread may reach the operator: {rendered}"
    );
    assert_eq!(
        thread_requests(&env).await,
        0,
        "a cross-channel thread read must issue no thread request at all"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// A thread whose anchor is legitimately in the named channel, but
/// whose envelope also carries a reply from a channel the operator did
/// not name, is refused whole — including the in-channel root.
///
/// This is the case the anchor bind alone does not cover. The bot's
/// credential reaches every channel it is a member of; the channel the
/// operator named is the narrower scope, and it is the one that governs
/// the read. An envelope that mixes the two is not a thread the
/// operator asked for, and there is no honest way to hand back "the
/// part you were allowed to see": once a post becomes a `Message` its
/// channel is gone, so a partial result would be labelled with the
/// requested channel and be wrong about it.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn thread_with_an_out_of_channel_reply_is_refused_whole() {
    let env = TestEnv::new("thread-mixed-envelope").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-tme", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    // The anchor is in the requested channel, so the bind on it passes
    // and the thread request is genuinely issued.
    let root = wire_post("root-1", CHANNEL_ID, "user-a", 1_700_000_000_000, "");
    let stray = wire_post(
        "stray-reply",
        OTHER_CHANNEL_ID,
        "user-b",
        1_700_000_001_000,
        "root-1",
    );
    mount_post(&env, root.clone()).await;
    mount_thread(&env, "root-1", vec![root, stray]).await;
    mount_user(&env, "user-a", "alice").await;
    mount_user(&env, "user-b", "bob").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "thread", CHANNEL, "root-1"]).await;

    assert!(
        !out.status.success(),
        "a thread carrying an out-of-channel post must exit non-zero; output={}",
        combined_output(&out)
    );
    assert!(
        stdout_of(&out).trim().is_empty(),
        "a refusal prints no result document: {}",
        stdout_of(&out)
    );
    let rendered = combined_output(&out);
    for id in ["root-1", "stray-reply"] {
        assert!(
            !rendered.contains(&body_of(id)),
            "no body from the envelope may reach the operator, including the \
             in-channel root's: {rendered}"
        );
    }
    assert!(
        !rendered.contains(OTHER_CHANNEL_ID),
        "the refusal must not name the channel the stray post came from: {rendered}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `--latest` is the newest message, not the last element of the list.
///
/// The thread ordering pins the root first no matter what its timestamp
/// says, so on a thread whose root is newer than its reply the two
/// answers differ: the tail is the reply, the newest message is the
/// root. A root can legitimately carry the later timestamp — a post
/// edited after it was replied to, or a backdated import — and an
/// operator asking for the latest message wants the latest message.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn thread_latest_is_the_newest_message_even_when_the_root_is_newest() {
    let env = TestEnv::new("thread-latest-backdated").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-tlb", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    // The root's timestamp is LATER than its reply's. Root-first
    // ordering still puts the root at index 0, so the tail of the list
    // is the reply — the wrong answer.
    let root = wire_post("root-1", CHANNEL_ID, "user-a", 1_700_000_009_000, "");
    let reply = wire_post("reply-1", CHANNEL_ID, "user-b", 1_700_000_001_000, "root-1");
    mount_post(&env, root.clone()).await;
    mount_thread(&env, "root-1", vec![root, reply]).await;
    mount_user(&env, "user-a", "alice").await;
    mount_user(&env, "user-b", "bob").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "thread", CHANNEL, "root-1", "--latest"]).await;
    assert!(
        out.status.success(),
        "thread --latest must exit 0; output={}",
        combined_output(&out)
    );

    let value: Value = serde_json::from_str(&stdout_of(&out)).expect("parses");
    let array = value
        .as_array()
        .unwrap_or_else(|| panic!("--latest must not change the JSON type: {value}"));
    assert_eq!(array.len(), 1, "--latest is one element, got {value}");
    assert_eq!(
        array[0]["id"], "root-1",
        "the newest message here is the root; taking the tail of a \
         root-pinned list returns the reply instead: {value}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// Cursor neutrality
// ----------------------------------------------------------------------

/// `show`, `thread`, and `thread --latest` are reads. Attention state
/// must come out of all three byte-identical.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn show_and_thread_do_not_advance_the_cursor() {
    let env = TestEnv::new("show-thread-cursor").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-stc", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    let root = wire_post("root-1", CHANNEL_ID, "user-a", 1_700_000_000_000, "");
    let reply = wire_post("reply-1", CHANNEL_ID, "user-b", 1_700_000_001_000, "root-1");
    mount_post(&env, root.clone()).await;
    mount_thread(&env, "root-1", vec![root, reply]).await;
    mount_user(&env, "user-a", "alice").await;
    mount_user(&env, "user-b", "bob").await;

    // Seed a real cursor first. Without this the state file does not
    // exist, every comparison is `None == None`, and the test can only
    // catch a verb that CREATES state — never one that mutates an
    // existing entry, which is what every real cursor writer does.
    env.mock_post_create("seed-post-1").await;
    let daemon = spawn_daemon(&env).await;
    let seed = run_chanvoy(&env, &["post", CHANNEL, "seed"]).await;
    assert!(
        seed.status.success(),
        "seeding post must succeed; output={}",
        combined_output(&seed)
    );

    let pre_state =
        read_attention_state_bytes(&env).expect("a cursor exists after the seeding post");
    let parsed = read_attention_state(&env).expect("state parses");
    assert!(
        parsed.channels.keys().any(|key| key.ends_with(CHANNEL)),
        "the seeded channel must have a cursor entry, or there is nothing for a \
         read to accidentally advance; keys={:?}",
        parsed.channels.keys().collect::<Vec<_>>()
    );

    for args in [
        vec!["show", CHANNEL, "root-1"],
        vec!["thread", CHANNEL, "root-1"],
        vec!["thread", CHANNEL, "root-1", "--latest"],
    ] {
        let out = run_chanvoy(&env, &args).await;
        assert!(
            out.status.success(),
            "{args:?} must exit 0; output={}",
            combined_output(&out)
        );
        assert_eq!(
            Some(&pre_state),
            read_attention_state_bytes(&env).as_ref(),
            "{args:?} is a read and must not mutate attention state"
        );
    }

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// Human-readable output
// ----------------------------------------------------------------------

/// Without `--json` an operator still gets a citation they can hand
/// straight back to `show` / `thread` / `post --reply-to`. Every row
/// carries both crumbs: its own id, and the thread it belongs to —
/// which for a top-level post is itself.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_human_output_carries_the_post_id_and_a_root_on_every_row() {
    let env = TestEnv::new("read-id-crumb").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-crumb", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/channels/{CHANNEL_ID}/posts")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(posts_envelope(vec![
                wire_post("root-1", CHANNEL_ID, "user-a", 1_700_000_000_000, ""),
                wire_post("reply-1", CHANNEL_ID, "user-b", 1_700_000_001_000, "root-1"),
            ])),
        )
        .mount(&env.mock)
        .await;
    mount_user(&env, "user-a", "alice").await;
    mount_user(&env, "user-b", "bob").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["read", CHANNEL]).await;
    assert!(
        out.status.success(),
        "read must exit 0; output={}",
        combined_output(&out)
    );
    let rendered = stdout_of(&out);

    assert!(
        rendered.contains("id=root-1"),
        "every row carries its full post id: {rendered}"
    );
    assert!(
        rendered.contains("id=reply-1"),
        "every row carries its full post id: {rendered}"
    );
    assert!(
        rendered.contains("id=root-1 root=root-1"),
        "a top-level row names itself as its own thread, so an operator \
         never has to infer which id `--reply-to` wants: {rendered}"
    );
    assert!(
        rendered.contains("id=reply-1 root=root-1"),
        "a reply names the thread it belongs to: {rendered}"
    );
    assert_eq!(
        rendered.matches("root=").count(),
        2,
        "both rows carry a root crumb — the root's is not omitted: {rendered}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// Clap surface
// ----------------------------------------------------------------------

/// Both cross-team spellings select a *different* team from the
/// profile's primary one, and a team the bot is not in is refused.
///
/// The fixture gives two teams a channel of the same slug, each holding
/// a differently-marked post. That duplicate slug is the whole point:
/// with a single team, or with the primary team as the only membership,
/// `<team>/` and `--team` can be deleted outright and every assertion
/// still passes, because primary-team resolution lands on the same
/// channel either way. Here the two spellings must reach the second
/// team's channel — and its post — or the read is bound against the
/// primary team's channel id and refused.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn team_syntax_and_team_flag_select_a_non_primary_team() {
    let env = TestEnv::new("show-thread-clap").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-clap", "agent-bravo-devlead", "team-id-456")
        .await;
    mount_my_teams(
        &env,
        &[
            ("team-id-456", "org-lanytehq"),
            (OTHER_TEAM_ID, OTHER_TEAM_SLUG),
        ],
    )
    .await;
    // Same channel slug in both teams, different channel ids.
    env.mock_channel_lookup(CHANNEL, CHANNEL_ID).await;
    env.mock_channel_lookup_for_team(OTHER_TEAM_ID, CHANNEL, OTHER_TEAM_CHANNEL_ID)
        .await;

    let primary_root = wire_post("root-1", CHANNEL_ID, "user-a", 1_700_000_000_000, "");
    let other_root = wire_post(
        "other-root-1",
        OTHER_TEAM_CHANNEL_ID,
        "user-b",
        1_700_000_000_000,
        "",
    );
    mount_post(&env, primary_root.clone()).await;
    mount_post(&env, other_root.clone()).await;
    mount_thread(&env, "other-root-1", vec![other_root]).await;
    mount_user(&env, "user-a", "alice").await;
    mount_user(&env, "user-b", "bob").await;

    let daemon = spawn_daemon(&env).await;

    // `<team>/<channel>` reaches the second team's channel, and the post
    // that lives there.
    let qualified = run_chanvoy(
        &env,
        &[
            "--json",
            "show",
            &format!("{OTHER_TEAM_SLUG}/{CHANNEL}"),
            "other-root-1",
        ],
    )
    .await;
    assert!(
        qualified.status.success(),
        "<team>/<channel> must resolve against the named team, not the \
         primary one; output={}",
        combined_output(&qualified)
    );
    let value: Value = serde_json::from_str(&stdout_of(&qualified)).expect("parses");
    assert_eq!(value["id"], "other-root-1");
    assert_eq!(
        value["message"],
        body_of("other-root-1"),
        "the body is the second team's post, not the primary team's: {value}"
    );

    // `--team` does the same, and `--latest` still parses alongside it.
    let flagged = run_chanvoy(
        &env,
        &[
            "--json",
            "thread",
            CHANNEL,
            "other-root-1",
            "--latest",
            "--team",
            OTHER_TEAM_SLUG,
        ],
    )
    .await;
    assert!(
        flagged.status.success(),
        "--team must select the named team and --latest must parse \
         alongside it; output={}",
        combined_output(&flagged)
    );
    let value: Value = serde_json::from_str(&stdout_of(&flagged)).expect("parses");
    let array = value
        .as_array()
        .unwrap_or_else(|| panic!("--latest must not change the JSON type: {value}"));
    assert_eq!(
        array.len(),
        1,
        "--latest on a one-post thread is still a one-element array: {value}"
    );
    assert_eq!(array[0]["id"], "other-root-1");

    // The duplicate slug is not a shortcut between teams: naming the
    // primary team's channel reaches the primary team's channel, so the
    // other team's post is refused there.
    let crossed = run_chanvoy(
        &env,
        &[
            "--json",
            "show",
            &format!("org-lanytehq/{CHANNEL}"),
            "other-root-1",
        ],
    )
    .await;
    assert!(
        !crossed.status.success(),
        "a post from the other team's same-named channel must not resolve \
         through the primary team; output={}",
        combined_output(&crossed)
    );
    assert!(
        !combined_output(&crossed).contains(&body_of("other-root-1")),
        "no body may leak across the duplicate slug: {}",
        combined_output(&crossed)
    );

    // A team the bot is not a member of is refused by name, and the
    // refusal is about the team rather than the channel or the post.
    let stranger = run_chanvoy(
        &env,
        &["--json", "show", "org-not-a-member/bravo-team", "root-1"],
    )
    .await;
    assert!(
        !stranger.status.success(),
        "a team outside the bot's membership must be refused; output={}",
        combined_output(&stranger)
    );
    let rendered = combined_output(&stranger);
    assert!(
        rendered.contains("org-not-a-member"),
        "the refusal names the team that was asked for: {rendered}"
    );
    assert!(
        !rendered.contains(&body_of("root-1")),
        "a wrong-team read must not fall back to a team that does have \
         the channel: {rendered}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}
