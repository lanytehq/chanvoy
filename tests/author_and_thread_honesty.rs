//! Author resolution and thread completeness against a mocked
//! Mattermost.
//!
//! The contract these tests hold down:
//!
//! - A real Mattermost post object carries `user_id` and no author
//!   name, so every fixture here omits the name and serves it from
//!   `GET /users/{user_id}` instead.
//! - No post is ever dropped for want of an author name, and no post
//!   is ever attributed to the literal word "unknown". When the name
//!   cannot be resolved, the author is the user id — something an
//!   operator can actually look up.
//! - A thread read returns the root plus every reply.
//! - Author lookups are cached, shared between clones of a client, and
//!   never re-issued for an author already resolved.

use std::collections::BTreeMap;

use chanvoy_core::{CapabilityClass, CredentialMode, MattermostClient, Message, Profile, Provider};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TEAM_SLUG: &str = "team-slug-stable";
const CHANNEL_ID: &str = "channel-id-stable";
const ROOT_ID: &str = "root-post";

fn build_client(mock_url: &str) -> MattermostClient {
    let profile = Profile {
        name: "author-honesty".to_string(),
        role: "bravo-devlead".to_string(),
        scope: "lanytehq".to_string(),
        provider: Provider::Mattermost,
        bot_username: "bot-stable".to_string(),
        team_name: TEAM_SLUG.to_string(),
        server_url: mock_url.to_string(),
        env_name: "LANYTE_MM_TOKEN".to_string(),
        env_file: None,
        credential_mode: CredentialMode::EnvName,
        capability_class: CapabilityClass::Standard,
        monitored_channels: vec![],
        ipc: None,
        reduce: None,
    };
    MattermostClient::new(&profile, "fixture-token".to_string()).expect("build MattermostClient")
}

/// One post object in the shape Mattermost actually sends: no author
/// name anywhere on it.
fn wire_post(id: &str, user_id: &str, create_at: i64, root_id: &str) -> serde_json::Value {
    wire_post_in_channel(id, CHANNEL_ID, user_id, create_at, root_id)
}

/// `wire_post` with the channel spelled out, for the cases where the
/// point is that a post is *not* in the channel under test.
fn wire_post_in_channel(
    id: &str,
    channel_id: &str,
    user_id: &str,
    create_at: i64,
    root_id: &str,
) -> serde_json::Value {
    json!({
        "id": id,
        "channel_id": channel_id,
        "user_id": user_id,
        "message": format!("body of {id}"),
        "create_at": create_at,
        "root_id": root_id,
    })
}

fn posts_envelope(posts: Vec<serde_json::Value>) -> serde_json::Value {
    let order: Vec<String> = posts
        .iter()
        .map(|p| p["id"].as_str().unwrap().to_string())
        .collect();
    let map: serde_json::Map<String, serde_json::Value> = posts
        .into_iter()
        .map(|p| (p["id"].as_str().unwrap().to_string(), p))
        .collect();
    json!({ "order": order, "posts": map })
}

async fn mount_thread(server: &MockServer, root_id: &str, body: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/posts/{root_id}/thread")))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

async fn mount_channel_posts(server: &MockServer, body: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/channels/{CHANNEL_ID}/posts")))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

async fn mount_user(server: &MockServer, user_id: &str, username: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/users/{user_id}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"id": user_id, "username": username})),
        )
        .mount(server)
        .await;
}

async fn mount_user_failure(server: &MockServer, user_id: &str, template: ResponseTemplate) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/users/{user_id}")))
        .respond_with(template)
        .mount(server)
        .await;
}

async fn requests_to(server: &MockServer, expected_path: &str) -> usize {
    server
        .received_requests()
        .await
        .expect("wiremock received_requests")
        .iter()
        .filter(|req| req.url.path() == expected_path)
        .count()
}

// ----------------------------------------------------------------------
// Thread completeness
// ----------------------------------------------------------------------

/// A thread of a root plus two replies returns three messages,
/// root-first, even though not one of them names its author.
#[tokio::test]
async fn thread_returns_root_and_every_reply() {
    let server = MockServer::start().await;
    mount_thread(
        &server,
        ROOT_ID,
        posts_envelope(vec![
            wire_post("reply-2", "user-c", 1_700_000_002_000, ROOT_ID),
            wire_post(ROOT_ID, "user-a", 1_700_000_000_000, ""),
            wire_post("reply-1", "user-b", 1_700_000_001_000, ROOT_ID),
        ]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    mount_user(&server, "user-b", "bob").await;
    mount_user(&server, "user-c", "carol").await;
    let client = build_client(&server.uri());

    let messages = client
        .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
        .await
        .expect("read_thread_in_channel");

    assert_eq!(
        messages.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
        vec![ROOT_ID, "reply-1", "reply-2"],
        "root + N replies = N+1 messages, root first"
    );
    assert_eq!(
        messages
            .iter()
            .map(|m| m.username.as_str())
            .collect::<Vec<_>>(),
        vec!["alice", "bob", "carol"]
    );
}

/// The root leads the thread even when a reply shares its timestamp and
/// sorts ahead of it by id.
///
/// Ordering by time alone puts the root first only because it is
/// normally the oldest post. That is an accident of the data, not a
/// guarantee, and callers treat the first item as the root.
#[tokio::test]
async fn the_root_leads_the_thread_even_when_a_reply_ties_its_timestamp() {
    let server = MockServer::start().await;
    // "aaa-reply" sorts before the root by id and shares its timestamp,
    // so it wins on both keys a naive comparison would use.
    mount_thread(
        &server,
        ROOT_ID,
        posts_envelope(vec![
            wire_post("aaa-reply", "user-b", 1_700_000_000_000, ROOT_ID),
            wire_post(ROOT_ID, "user-a", 1_700_000_000_000, ""),
        ]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    mount_user(&server, "user-b", "bob").await;
    let client = build_client(&server.uri());

    let messages = client
        .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
        .await
        .expect("read_thread_in_channel");

    assert_eq!(
        messages.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
        vec![ROOT_ID, "aaa-reply"],
        "the root leads regardless of how its timestamp and id compare \
         against a reply's"
    );
}

/// A root with no replies is a thread of exactly one message.
#[tokio::test]
async fn thread_with_no_replies_returns_one_message() {
    let server = MockServer::start().await;
    mount_thread(
        &server,
        ROOT_ID,
        posts_envelope(vec![wire_post(ROOT_ID, "user-a", 1_700_000_000_000, "")]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    let client = build_client(&server.uri());

    let messages = client
        .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
        .await
        .expect("read_thread_in_channel");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, ROOT_ID);
    assert_eq!(
        messages[0].root_id, ROOT_ID,
        "a root is its own thread root"
    );
}

/// A thread body with no posts in it is a failure, not an empty
/// result: every thread contains at least its own root, so an empty
/// body means we did not get the thread.
#[tokio::test]
async fn empty_thread_body_is_an_error() {
    let server = MockServer::start().await;
    mount_thread(&server, ROOT_ID, json!({"order": [], "posts": {}})).await;
    let client = build_client(&server.uri());

    let err = client
        .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
        .await
        .expect_err("an empty thread body must not read as success");
    let rendered = err.to_string();
    assert!(
        rendered.contains(ROOT_ID),
        "diagnostic names the thread it failed on: {rendered}"
    );
}

/// A thread envelope whose root is in the requested channel but which
/// also carries a post from somewhere else is refused outright, and
/// nothing from it is returned — not even the in-channel root.
///
/// The caller's channel is a narrower scope than the bot's credential:
/// the bot can read many channels, the caller asked about one. Binding
/// only the anchor would leave the rest of the envelope unchecked,
/// which is exactly the shape a provider bug or a crafted root id could
/// exploit — a legitimate anchor, and replies from a channel the caller
/// never named. Partial trust in an envelope is not a usable middle
/// ground here, because the channel is dropped on the way into a
/// `Message` and no later layer can tell which posts were checked.
#[tokio::test]
async fn a_thread_carrying_a_post_from_another_channel_is_refused_whole() {
    let server = MockServer::start().await;
    let stray_channel = "channel-id-somewhere-else";
    mount_thread(
        &server,
        ROOT_ID,
        posts_envelope(vec![
            // The root is genuinely in the requested channel.
            wire_post(ROOT_ID, "user-a", 1_700_000_000_000, ""),
            // The reply is not.
            wire_post_in_channel(
                "stray-reply",
                stray_channel,
                "user-b",
                1_700_000_001_000,
                ROOT_ID,
            ),
        ]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    mount_user(&server, "user-b", "bob").await;
    let client = build_client(&server.uri());

    let error = client
        .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
        .await
        .expect_err("a mixed-channel thread envelope must not read as success");

    match &error {
        chanvoy_core::CoreError::AnchorChannelMismatch { post_id, channel } => {
            // The refusal names the id the CALLER asked about, never the
            // stray post's. That id was not theirs to learn: disclosing
            // it would confirm the existence and identity of a post
            // outside the channel they named.
            assert_eq!(
                post_id, ROOT_ID,
                "the refusal names the requested anchor, not the offending post"
            );
            assert_ne!(
                post_id, "stray-reply",
                "a provider-returned id must not reach the caller"
            );
            assert_eq!(channel, "the-channel");
        }
        other => panic!("expected AnchorChannelMismatch, got {other:?}"),
    }
    let rendered = error.to_string();
    assert!(
        !rendered.contains("stray-reply"),
        "the offending post's id must not appear anywhere in the refusal: {rendered}"
    );
    for body in ["body of root-post", "body of stray-reply"] {
        assert!(
            !rendered.contains(body),
            "no post body may survive the refusal, including the root's: {rendered}"
        );
    }
    assert!(
        !rendered.contains(stray_channel),
        "the refusal must not name the channel the stray post came from: {rendered}"
    );
}

/// An envelope entirely inside the requested channel, but with no post
/// carrying the requested id, is refused rather than returned.
///
/// Every post in it passes the channel bind, so the channel check alone
/// cannot see this. What comes back is a set of replies whose root was
/// not included — plausible thread-shaped data that is not the thread
/// that was asked for. Returned, it would be labelled with the
/// requested root anyway, and a caller selecting the latest post in
/// "the thread" would select a post from a conversation it never named.
#[tokio::test]
async fn a_thread_envelope_without_the_requested_root_is_refused() {
    let server = MockServer::start().await;
    // Both posts name ROOT_ID as their thread root, so the per-post
    // "belongs to this thread" check passes on each of them. Only the
    // requested root's own absence is wrong here.
    mount_thread(
        &server,
        ROOT_ID,
        posts_envelope(vec![
            wire_post("reply-1", "user-a", 1_700_000_001_000, ROOT_ID),
            wire_post("reply-2", "user-b", 1_700_000_002_000, ROOT_ID),
        ]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    mount_user(&server, "user-b", "bob").await;
    let client = build_client(&server.uri());

    let error = client
        .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
        .await
        .expect_err("an envelope missing the requested root must not read as success");

    match &error {
        chanvoy_core::CoreError::AnchorChannelMismatch { post_id, channel } => {
            assert_eq!(
                post_id, ROOT_ID,
                "the refusal names the root the caller asked for"
            );
            assert_eq!(channel, "the-channel");
        }
        other => panic!("expected AnchorChannelMismatch, got {other:?}"),
    }
    let rendered = error.to_string();
    for provider_id in ["reply-1", "reply-2"] {
        assert!(
            !rendered.contains(provider_id),
            "no provider-returned post id may reach the caller: {rendered}"
        );
    }
    assert!(
        !rendered.contains("body of"),
        "no post body may survive the refusal: {rendered}"
    );
}

/// An envelope in the right channel that carries a post rooted in a
/// different conversation is refused whole.
///
/// Same channel is not the same thread. The requested root is present
/// and genuinely top-level, so neither the channel bind nor the
/// root-shape check sees anything wrong — the stray post is only
/// detectable by the root it names.
#[tokio::test]
async fn a_thread_carrying_a_post_from_another_conversation_is_refused() {
    let server = MockServer::start().await;
    let other_root = "root-of-a-different-conversation";
    mount_thread(
        &server,
        ROOT_ID,
        posts_envelope(vec![
            wire_post(ROOT_ID, "user-a", 1_700_000_000_000, ""),
            // Same channel, correct-looking reply, wrong conversation.
            wire_post("foreign-reply", "user-b", 1_700_000_001_000, other_root),
        ]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    mount_user(&server, "user-b", "bob").await;
    let client = build_client(&server.uri());

    let error = client
        .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
        .await
        .expect_err("a post from another thread must not read as success");

    match &error {
        chanvoy_core::CoreError::AnchorChannelMismatch { post_id, channel } => {
            assert_eq!(
                post_id, ROOT_ID,
                "the refusal names the requested root, not the offending post"
            );
            assert_ne!(
                post_id, "foreign-reply",
                "a provider-returned id must not reach the caller"
            );
            assert_eq!(channel, "the-channel");
        }
        other => panic!("expected AnchorChannelMismatch, got {other:?}"),
    }
    let rendered = error.to_string();
    assert!(
        !rendered.contains("foreign-reply"),
        "the offending post's id must not appear in the refusal: {rendered}"
    );
    assert!(
        !rendered.contains(other_root),
        "the refusal must not name the conversation the stray post came from: {rendered}"
    );
    assert!(
        !rendered.contains("body of"),
        "no post body may survive the refusal: {rendered}"
    );
}

/// A requested "root" that is itself a reply is refused.
///
/// The provider answers a thread request made against a reply with the
/// whole thread, so the envelope is well-formed and in the right
/// channel. But the caller named a post that is not the thread's root,
/// and everything downstream treats the first element as the root —
/// hydrating this would hand back a thread whose root is not the id it
/// was requested under. The caller is told to ask against the canonical
/// root instead of being quietly given a differently-shaped answer.
#[tokio::test]
async fn a_requested_root_that_is_itself_a_reply_is_refused() {
    let server = MockServer::start().await;
    let elder_root = "the-post-this-one-replies-to";
    // Only one post, so the "every other post names this root" check
    // has nothing to look at: the sole thing wrong is that the
    // requested post is a reply.
    mount_thread(
        &server,
        ROOT_ID,
        posts_envelope(vec![wire_post(
            ROOT_ID,
            "user-a",
            1_700_000_001_000,
            elder_root,
        )]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    let client = build_client(&server.uri());

    let error = client
        .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
        .await
        .expect_err("a reply requested as a root must not read as success");

    match &error {
        chanvoy_core::CoreError::AnchorChannelMismatch { post_id, channel } => {
            assert_eq!(
                post_id, ROOT_ID,
                "the refusal names the id the caller supplied"
            );
            assert_ne!(
                post_id, elder_root,
                "the real root is provider-supplied and must not be echoed back"
            );
            assert_eq!(channel, "the-channel");
        }
        other => panic!("expected AnchorChannelMismatch, got {other:?}"),
    }
    let rendered = error.to_string();
    assert!(
        !rendered.contains(elder_root),
        "the refusal must not disclose the thread the post really belongs to: {rendered}"
    );
    assert!(
        !rendered.contains("body of"),
        "no post body may survive the refusal: {rendered}"
    );
}

/// The removed unbound thread read refuses without asking the provider
/// anything at all.
///
/// It is kept as an exported symbol so code built against the previous
/// release still compiles, with a deprecation warning instead of a hard
/// break. Refusing is the whole behaviour: forwarding to the bound read
/// would mean inventing a channel, which is the unscoped read it was
/// removed for. Asserting on the error alone would still pass if it
/// fetched the thread and then threw the result away, so the request
/// count is asserted too — that is the part that matters.
#[tokio::test]
#[allow(deprecated)]
async fn the_removed_unbound_thread_read_refuses_without_touching_the_provider() {
    let server = MockServer::start().await;
    // Mounted and answerable on purpose: the point is that it is never
    // called, not that calling it would fail.
    mount_thread(
        &server,
        ROOT_ID,
        posts_envelope(vec![wire_post(ROOT_ID, "user-a", 1_700_000_000_000, "")]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    let client = build_client(&server.uri());

    let error = client
        .read_thread(ROOT_ID)
        .await
        .expect_err("the removed read must not return posts");
    assert!(
        matches!(error, chanvoy_core::CoreError::UnboundThreadReadRemoved),
        "expected UnboundThreadReadRemoved, got {error:?}"
    );
    let rendered = error.to_string();
    assert!(
        rendered.contains("read_thread_in_channel"),
        "the refusal says what to call instead: {rendered}"
    );

    assert_eq!(
        server
            .received_requests()
            .await
            .expect("wiremock received_requests")
            .len(),
        0,
        "the removed read must issue no request of any kind"
    );
}

// ----------------------------------------------------------------------
// Author resolution
// ----------------------------------------------------------------------

/// Every failure mode of the author lookup falls back to the literal
/// user id, keeps the post, and never invents a placeholder name.
#[tokio::test]
async fn author_lookup_failures_fall_back_to_the_user_id() {
    for template in [
        ResponseTemplate::new(404).set_body_json(json!({"status_code": 404})),
        ResponseTemplate::new(403).set_body_json(json!({"status_code": 403})),
        ResponseTemplate::new(500).set_body_string("upstream exploded"),
        // A body that is not the shape we asked for — what a truncated
        // or proxied-error response looks like at the decode layer.
        ResponseTemplate::new(200).set_body_string("<html>not json</html>"),
    ] {
        let server = MockServer::start().await;
        mount_thread(
            &server,
            ROOT_ID,
            posts_envelope(vec![
                wire_post(ROOT_ID, "user-a", 1_700_000_000_000, ""),
                wire_post("reply-1", "user-b", 1_700_000_001_000, ROOT_ID),
            ]),
        )
        .await;
        mount_user_failure(&server, "user-a", template).await;
        mount_user(&server, "user-b", "bob").await;
        let client = build_client(&server.uri());

        let messages = client
            .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
            .await
            .expect("read_thread_in_channel");

        assert_eq!(messages.len(), 2, "a failed lookup never drops a post");
        assert_eq!(
            messages[0].username, "user-a",
            "unresolvable author is reported as the user id"
        );
        assert_eq!(
            messages[1].username, "bob",
            "the other author still resolves"
        );
        let rendered = serde_json::to_string(&messages).expect("serialize messages");
        assert!(
            !rendered.contains("unknown"),
            "no message may be attributed to \"unknown\": {rendered}"
        );
    }
}

/// A provider that cannot be reached at all still yields the user id
/// rather than an error or a placeholder.
#[tokio::test]
async fn unreachable_provider_falls_back_to_the_user_id() {
    let server = MockServer::start().await;
    let uri = server.uri();
    // Shut the server down so the next connection is refused — a real
    // transport failure rather than an HTTP error status.
    drop(server);
    let client = build_client(&uri);

    assert_eq!(client.author_username("user-a").await, "user-a");
}

/// A failed lookup must not be remembered: the next read tries again
/// and picks up the name once the provider recovers.
#[tokio::test]
async fn a_failed_lookup_is_not_cached() {
    let server = MockServer::start().await;
    mount_thread(
        &server,
        ROOT_ID,
        posts_envelope(vec![wire_post(ROOT_ID, "user-a", 1_700_000_000_000, "")]),
    )
    .await;
    mount_user_failure(&server, "user-a", ResponseTemplate::new(503)).await;
    let client = build_client(&server.uri());

    let first = client
        .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
        .await
        .expect("read_thread_in_channel");
    assert_eq!(
        first[0].username, "user-a",
        "fallback while the lookup fails"
    );

    // Provider recovers.
    server.reset().await;
    mount_thread(
        &server,
        ROOT_ID,
        posts_envelope(vec![wire_post(ROOT_ID, "user-a", 1_700_000_000_000, "")]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;

    let second = client
        .read_thread_in_channel(CHANNEL_ID, "the-channel", ROOT_ID)
        .await
        .expect("read_thread_in_channel");
    assert_eq!(
        second[0].username, "alice",
        "the failure must not have pinned the fallback for the cache window"
    );
}

// ----------------------------------------------------------------------
// Caching
// ----------------------------------------------------------------------

/// The same author across many posts, many calls, and a cloned client
/// costs exactly one lookup.
#[tokio::test]
async fn one_author_lookup_serves_every_post_and_every_clone() {
    let server = MockServer::start().await;
    mount_channel_posts(
        &server,
        posts_envelope(vec![
            wire_post("post-1", "user-a", 1_700_000_000_000, ""),
            wire_post("post-2", "user-a", 1_700_000_001_000, ""),
            wire_post("post-3", "user-a", 1_700_000_002_000, ""),
        ]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    let client = build_client(&server.uri());

    let first = client
        .read_channel_by_id_since_millis(CHANNEL_ID, 0)
        .await
        .expect("first read");
    assert_eq!(first.len(), 3);
    assert!(first.iter().all(|m| m.username == "alice"));

    // A second read on the same client, then a read on a clone of it.
    let _ = client
        .read_channel_by_id_since_millis(CHANNEL_ID, 0)
        .await
        .expect("second read");
    let cloned = client.clone();
    let from_clone = cloned
        .read_channel_by_id_since_millis(CHANNEL_ID, 0)
        .await
        .expect("read from clone");
    assert!(from_clone.iter().all(|m| m.username == "alice"));

    assert_eq!(
        requests_to(&server, "/api/v4/users/user-a").await,
        1,
        "three posts, three reads, and a clone must cost one author lookup"
    );
}

/// Distinct authors in one response are resolved once each, not once
/// per post.
#[tokio::test]
async fn each_distinct_author_is_resolved_once_per_response() {
    let server = MockServer::start().await;
    mount_channel_posts(
        &server,
        posts_envelope(vec![
            wire_post("post-1", "user-a", 1_700_000_000_000, ""),
            wire_post("post-2", "user-b", 1_700_000_001_000, ""),
            wire_post("post-3", "user-a", 1_700_000_002_000, ""),
            wire_post("post-4", "user-b", 1_700_000_003_000, ""),
        ]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    mount_user(&server, "user-b", "bob").await;
    let client = build_client(&server.uri());

    let messages = client
        .read_channel_by_id_since_millis(CHANNEL_ID, 0)
        .await
        .expect("read");
    assert_eq!(messages.len(), 4);
    assert_eq!(requests_to(&server, "/api/v4/users/user-a").await, 1);
    assert_eq!(requests_to(&server, "/api/v4/users/user-b").await, 1);
}

// ----------------------------------------------------------------------
// Thread-root normalization + ordering
// ----------------------------------------------------------------------

/// A post that arrives with an empty thread root is its own root; a
/// reply keeps the root it came with.
#[tokio::test]
async fn thread_root_is_normalized_on_the_way_out() {
    let server = MockServer::start().await;
    mount_channel_posts(
        &server,
        posts_envelope(vec![
            wire_post("post-top", "user-a", 1_700_000_000_000, ""),
            wire_post("post-reply", "user-a", 1_700_000_001_000, "some-other-root"),
        ]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    let client = build_client(&server.uri());

    let messages = client
        .read_channel_by_id_since_millis(CHANNEL_ID, 0)
        .await
        .expect("read");

    let by_id: BTreeMap<&str, &Message> = messages.iter().map(|m| (m.id.as_str(), m)).collect();
    assert_eq!(
        by_id["post-top"].root_id, "post-top",
        "a top-level post names itself as its thread root"
    );
    assert_eq!(
        by_id["post-reply"].root_id, "some-other-root",
        "a reply keeps the root the provider gave it"
    );
    assert!(
        messages.iter().all(|m| !m.root_id.is_empty()),
        "nothing chanvoy produces carries an empty thread root"
    );
}

/// Channel reads stay chronological, tie-broken by id so equal
/// timestamps do not shuffle between calls.
#[tokio::test]
async fn channel_reads_are_chronological_with_a_stable_tie_break() {
    let server = MockServer::start().await;
    // Ids are deliberately in the OPPOSITE order to timestamps. The
    // provider hands posts back in a map keyed by id, so an id-ordered
    // result is what you get for free by doing nothing — a fixture whose
    // id order already matches the expected output cannot tell the two
    // apart, and would pass with the sort deleted outright.
    mount_channel_posts(
        &server,
        posts_envelope(vec![
            wire_post("alpha", "user-a", 1_700_000_003_000, ""),
            wire_post("bravo", "user-a", 1_700_000_001_000, ""),
            wire_post("zeta", "user-a", 1_700_000_002_000, ""),
        ]),
    )
    .await;
    mount_user(&server, "user-a", "alice").await;
    let client = build_client(&server.uri());

    let messages = client
        .read_channel_by_id_since_millis(CHANNEL_ID, 0)
        .await
        .expect("read");
    assert_eq!(
        messages.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
        vec!["bravo", "zeta", "alpha"],
        "channel reads are ordered by time, not by the id order the \
         provider's map happens to yield"
    );
}

// ----------------------------------------------------------------------
// Mixed-version tolerance
// ----------------------------------------------------------------------

/// A freshly installed CLI has to be able to read what a still-running
/// older daemon sends, which has no thread root on it at all.
#[test]
fn a_message_without_a_thread_root_still_deserializes() {
    let legacy = r#"{
        "id": "post-1",
        "user_id": "user-a",
        "username": "alice",
        "message": "sent by an older daemon",
        "create_at": 1700000000000
    }"#;
    let message: Message = serde_json::from_str(legacy).expect("legacy message shape deserializes");
    assert_eq!(message.id, "post-1");
    assert_eq!(
        message.root_id, "",
        "an absent thread root reads as empty, meaning unknown"
    );
}
