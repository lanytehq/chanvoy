//! PER-032 Item J Tier-A — canonical endpoint manifest + fixture replay.
//!
//! AC #5, #6, #11 — every chanvoy verb that hits a Mattermost endpoint
//! must request the canonical URL recorded in
//! `tests/fixtures/mm-v4-shapes/endpoints.json`. This file is the
//! structural fix for the `pinned_posts` vs `pinned` class of URL-shape
//! drift (PR #25 / v0.2.1) — a regression that changes a call-site
//! path-template must fail Tier-A in CI rather than silently shipping.
//!
//! ## How the replay works
//!
//! For each manifest entry, we:
//! 1. Mount a wiremock that **exact-path-matches** the manifest's
//!    `path_template` after substituting stable placeholders. The mock
//!    body is the corresponding fixture file `<key>.json`.
//! 2. Drive a real chanvoy-core call site that issues this endpoint's
//!    request through a `MattermostClient` pointed at the mock server.
//! 3. Assert the call returns `Ok` — proves the URL matched the mock's
//!    exact-path matcher AND the response body parsed cleanly into the
//!    chanvoy-core type.
//! 4. Inspect `received_requests` for a definitive URL diff message
//!    when the assertion fails.
//!
//! Wiremock's default behavior for an unmatched path is 404. If
//! chanvoy-core asks for the wrong URL (e.g., the `pinned_posts`
//! regression), the mock doesn't match, the request 404s, the core
//! call errors, and the test fails with a path-mismatch diagnostic.
//!
//! ## Coverage
//!
//! `manifest_coverage_complete` enforces that every endpoint declared
//! in `endpoints.json` has at least one replay test covering it.
//! Future MM-endpoint additions land a manifest entry first (mirrors
//! the schemas-before-code platform invariant) and the coverage test
//! fails until the corresponding replay lands.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use chanvoy_core::{CapabilityClass, CredentialMode, MattermostClient, Profile, Provider};
use serde_json::Value;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const FIXTURES_DIR: &str = "tests/fixtures/mm-v4-shapes";
const API_ROOT: &str = "/api/v4";

// ----------------------------------------------------------------------
// Stable placeholder values used to expand manifest path templates.
// ----------------------------------------------------------------------

const TEAM_SLUG: &str = "team-slug-stable";
const TEAM_ID: &str = "team-id-stable";
const CHANNEL_ID: &str = "channel-id-stable";
const CHANNEL_NAME: &str = "channel-name-stable";
const USER_ID: &str = "user-id-stable";
const USERNAME: &str = "username-stable";
const POST_ID: &str = "post-id-stable";
const ROOT_POST_ID: &str = "root-post-id-stable";
const EMOJI: &str = "thumbsup";
const BOT_ID: &str = "user-id-stable";
const BOT_USERNAME: &str = "bot-stable";

// ----------------------------------------------------------------------
// Manifest loading + path-template expansion.
// ----------------------------------------------------------------------

/// Manifest contents needed by the coverage + existence tests. Parsed
/// via `serde_json::Value` to avoid pulling `serde` into root
/// dev-dependencies — the manifest is reviewed manually anyway, so a
/// lightweight typed view here is sufficient.
struct Manifest {
    endpoints: BTreeMap<String, EndpointSpec>,
}

struct EndpointSpec {
    method: String,
    path_template: String,
}

fn load_manifest() -> Manifest {
    let path = format!("{FIXTURES_DIR}/endpoints.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("read manifest {path}: {err}"));
    let v: Value =
        serde_json::from_str(&raw).unwrap_or_else(|err| panic!("parse manifest {path}: {err}"));
    let endpoints = v["endpoints"]
        .as_object()
        .unwrap_or_else(|| panic!("manifest missing endpoints object"))
        .iter()
        .map(|(k, spec)| {
            let method = spec["method"]
                .as_str()
                .unwrap_or_else(|| panic!("endpoint {k} missing method"))
                .to_string();
            let path_template = spec["path_template"]
                .as_str()
                .unwrap_or_else(|| panic!("endpoint {k} missing path_template"))
                .to_string();
            (
                k.clone(),
                EndpointSpec {
                    method,
                    path_template,
                },
            )
        })
        .collect();
    Manifest { endpoints }
}

fn load_fixture(key: &str) -> Value {
    let path = format!("{FIXTURES_DIR}/{key}.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("read fixture {path}: {err}"));
    serde_json::from_str(&raw).unwrap_or_else(|err| panic!("parse fixture {path}: {err}"))
}

/// Substitute the stable placeholder values into a manifest path
/// template. Unknown placeholders are deliberately left in `{...}`
/// form so an unsubstituted template surfaces as a clear test failure
/// rather than a silent URL mismatch.
fn expand_template(template: &str) -> String {
    template
        .replace("{slug}", TEAM_SLUG)
        .replace("{team_id}", TEAM_ID)
        .replace("{channel_id}", CHANNEL_ID)
        .replace("{channel_name}", CHANNEL_NAME)
        .replace("{user_id}", USER_ID)
        .replace("{post_id}", POST_ID)
        .replace("{root_post_id}", ROOT_POST_ID)
        .replace("{username}", USERNAME)
        .replace("{emoji}", EMOJI)
}

/// Construct a `MattermostClient` pointing at the given mock URL.
fn build_client(mock_url: &str) -> MattermostClient {
    let profile = Profile {
        name: "url-shape-replay".to_string(),
        role: "bravo-devlead".to_string(),
        scope: "lanytehq".to_string(),
        provider: Provider::Mattermost,
        bot_username: BOT_USERNAME.to_string(),
        team_name: TEAM_SLUG.to_string(),
        server_url: mock_url.to_string(),
        env_name: "LANYTE_MM_TOKEN".to_string(),
        env_file: None,
        credential_mode: CredentialMode::EnvName,
        capability_class: CapabilityClass::Standard,
        monitored_channels: vec![],
        ipc: None,
    };
    MattermostClient::new(&profile, "fixture-token".to_string()).expect("build MattermostClient")
}

/// Mount the resolver-baseline endpoints (whoami, list_teams,
/// team_by_name, channel_by_name) every verb that resolves a channel
/// by name depends on. Each mount serves the corresponding fixture.
async fn mount_resolver_baseline(server: &MockServer) {
    mount_fixture(server, "GET", "/users/me", "whoami").await;
    mount_fixture(server, "GET", "/users/me/teams", "list_teams").await;
    mount_fixture(
        server,
        "GET",
        &format!("/teams/name/{TEAM_SLUG}"),
        "team_by_name",
    )
    .await;
    mount_fixture(
        server,
        "GET",
        &format!("/teams/{TEAM_ID}/channels/name/{CHANNEL_NAME}"),
        "channel_by_name",
    )
    .await;
}

/// Mount a wiremock that exact-path-matches `expanded_path` with the
/// given method and serves the fixture body for `fixture_key`.
async fn mount_fixture(
    server: &MockServer,
    http_method: &str,
    expanded_path: &str,
    fixture_key: &str,
) {
    let full_path = format!("{API_ROOT}{expanded_path}");
    let body = load_fixture(fixture_key);
    Mock::given(method(http_method))
        .and(path(full_path))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

/// Assert at least one received request matches `(http_method, expected_path)`
/// — the URL-shape contract assertion. Produces a clear diff message
/// listing actual URLs received if the assertion fails.
async fn assert_request_to(
    server: &MockServer,
    http_method: &str,
    expected_path: &str,
    endpoint_key: &str,
) {
    let full_path = format!("{API_ROOT}{expected_path}");
    let requests = server
        .received_requests()
        .await
        .expect("wiremock received_requests");
    let matched = requests.iter().any(|req| {
        req.method.as_str().eq_ignore_ascii_case(http_method) && req.url.path() == full_path
    });
    assert!(
        matched,
        "URL-shape contract violation for endpoint {endpoint_key:?}: \
         expected {http_method} {full_path}; \
         received: {actual:?}",
        actual = requests
            .iter()
            .map(|r| format!("{} {}", r.method, r.url.path()))
            .collect::<Vec<_>>()
    );
}

// ----------------------------------------------------------------------
// Per-endpoint replay tests. Each test mounts only the endpoints its
// call site touches and asserts the URL contract.
// ----------------------------------------------------------------------

/// AC #5/#6/#11 — `whoami` hits `GET /users/me`.
#[tokio::test]
async fn replay_whoami() {
    let server = MockServer::start().await;
    mount_fixture(&server, "GET", "/users/me", "whoami").await;
    let client = build_client(&server.uri());

    let identity = client.whoami().await.expect("whoami parses");
    assert_eq!(identity.id, USER_ID, "fixture id round-trips");

    assert_request_to(&server, "GET", "/users/me", "whoami").await;
}

/// `list_my_teams` hits `GET /users/me/teams` (covers the
/// `list_teams` manifest entry).
#[tokio::test]
async fn replay_list_teams() {
    let server = MockServer::start().await;
    mount_fixture(&server, "GET", "/users/me/teams", "list_teams").await;
    let client = build_client(&server.uri());

    let teams = client.list_my_teams().await.expect("list_my_teams parses");
    assert!(!teams.is_empty(), "fixture has at least one team");

    assert_request_to(&server, "GET", "/users/me/teams", "list_teams").await;
}

/// `list_dms` hits `GET /users/{user_id}/channels` (covers
/// `list_user_channels`). The manifest's `list_user_channels` entry
/// corresponds to the `dms` verb's MM endpoint, not the per-team
/// channel enumeration.
#[tokio::test]
async fn replay_list_user_channels() {
    let server = MockServer::start().await;
    mount_fixture(&server, "GET", "/users/me", "whoami").await;
    mount_fixture(
        &server,
        "GET",
        &format!("/users/{USER_ID}/channels"),
        "list_user_channels",
    )
    .await;
    let client = build_client(&server.uri());

    let _ = client.list_dms().await.expect("list_dms parses");

    assert_request_to(
        &server,
        "GET",
        &format!("/users/{USER_ID}/channels"),
        "list_user_channels",
    )
    .await;
}

/// `list_channels` hits
/// `GET /users/me/teams/{team_id}/channels` (covers
/// `list_team_channels`). Resolution chain: `team_id()` →
/// `team_by_name` → list call.
#[tokio::test]
async fn replay_list_team_channels() {
    let server = MockServer::start().await;
    mount_fixture(
        &server,
        "GET",
        &format!("/teams/name/{TEAM_SLUG}"),
        "team_by_name",
    )
    .await;
    mount_fixture(
        &server,
        "GET",
        &format!("/users/me/teams/{TEAM_ID}/channels"),
        "list_team_channels",
    )
    .await;
    let client = build_client(&server.uri());

    let _ = client.list_channels().await.expect("list_channels parses");

    assert_request_to(
        &server,
        "GET",
        &format!("/users/me/teams/{TEAM_ID}/channels"),
        "list_team_channels",
    )
    .await;
}

/// `read_channel` covers four manifest entries through the γ hybrid
/// resolver chain: `list_teams` → `team_by_name` →
/// `channel_by_name` → `channel_posts`. This is the highest-coverage
/// replay since every channel-scoped verb traverses the same
/// resolver path.
#[tokio::test]
async fn replay_channel_posts_and_resolver_chain() {
    let server = MockServer::start().await;
    mount_resolver_baseline(&server).await;
    mount_fixture(
        &server,
        "GET",
        &format!("/channels/{CHANNEL_ID}/posts"),
        "channel_posts",
    )
    .await;
    let client = build_client(&server.uri());

    let _ = client
        .read_channel(CHANNEL_NAME, 60, None)
        .await
        .expect("read_channel parses");

    // Verify the URL contract for every endpoint the resolver chain hit.
    // Note: the resolver short-circuits `/users/me/teams` when the
    // requested team matches the profile's primary — `list_teams`
    // coverage lives in the dedicated `replay_list_teams` test.
    assert_request_to(
        &server,
        "GET",
        &format!("/teams/name/{TEAM_SLUG}"),
        "team_by_name",
    )
    .await;
    assert_request_to(
        &server,
        "GET",
        &format!("/teams/{TEAM_ID}/channels/name/{CHANNEL_NAME}"),
        "channel_by_name",
    )
    .await;
    assert_request_to(
        &server,
        "GET",
        &format!("/channels/{CHANNEL_ID}/posts"),
        "channel_posts",
    )
    .await;
}

/// AC #11 load-bearing case — `read_channel_pinned` must hit
/// `/channels/{id}/pinned`, NOT `/pinned_posts`. The original v0.2.1
/// regression class is exactly this URL-shape drift; a future
/// regression to `_posts` will fail this test before reaching live
/// MM.
#[tokio::test]
async fn replay_channel_pinned() {
    let server = MockServer::start().await;
    mount_resolver_baseline(&server).await;
    mount_fixture(
        &server,
        "GET",
        &format!("/channels/{CHANNEL_ID}/pinned"),
        "channel_pinned",
    )
    .await;
    let client = build_client(&server.uri());

    let _ = client
        .read_channel_pinned(CHANNEL_NAME, None)
        .await
        .expect("read_channel_pinned parses");

    assert_request_to(
        &server,
        "GET",
        &format!("/channels/{CHANNEL_ID}/pinned"),
        "channel_pinned",
    )
    .await;
}

/// `read_thread` hits `GET /posts/{root_post_id}/thread` (covers
/// `post_thread`).
#[tokio::test]
async fn replay_post_thread() {
    let server = MockServer::start().await;
    mount_fixture(
        &server,
        "GET",
        &format!("/posts/{ROOT_POST_ID}/thread"),
        "post_thread",
    )
    .await;
    let client = build_client(&server.uri());

    let _ = client
        .read_thread(ROOT_POST_ID)
        .await
        .expect("read_thread parses");

    assert_request_to(
        &server,
        "GET",
        &format!("/posts/{ROOT_POST_ID}/thread"),
        "post_thread",
    )
    .await;
}

/// `post_message` hits `POST /posts` (covers `create_post`). Also
/// exercises the resolver baseline.
#[tokio::test]
async fn replay_create_post() {
    let server = MockServer::start().await;
    mount_resolver_baseline(&server).await;
    mount_fixture(&server, "POST", "/posts", "create_post").await;
    let client = build_client(&server.uri());

    let _ = client
        .post_message(CHANNEL_NAME, "fixture-msg", None)
        .await
        .expect("post_message parses");

    assert_request_to(&server, "POST", "/posts", "create_post").await;
}

/// `create_channel` hits `POST /channels`. Resolver chain hits
/// `team_by_name` first to look up the primary team.
#[tokio::test]
async fn replay_create_channel() {
    let server = MockServer::start().await;
    mount_fixture(
        &server,
        "GET",
        &format!("/teams/name/{TEAM_SLUG}"),
        "team_by_name",
    )
    .await;
    mount_fixture(&server, "POST", "/channels", "create_channel").await;
    let client = build_client(&server.uri());

    let _ = client
        .create_channel("new-channel-name", "New Channel", None, None)
        .await
        .expect("create_channel parses");

    assert_request_to(&server, "POST", "/channels", "create_channel").await;
}

/// `archive_channel` hits `DELETE /channels/{channel_id}` (covers
/// `delete_channel`).
#[tokio::test]
async fn replay_delete_channel() {
    let server = MockServer::start().await;
    mount_resolver_baseline(&server).await;
    mount_fixture(
        &server,
        "DELETE",
        &format!("/channels/{CHANNEL_ID}"),
        "delete_channel",
    )
    .await;
    let client = build_client(&server.uri());

    client
        .archive_channel(CHANNEL_NAME)
        .await
        .expect("archive_channel succeeds");

    assert_request_to(
        &server,
        "DELETE",
        &format!("/channels/{CHANNEL_ID}"),
        "delete_channel",
    )
    .await;
}

/// `restore_channel` hits `POST /channels/{channel_id}/restore`. The
/// underlying admin verb is dropped from Tier-B live smoke (elevated
/// capability — see manifest `tier_b_dropped_rationale`) but the URL
/// contract is guarded here.
#[tokio::test]
async fn replay_restore_channel() {
    let server = MockServer::start().await;
    mount_resolver_baseline(&server).await;
    mount_fixture(
        &server,
        "POST",
        &format!("/channels/{CHANNEL_ID}/restore"),
        "restore_channel",
    )
    .await;
    let client = build_client(&server.uri());

    client
        .restore_channel(CHANNEL_NAME)
        .await
        .expect("restore_channel succeeds");

    assert_request_to(
        &server,
        "POST",
        &format!("/channels/{CHANNEL_ID}/restore"),
        "restore_channel",
    )
    .await;
}

/// `add_member` hits `POST /channels/{channel_id}/members` (covers
/// `add_channel_member`) and `GET /users/username/{username}` (covers
/// `user_by_username`). Dropped from Tier-B live smoke (peer-
/// principal dependent); URL contract guarded here.
#[tokio::test]
async fn replay_add_channel_member_and_user_by_username() {
    let server = MockServer::start().await;
    mount_resolver_baseline(&server).await;
    mount_fixture(
        &server,
        "GET",
        &format!("/users/username/{USERNAME}"),
        "user_by_username",
    )
    .await;
    mount_fixture(
        &server,
        "POST",
        &format!("/channels/{CHANNEL_ID}/members"),
        "add_channel_member",
    )
    .await;
    let client = build_client(&server.uri());

    client
        .add_member(CHANNEL_NAME, USERNAME)
        .await
        .expect("add_member succeeds");

    assert_request_to(
        &server,
        "GET",
        &format!("/users/username/{USERNAME}"),
        "user_by_username",
    )
    .await;
    assert_request_to(
        &server,
        "POST",
        &format!("/channels/{CHANNEL_ID}/members"),
        "add_channel_member",
    )
    .await;
}

/// `add_reaction` hits `POST /reactions` (covers `create_reaction`)
/// and `GET /posts/{post_id}` (covers `post_by_id`).
#[tokio::test]
async fn replay_create_reaction_and_post_by_id() {
    let server = MockServer::start().await;
    mount_resolver_baseline(&server).await;
    mount_fixture(&server, "GET", &format!("/posts/{POST_ID}"), "post_by_id").await;
    mount_fixture(&server, "POST", "/reactions", "create_reaction").await;
    let client = build_client(&server.uri());

    client
        .add_reaction(CHANNEL_NAME, POST_ID, EMOJI, None)
        .await
        .expect("add_reaction succeeds");

    assert_request_to(&server, "POST", "/reactions", "create_reaction").await;
    assert_request_to(&server, "GET", &format!("/posts/{POST_ID}"), "post_by_id").await;
}

/// `remove_reaction` hits
/// `DELETE /users/{user_id}/posts/{post_id}/reactions/{emoji}`
/// (covers `delete_reaction`).
#[tokio::test]
async fn replay_delete_reaction() {
    let server = MockServer::start().await;
    mount_resolver_baseline(&server).await;
    mount_fixture(&server, "GET", &format!("/posts/{POST_ID}"), "post_by_id").await;
    let delete_path = format!("/users/{USER_ID}/posts/{POST_ID}/reactions/{EMOJI}");
    mount_fixture(&server, "DELETE", &delete_path, "delete_reaction").await;
    let client = build_client(&server.uri());

    client
        .remove_reaction(CHANNEL_NAME, POST_ID, EMOJI, None)
        .await
        .expect("remove_reaction succeeds");

    assert_request_to(&server, "DELETE", &delete_path, "delete_reaction").await;
}

/// `direct_message` hits `POST /channels/direct` (covers
/// `create_direct_channel`) plus the user-by-username lookup. The
/// underlying `dm send` verb is dropped from Tier-B live smoke
/// (peer-principal dependent — see manifest
/// `tier_b_dropped_rationale`); URL contract guarded here.
#[tokio::test]
async fn replay_create_direct_channel() {
    let server = MockServer::start().await;
    mount_fixture(&server, "GET", "/users/me", "whoami").await;
    mount_fixture(
        &server,
        "GET",
        &format!("/users/username/{USERNAME}"),
        "user_by_username",
    )
    .await;
    mount_fixture(&server, "POST", "/channels/direct", "create_direct_channel").await;
    mount_fixture(&server, "POST", "/posts", "create_post").await;
    let client = build_client(&server.uri());

    let _ = client
        .direct_message(USERNAME, "fixture-dm")
        .await
        .expect("direct_message succeeds");

    assert_request_to(&server, "POST", "/channels/direct", "create_direct_channel").await;
}

/// `search_channel` hits `POST /teams/{team_id}/posts/search` (covers
/// `search_posts`).
#[tokio::test]
async fn replay_search_posts() {
    let server = MockServer::start().await;
    mount_resolver_baseline(&server).await;
    let search_path = format!("/teams/{TEAM_ID}/posts/search");
    mount_fixture(&server, "POST", &search_path, "search_posts").await;
    let client = build_client(&server.uri());

    let _ = client
        .search_channel(CHANNEL_NAME, "fixture-query", 5, None, None, None)
        .await
        .expect("search_channel parses");

    assert_request_to(&server, "POST", &search_path, "search_posts").await;
}

// ----------------------------------------------------------------------
// Coverage assertion: every manifest entry must be covered by at
// least one replay test in this file.
// ----------------------------------------------------------------------

/// Hand-maintained map of manifest endpoint key → the names of replay
/// test functions in this file that exercise that endpoint. The
/// `manifest_coverage_complete` test fails if any manifest entry is
/// missing from this map, forcing new endpoint additions to land both
/// a fixture and a replay.
fn coverage_map() -> BTreeMap<&'static str, Vec<&'static str>> {
    BTreeMap::from([
        ("whoami", vec!["replay_whoami"]),
        ("list_teams", vec!["replay_list_teams"]),
        (
            "team_by_name",
            vec!["replay_channel_posts_and_resolver_chain"],
        ),
        ("list_team_channels", vec!["replay_list_team_channels"]),
        ("list_user_channels", vec!["replay_list_user_channels"]),
        (
            "channel_by_name",
            vec!["replay_channel_posts_and_resolver_chain"],
        ),
        (
            "channel_posts",
            vec!["replay_channel_posts_and_resolver_chain"],
        ),
        ("channel_pinned", vec!["replay_channel_pinned"]),
        (
            "post_by_id",
            vec![
                "replay_create_reaction_and_post_by_id",
                "replay_delete_reaction",
            ],
        ),
        ("post_thread", vec!["replay_post_thread"]),
        (
            "user_by_username",
            vec![
                "replay_add_channel_member_and_user_by_username",
                "replay_create_direct_channel",
            ],
        ),
        (
            "create_post",
            vec!["replay_create_post", "replay_create_direct_channel"],
        ),
        ("create_channel", vec!["replay_create_channel"]),
        (
            "create_direct_channel",
            vec!["replay_create_direct_channel"],
        ),
        ("restore_channel", vec!["replay_restore_channel"]),
        (
            "add_channel_member",
            vec!["replay_add_channel_member_and_user_by_username"],
        ),
        (
            "create_reaction",
            vec!["replay_create_reaction_and_post_by_id"],
        ),
        ("delete_reaction", vec!["replay_delete_reaction"]),
        ("delete_channel", vec!["replay_delete_channel"]),
        ("search_posts", vec!["replay_search_posts"]),
    ])
}

/// Endpoints whose call sites are reachable only through private
/// chanvoy-core paths (e.g., WebSocket-handler internals) and cannot
/// be driven from an integration test without exposing additional
/// surface area. The coverage gate treats these as covered for v0.2.2
/// with the rationale recorded here; the URL template is reviewed
/// manually against the cited call site.
///
/// Future brief candidate: expose a test-only hook to drive these
/// internal handlers through a recorded mock so the URL contract gets
/// CI guard parity with the rest of the surface.
fn documented_gaps() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([(
        "user_by_id",
        "Hit only via the private `WsHandler::resolve_username` helper \
             (crates/chanvoy-core/src/lib.rs around line 4453) when the WebSocket \
             pipeline annotates a notification with the sender's username. The \
             call uses `request_raw` and soft-fails to `\"unknown\"` on error, \
             so a URL regression would silently degrade UX rather than fail \
             loudly — review the call site manually on any change to \
             notification handling. v0.2.3+ candidate: expose a test hook so \
             this contract gets CI parity with the rest of the manifest.",
    )])
}

/// AC #11 coverage gate. Fails if any manifest entry has no replay
/// test mapped to it, or has an empty mapping. Documented gaps
/// (private-API endpoints not reachable from integration tests) are
/// accepted with their rationale recorded in `documented_gaps()`.
#[test]
fn manifest_coverage_complete() {
    let manifest = load_manifest();
    let manifest_keys: BTreeSet<&str> = manifest.endpoints.keys().map(String::as_str).collect();
    let coverage = coverage_map();
    let gaps = documented_gaps();
    let coverage_keys: BTreeSet<&str> = coverage.keys().copied().collect();
    let gap_keys: BTreeSet<&str> = gaps.keys().copied().collect();
    let known_keys: BTreeSet<&str> = coverage_keys.union(&gap_keys).copied().collect();

    let missing: Vec<&&str> = manifest_keys.difference(&known_keys).collect();
    assert!(
        missing.is_empty(),
        "manifest entries with no replay coverage and no documented gap: {missing:?}. \
         Each new MM-endpoint usage must land either a replay test or a \
         documented_gaps entry before merge (PER-032 AC #11 / \
         schemas-before-code invariant)."
    );

    let extra: Vec<&&str> = known_keys.difference(&manifest_keys).collect();
    assert!(
        extra.is_empty(),
        "coverage/gap map references endpoints not in manifest: {extra:?}. \
         Either add the missing manifest entry or remove the stale mapping."
    );

    let placeholders: Vec<&&str> = coverage
        .iter()
        .filter(|(_, tests)| tests.is_empty())
        .map(|(key, _)| key)
        .collect();
    assert!(
        placeholders.is_empty(),
        "manifest entries with empty replay coverage (placeholder gaps): {placeholders:?}. \
         Either land a replay test or move to documented_gaps with rationale."
    );

    // Sanity: every documented-gap key has non-empty rationale text.
    for (key, rationale) in &gaps {
        assert!(
            !rationale.trim().is_empty(),
            "documented_gap {key:?} has empty rationale — gap entries must justify why no replay exists"
        );
    }
}

/// Every fixture file declared in the manifest must exist on disk and
/// be valid JSON. Catches typos and orphan fixtures.
#[test]
fn fixture_files_exist_and_parse() {
    let manifest = load_manifest();
    for key in manifest.endpoints.keys() {
        let path = format!("{FIXTURES_DIR}/{key}.json");
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("fixture {path} missing or unreadable: {err}"));
        let _: Value = serde_json::from_str(&raw)
            .unwrap_or_else(|err| panic!("fixture {path} is not valid JSON: {err}"));
    }
}
