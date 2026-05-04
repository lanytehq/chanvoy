//! PER-023: session-start ergonomics integration tests.
//!
//! Covers the four primitives shipped in PER-023, end-to-end through the
//! real daemon binary against a wiremock Mattermost server:
//!
//! 1. `chanvoy pinned <channel>` — fetch pinned posts (pure read)
//! 2. `chanvoy read --since-bootstrap` + general `--limit` (with bare-limit
//!    rejection and bootstrap default N=50)
//! 3. Time-unit suffixes (`30s`/`5m`/`4h`/`2d`) on `read --since`,
//!    `notifications --since`, `wait --timeout`, plus M/mo loud-failure
//! 4. `chanvoy read --advance` and `chanvoy ack <channel>` — cursor
//!    advancement plumbing
//!
//! Test contract pins (per productbook PR #47, settled 2026-05-03):
//! - Bare `read --limit N` is rejected with a diagnostic naming the
//!   ambiguity; explicit read-mode flag required.
//! - `read --advance` cursor target = latest post **returned**,
//!   mode-independent; no-op when zero posts returned.
//! - `ack <channel>` advances cursor to the channel's current latest post
//!   without surfacing content; no-op success when channel has no posts.
//! - Uppercase `M` and `mo` rejected with diagnostic; lowercase `m`
//!   parses as minutes.
//!
//! Shared harness primitives live in `tests/common/mod.rs`.

#![allow(dead_code)]

mod common;

use common::{read_attention_state, run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

/// Bare `read --limit N` (no read-mode flag) must be rejected with a
/// diagnostic suggesting `--since-bootstrap --limit N`. CLI-level
/// rejection — never reaches the daemon.
#[tokio::test]
async fn read_bare_limit_without_read_mode_rejected() {
    let env = TestEnv::new("per-023-bare-limit").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(&env, &["read", "some-channel", "--limit", "20"]).await;
    assert!(
        !out.status.success(),
        "bare `read --limit N` must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--since-bootstrap")
            || stderr.contains("read-mode flag")
            || stderr.contains("rejected"),
        "diagnostic should suggest --since-bootstrap or name the rejection; got: {stderr}"
    );
}

/// `read --since 30M` must reject — uppercase M is ambiguous with months.
#[tokio::test]
async fn read_since_uppercase_m_rejected() {
    let env = TestEnv::new("per-023-uppercase-m").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(&env, &["read", "some-channel", "--since", "30M"]).await;
    assert!(!out.status.success(), "30M must reject");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("uppercase 'M'") || stderr.contains("month"),
        "diagnostic should name the M/months ambiguity; got: {stderr}"
    );
}

/// `read --since 5mo` must reject — months suffix is unsupported.
#[tokio::test]
async fn read_since_mo_suffix_rejected() {
    let env = TestEnv::new("per-023-mo-suffix").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(&env, &["read", "some-channel", "--since", "5mo"]).await;
    assert!(!out.status.success(), "5mo must reject");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("'mo'") || stderr.contains("month"),
        "diagnostic should name the months suffix; got: {stderr}"
    );
}

/// `wait --timeout 30M` must reject (suffix-parsing fires on every
/// affected flag, not just `read --since`).
#[tokio::test]
async fn wait_timeout_uppercase_m_rejected() {
    let env = TestEnv::new("per-023-wait-uppercase-m").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(&env, &["wait", "some-channel", "--timeout", "30M"]).await;
    assert!(!out.status.success(), "wait --timeout 30M must reject");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("uppercase 'M'") || stderr.contains("month"),
        "diagnostic should name the ambiguity; got: {stderr}"
    );
}

/// `chanvoy pinned <channel>` happy path: returns the channel's pinned
/// posts; no cursor side effects.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn pinned_returns_pinned_posts() {
    let env = TestEnv::new("per-023-pinned-happy").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-p1", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-p1").await;

    // Pinned-posts endpoint mock.
    Mock::given(method("GET"))
        .and(path("/api/v4/channels/chan-id-p1/pinned_posts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "posts": {
                "post-1": {
                    "id": "post-1",
                    "user_id": "user-1",
                    "username": "alice",
                    "message": "pinned context #1",
                    "create_at": 1_700_000_000_000i64,
                },
                "post-2": {
                    "id": "post-2",
                    "user_id": "user-2",
                    "username": "bob",
                    "message": "pinned context #2",
                    "create_at": 1_700_000_001_000i64,
                },
            }
        })))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let pre_state = read_attention_state(&env);

    let out = run_chanvoy(&env, &["--json", "pinned", "bravo-team"]).await;
    assert!(
        out.status.success(),
        "pinned must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).expect("pinned --json parses as Vec<Message>");
    assert_eq!(parsed.len(), 2, "two pinned posts mocked");
    let messages: Vec<&str> = parsed
        .iter()
        .filter_map(|m| m["message"].as_str())
        .collect();
    assert!(messages.contains(&"pinned context #1"));
    assert!(messages.contains(&"pinned context #2"));

    // Pure read invariant: pinned MUST NOT mutate attention state.
    let post_state = read_attention_state(&env);
    assert_eq!(
        pre_state, post_state,
        "pinned must not mutate attention state"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy pinned <channel>` on a channel with no pins returns an
/// empty list (exit 0; not an error).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn pinned_empty_channel_returns_empty_list() {
    let env = TestEnv::new("per-023-pinned-empty").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-p2", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-p2").await;

    Mock::given(method("GET"))
        .and(path("/api/v4/channels/chan-id-p2/pinned_posts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"posts": {}})))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "pinned", "bravo-team"]).await;
    assert!(out.status.success(), "empty pinned set is not an error");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).expect("pinned --json empty parses");
    assert_eq!(parsed.len(), 0);

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy read --advance` advances the attention cursor to the latest
/// post returned by the read.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_advance_records_cursor_to_latest_returned() {
    let env = TestEnv::new("per-023-advance-records").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-a1", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-a1").await;
    env.mock_channel_posts(
        "chan-id-a1",
        &[
            ("post-a1", "user-1", "alice", "hello", 1_700_000_000_000),
            ("post-a2", "user-2", "bob", "world", 1_700_000_001_000),
        ],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let pre_state = read_attention_state(&env);
    assert!(
        pre_state
            .as_ref()
            .map(|s| s.channels.is_empty())
            .unwrap_or(true),
        "no cursor before --advance"
    );

    let out = run_chanvoy(
        &env,
        &["--json", "read", "bravo-team", "--since", "5m", "--advance"],
    )
    .await;
    assert!(
        out.status.success(),
        "read --advance must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let post_state = read_attention_state(&env).expect("attention state written by --advance");
    let cursor = post_state
        .channels
        .values()
        .next()
        .expect("one cursor recorded");
    assert_eq!(
        cursor.last_seen_post_id.as_deref(),
        Some("post-a2"),
        "cursor advances to latest post returned"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy read --advance` is a no-op on the cursor when zero posts are
/// returned. Per AC #4: "No-op when no posts are returned — cursor
/// unchanged."
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_advance_noop_when_zero_posts_returned() {
    let env = TestEnv::new("per-023-advance-noop").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-a2", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-a2").await;
    // Empty post window — channel has no recent posts.
    env.mock_channel_posts("chan-id-a2", &[]).await;

    let daemon = spawn_daemon(&env).await;
    let pre_state = read_attention_state(&env);

    let out = run_chanvoy(
        &env,
        &["--json", "read", "bravo-team", "--since", "5m", "--advance"],
    )
    .await;
    assert!(
        out.status.success(),
        "empty read with --advance is not an error"
    );

    let post_state = read_attention_state(&env);
    // No cursor written by an empty read+advance — cursor unchanged
    // (none → none, or whatever pre-existing value).
    assert_eq!(
        pre_state, post_state,
        "no-op cursor when zero posts returned"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy ack <channel>` on a non-empty channel advances the cursor
/// to the channel's current latest post id without surfacing content.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn ack_records_cursor_to_channel_latest() {
    let env = TestEnv::new("per-023-ack-happy").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-k1", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-k1").await;
    // ack uses read_channel_most_recent under the hood (per_page=1).
    // The wiremock path matcher matches on path only (query params
    // ignored), so this single mock satisfies both the lookup and the
    // recent-fetch.
    env.mock_channel_posts(
        "chan-id-k1",
        &[(
            "post-k1-latest",
            "user-1",
            "alice",
            "latest",
            1_700_000_002_000,
        )],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "ack", "bravo-team"]).await;
    assert!(
        out.status.success(),
        "ack must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).expect("ack --json parses");
    assert_eq!(
        result["cursor_post_id"].as_str(),
        Some("post-k1-latest"),
        "ack returns the post id it advanced the cursor to"
    );
    assert_eq!(result["channel"].as_str(), Some("bravo-team"));

    let post_state = read_attention_state(&env).expect("ack writes attention state");
    let cursor = post_state
        .channels
        .values()
        .next()
        .expect("one cursor recorded");
    assert_eq!(cursor.last_seen_post_id.as_deref(), Some("post-k1-latest"));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy ack <channel>` on an empty channel returns success with
/// `cursor_post_id: null`; cursor is unchanged.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn ack_empty_channel_returns_success_no_advance() {
    let env = TestEnv::new("per-023-ack-empty").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-k2", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-k2").await;
    env.mock_channel_posts("chan-id-k2", &[]).await;

    let daemon = spawn_daemon(&env).await;
    let pre_state = read_attention_state(&env);

    let out = run_chanvoy(&env, &["--json", "ack", "bravo-team"]).await;
    assert!(
        out.status.success(),
        "ack on empty channel is success, not error; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).expect("ack --json parses");
    assert!(
        result["cursor_post_id"].is_null(),
        "empty channel → cursor_post_id is null"
    );

    let post_state = read_attention_state(&env);
    assert_eq!(
        pre_state, post_state,
        "ack on empty channel does not mutate attention state"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy read --since-bootstrap` happy path: returns the bounded
/// most-recent-N posts via the per_page-N endpoint.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_since_bootstrap_returns_recent_posts() {
    let env = TestEnv::new("per-023-bootstrap-happy").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-b1", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-b1").await;
    env.mock_channel_posts(
        "chan-id-b1",
        &[
            ("post-b1", "user-1", "alice", "first", 1_700_000_000_000),
            ("post-b2", "user-2", "bob", "second", 1_700_000_001_000),
            ("post-b3", "user-3", "carol", "third", 1_700_000_002_000),
        ],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "read", "bravo-team", "--since-bootstrap"]).await;
    assert!(
        out.status.success(),
        "read --since-bootstrap must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let messages: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).expect("bootstrap --json parses");
    assert_eq!(messages.len(), 3, "all mocked posts returned");

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// Devrev PR #20 P2 follow-ups: AC test cases the first cut missed.
// ----------------------------------------------------------------------

/// Mount the additional mocks needed for cross-team `<team>/<channel>`
/// resolution: `/users/me/teams` enumeration plus the by-name lookup
/// against the alternate team. `mock_baseline` already mounts the
/// per-team-name lookup for `org-lanytehq` (the profile's primary
/// team); this helper adds the alternate team to the bot's membership
/// so the γ hybrid resolver can route an `<other-team>/<channel>`
/// invocation.
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
    // Per-team name lookup so `resolve_in_team` can find the alt team
    // by slug. `mock_baseline` already mounts this for the primary.
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/teams/name/{alt_team}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": alt_team_id,
            "name": alt_team,
        })))
        .mount(&env.mock)
        .await;
}

/// `read --since-bootstrap --advance` advances the cursor to the latest
/// post in the bounded window (per AC #4 + AC #2a — the
/// contract-load-bearing case where the bounded window may diverge
/// from the channel's absolute latest).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_since_bootstrap_advance_records_latest_in_window() {
    let env = TestEnv::new("per-023-bootstrap-advance").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-bba", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-bba").await;
    env.mock_channel_posts(
        "chan-id-bba",
        &[
            ("post-bba1", "user-1", "alice", "first", 1_700_000_000_000),
            ("post-bba2", "user-2", "bob", "second", 1_700_000_001_000),
        ],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "--json",
            "read",
            "bravo-team",
            "--since-bootstrap",
            "--advance",
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "bootstrap --advance must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let post_state = read_attention_state(&env).expect("--advance writes cursor");
    let cursor = post_state
        .channels
        .values()
        .next()
        .expect("cursor recorded");
    assert_eq!(
        cursor.last_seen_post_id.as_deref(),
        Some("post-bba2"),
        "cursor advances to latest post returned in the bounded window"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `read --since <window> --limit N --advance` where the window contains
/// MORE than N posts: cursor advances to the Nth-most-recent post
/// returned, NOT the channel absolute latest. Per amended brief PR #47
/// test matrix entry: "boundary case (window has more than N posts;
/// cursor still goes to Nth-most-recent returned, not channel absolute
/// latest)."
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_since_limit_advance_truncates_to_returned_window() {
    let env = TestEnv::new("per-023-limit-truncate").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-lt", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-lt").await;
    // 5 posts in the window, but `--limit 3` truncates to the most
    // recent 3 (post-lt3 / post-lt4 / post-lt5). Cursor target =
    // post-lt5 (latest returned within the limit), NOT post-lt5
    // because that's also the channel's absolute latest in this
    // mocked dataset — the assertion that protects the contract is
    // that the returned message list has length 3, not 5.
    env.mock_channel_posts(
        "chan-id-lt",
        &[
            ("post-lt1", "user-1", "alice", "1", 1_700_000_000_000),
            ("post-lt2", "user-2", "bob", "2", 1_700_000_001_000),
            ("post-lt3", "user-3", "carol", "3", 1_700_000_002_000),
            ("post-lt4", "user-4", "dave", "4", 1_700_000_003_000),
            ("post-lt5", "user-5", "eve", "5", 1_700_000_004_000),
        ],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "--json",
            "read",
            "bravo-team",
            "--since",
            "5m",
            "--limit",
            "3",
            "--advance",
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "read --since 5m --limit 3 --advance must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let messages: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).expect("--json parses");
    assert_eq!(
        messages.len(),
        3,
        "--limit 3 truncates the 5-post window to 3 most-recent"
    );
    let returned_ids: Vec<&str> = messages.iter().filter_map(|m| m["id"].as_str()).collect();
    assert_eq!(
        returned_ids,
        vec!["post-lt3", "post-lt4", "post-lt5"],
        "--limit truncates from the front (oldest dropped); newest preserved"
    );

    let post_state = read_attention_state(&env).expect("--advance writes cursor");
    let cursor = post_state
        .channels
        .values()
        .next()
        .expect("cursor recorded");
    assert_eq!(
        cursor.last_seen_post_id.as_deref(),
        Some("post-lt5"),
        "cursor advances to latest post in the **returned** window"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `read --after <post> --limit N --advance` (cheap-bonus case from
/// amended brief PR #47 test matrix).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_after_limit_advance_records_latest_returned() {
    let env = TestEnv::new("per-023-after-limit-advance").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-al", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-al").await;
    // The `read --after <post>` path validates the anchor exists via
    // `GET /posts/{post_id}` first (assert_post_in_channel), then
    // pages forward.
    env.mock_post_lookup("post-anchor", "chan-id-al", true)
        .await;
    env.mock_channel_posts(
        "chan-id-al",
        &[
            ("post-al1", "user-1", "alice", "after 1", 1_700_000_001_000),
            ("post-al2", "user-2", "bob", "after 2", 1_700_000_002_000),
        ],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "--json",
            "read",
            "bravo-team",
            "--after",
            "post-anchor",
            "--limit",
            "10",
            "--advance",
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "read --after --limit --advance must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let post_state = read_attention_state(&env).expect("--advance writes cursor");
    let cursor = post_state
        .channels
        .values()
        .next()
        .expect("cursor recorded");
    assert_eq!(
        cursor.last_seen_post_id.as_deref(),
        Some("post-al2"),
        "cursor advances to latest returned post regardless of read mode"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy pinned <team>/<channel>` cross-team syntax routes to the
/// explicit team via the PER-019 γ hybrid resolver.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn pinned_cross_team_syntax_routes_via_explicit_resolver() {
    let env = TestEnv::new("per-023-pinned-cross-team").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-pc", "agent-bravo-devlead", "team-id-456")
        .await;
    mount_alternate_team(&env, "team-id-456", "org-3leaps", "team-id-3leaps").await;
    // The channel exists on the alternate team only — confirms the
    // resolver is routing on the explicit team slug, not falling
    // through to the primary.
    env.mock_channel_lookup_for_team("team-id-3leaps", "ops-tech", "chan-id-3l-ops")
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/channels/chan-id-3l-ops/pinned_posts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "posts": {
                "pin-1": {
                    "id": "pin-1",
                    "user_id": "user-1",
                    "username": "alice",
                    "message": "cross-team pin",
                    "create_at": 1_700_000_000_000i64,
                },
            }
        })))
        .mount(&env.mock)
        .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "pinned", "org-3leaps/ops-tech"]).await;
    assert!(
        out.status.success(),
        "pinned cross-team must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).expect("pinned --json parses");
    assert_eq!(parsed.len(), 1, "alternate-team pin returned");
    assert_eq!(parsed[0]["message"].as_str(), Some("cross-team pin"));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `chanvoy ack <team>/<channel>` cross-team syntax routes via the
/// PER-019 γ hybrid resolver and records the cursor under the
/// qualified `<team>/<channel>` key (PER-019 cursor isolation).
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn ack_cross_team_syntax_records_under_qualified_key() {
    let env = TestEnv::new("per-023-ack-cross-team").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-ac", "agent-bravo-devlead", "team-id-456")
        .await;
    mount_alternate_team(&env, "team-id-456", "org-3leaps", "team-id-3leaps").await;
    env.mock_channel_lookup_for_team("team-id-3leaps", "ops-tech", "chan-id-3l-ack")
        .await;
    env.mock_channel_posts(
        "chan-id-3l-ack",
        &[(
            "post-3l-latest",
            "user-1",
            "alice",
            "alt-team latest",
            1_700_000_002_000,
        )],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["--json", "ack", "org-3leaps/ops-tech"]).await;
    assert!(
        out.status.success(),
        "ack cross-team must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).expect("ack --json parses");
    assert_eq!(
        result["team"].as_str(),
        Some("org-3leaps"),
        "ack reports the resolved team, confirming γ resolver routed via explicit"
    );
    assert_eq!(result["channel"].as_str(), Some("ops-tech"));
    assert_eq!(
        result["cursor_post_id"].as_str(),
        Some("post-3l-latest"),
        "ack returns the alt-team's current latest"
    );

    let post_state = read_attention_state(&env).expect("ack writes attention state");
    // PER-019 cursor isolation: the cursor must be keyed by qualified
    // `<team>/<channel>`, not bare channel name.
    let key = post_state
        .channels
        .keys()
        .next()
        .expect("one cursor recorded");
    assert!(
        key.contains("org-3leaps") && key.contains("ops-tech"),
        "cursor key must be qualified by alternate team; got: {key}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}
