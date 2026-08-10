//! PER-032 Item I — defensive contract tests for `chanvoy read --since N`.
//!
//! ## Why this test file exists
//!
//! The brief describes a symptom observed at PR #26 clearance on
//! 2026-05-08: `check` reported new posts, `read --since 60` returned
//! empty, and `read --since-bootstrap --limit 5` surfaced the same
//! posts `check` had flagged. The documented contract in
//! `docs/operator-guide.md` §"Resume And Attention" says `read --since`
//! is a **pure read** that returns posts within the time window,
//! independent of the daemon's stored cursor state.
//!
//! ## Investigation outcome (2026-05-12 / 13)
//!
//! End-to-end code trace (CLI → daemon dispatch → core
//! `read_channel_since_secs` → MM `/channels/{id}/posts?since={millis}`)
//! shows no client-side cursor coupling: the dispatch path does not
//! reference `AttentionState` and the response is forwarded unaltered.
//!
//! Two cross-identity dogfood probes ran on 2026-05-12:
//!
//! - **`agent-bravo-devlead`** (Claude on this harness): probed
//!   `repo-chanvoy-ops`, `ops-updates`, `bravo-team`, and
//!   `release-chanvoy-v022` across `check`, `read --since N`, and
//!   `read --since-bootstrap`. Behavior matched the documented
//!   contract on every channel.
//! - **`agent-bravo-devrev`** (GPT on opencode): same probe shape from
//!   a different runtime, model, and effective sandbox. Could NOT
//!   reproduce the exact original PR #26-clearance condition (`check
//!   new>0` AND bootstrap-returns-posts AND `--since` empty), but
//!   observed a broader anomaly: on `repo-chanvoy-ops`, `ops-updates`,
//!   and `release-chanvoy-v022`, `check` reported `new > 0` while
//!   `read --since N` AND `read --since-bootstrap --limit 5` both
//!   returned empty for that identity. One exception:
//!   `release-chanvoy-v022 read --since 1440` returned one recent
//!   post. The anomaly cuts across both `?since=` and `?per_page=` MM
//!   endpoints, so a chanvoy-side `--since`-specific bug is unlikely
//!   to be the whole story.
//!
//! Leading hypothesis: a Mattermost-side identity / permission /
//! caching factor affects multiple read endpoints under at least one
//! agent identity. Same pattern shows up with codex agents running
//! without escalated permissions — see
//! `feedback_chanvoy_mm_symptoms_sandbox_permission_factor` in agent
//! memory. Honest framing: the broader anomaly was not reproduced in
//! the chanvoy code path; the daemon-side contract is structurally
//! correct; the symptom surface is wider than the original brief
//! captured and warrants a v0.2.3+ `chanvoy doctor`-style permission
//! self-diagnostic verb to give operators a way to isolate it.
//!
//! ## What these tests guarantee (AC #1, #2, #4)
//!
//! Regardless of the original symptom's root cause, the daemon-side
//! contract is structurally guarded:
//!
//! 1. `read --since N` returns every post the upstream server returns,
//!    even when the daemon's stored cursor points at one of those posts
//!    (cursor inside window) or precedes them (cursor before window).
//! 2. `read --since N` without `--advance` does not mutate the stored
//!    cursor — `--since` is a pure read.
//!
//! These tests use wiremock's path-only matcher (query strings ignored),
//! so they cannot replicate an MM-side `?since=` filter. They assert the
//! daemon's behavior under the contract: given a response, the daemon
//! does not drop posts based on cursor state and does not advance the
//! cursor as a side effect of a plain `--since` call.

#![allow(dead_code)]

mod common;

use common::{read_attention_state, run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};

/// AC #1 + #2 — Item I core contract.
///
/// Pre-seeds the daemon cursor at a mocked post (via `ack` which
/// advances to the channel's latest), then issues `read --since 60`.
/// The response must include every post the upstream returned —
/// including the cursor-pre-seeded post itself. A regression that
/// added cursor-based filtering to the `--since` dispatch path would
/// drop the cursor's own post and fail this test.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_since_returns_window_including_cursor_post() {
    let env = TestEnv::new("per-032-since-includes-cursor").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-i1", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-i1").await;
    env.mock_channel_posts(
        "chan-id-i1",
        &[
            ("post-i1-a", "user-1", "alice", "first", 1_700_000_000_000),
            ("post-i1-b", "user-2", "bob", "second", 1_700_000_001_000),
            ("post-i1-c", "user-3", "carol", "third", 1_700_000_002_000),
        ],
    )
    .await;

    let daemon = spawn_daemon(&env).await;

    // Pre-seed cursor at the channel's latest (post-i1-c) via ack —
    // mirrors the operator session-start ritual that produced the PR
    // #26 symptom (`check` → `ack` → `read --since`).
    let ack_out = run_chanvoy(&env, &["--json", "ack", "bravo-team"]).await;
    assert!(
        ack_out.status.success(),
        "ack must exit 0 to pre-seed cursor; stderr={}",
        String::from_utf8_lossy(&ack_out.stderr)
    );
    let pre_state = read_attention_state(&env).expect("ack writes attention state");
    let pre_cursor = pre_state
        .channels
        .values()
        .next()
        .expect("cursor recorded by ack");
    assert_eq!(
        pre_cursor.last_seen_post_id.as_deref(),
        Some("post-i1-c"),
        "ack pre-seeds cursor at the channel's latest post"
    );

    // Contract: read --since must return all three mocked posts even
    // though the cursor now sits at post-i1-c. A failing run would
    // return fewer than 3 posts (cursor-based filtering would drop
    // post-i1-c and/or earlier).
    let out = run_chanvoy(&env, &["--json", "read", "bravo-team", "--since", "60"]).await;
    assert!(
        out.status.success(),
        "read --since 60 must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let messages: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).expect("read --json parses");
    assert_eq!(
        messages.len(),
        3,
        "read --since must return every post the upstream returned, \
         including the cursor's own post; got {} message(s)",
        messages.len()
    );
    let ids: Vec<&str> = messages
        .iter()
        .map(|m| m["id"].as_str().expect("message has id"))
        .collect();
    assert!(
        ids.contains(&"post-i1-c"),
        "cursor-pre-seeded post must appear in --since response; got ids={ids:?}"
    );
    assert!(
        ids.contains(&"post-i1-a") && ids.contains(&"post-i1-b"),
        "older posts within the window must also appear; got ids={ids:?}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// AC #1 + #2 — cursor at an older mid-window post.
///
/// Two-phase: phase 1 mounts only the two oldest posts and acks
/// (cursor lands at post-i2-b). Phase 2 resets mocks, mounts the
/// channel response with a third newer post added, and re-issues
/// `read --since 60`. The cursor is now strictly inside the response
/// window with newer posts following it — the precise scenario the
/// brief's hypothesis #1 (daemon dispatch intersects time-window
/// with stored cursor) would break. Contract: all three posts return.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_since_returns_window_when_cursor_at_older_post() {
    let env = TestEnv::new("per-032-since-cursor-mid-window").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-i2", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-i2").await;

    // Phase 1: only post-i2-a + post-i2-b visible — ack lands cursor at b.
    env.mock_channel_posts(
        "chan-id-i2",
        &[
            ("post-i2-a", "user-1", "alice", "first", 1_700_000_000_000),
            ("post-i2-b", "user-2", "bob", "second", 1_700_000_001_000),
        ],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let ack_out = run_chanvoy(&env, &["--json", "ack", "bravo-team"]).await;
    assert!(
        ack_out.status.success(),
        "phase-1 ack must exit 0; stderr={}",
        String::from_utf8_lossy(&ack_out.stderr)
    );
    let pre_state = read_attention_state(&env).expect("ack writes state");
    let pre_cursor = pre_state.channels.values().next().expect("one cursor");
    assert_eq!(
        pre_cursor.last_seen_post_id.as_deref(),
        Some("post-i2-b"),
        "cursor pre-seeded at older mid-window post"
    );

    // Phase 2: add a newer post (post-i2-c). Cursor is now strictly
    // inside the response window with one newer post following it.
    env.reset_mocks().await;
    env.mock_baseline("bot-id-i2", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-i2").await;
    env.mock_channel_posts(
        "chan-id-i2",
        &[
            ("post-i2-a", "user-1", "alice", "first", 1_700_000_000_000),
            ("post-i2-b", "user-2", "bob", "second", 1_700_000_001_000),
            ("post-i2-c", "user-3", "carol", "third", 1_700_000_002_000),
        ],
    )
    .await;

    let out = run_chanvoy(&env, &["--json", "read", "bravo-team", "--since", "60"]).await;
    assert!(
        out.status.success(),
        "phase-2 read --since 60 must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let messages: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).expect("read --json parses");
    let ids: Vec<&str> = messages
        .iter()
        .map(|m| m["id"].as_str().expect("message has id"))
        .collect();
    assert_eq!(
        messages.len(),
        3,
        "cursor at mid-window post must not filter older or newer posts; got ids={ids:?}"
    );
    assert!(
        ids.contains(&"post-i2-a"),
        "post older than cursor must appear; got ids={ids:?}"
    );
    assert!(
        ids.contains(&"post-i2-b"),
        "cursor's own post must appear; got ids={ids:?}"
    );
    assert!(
        ids.contains(&"post-i2-c"),
        "post newer than cursor must appear; got ids={ids:?}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// AC #4 — `read --since` without `--advance` does not mutate the
/// stored cursor. The contract calls `--since` a "pure read"; this
/// asserts it byte-for-byte. Regression catch: any future change
/// that records the latest-returned post as the new cursor on a
/// plain `--since` would fail here.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_since_does_not_advance_cursor() {
    let env = TestEnv::new("per-032-since-no-advance").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-i3", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-i3").await;

    // Pre-seed cursor at post-i3-a, then make a newer post visible.
    env.mock_channel_posts(
        "chan-id-i3",
        &[("post-i3-a", "user-1", "alice", "first", 1_700_000_000_000)],
    )
    .await;

    let daemon = spawn_daemon(&env).await;
    let ack_out = run_chanvoy(&env, &["--json", "ack", "bravo-team"]).await;
    assert!(
        ack_out.status.success(),
        "pre-seeding ack must exit 0; stderr={}",
        String::from_utf8_lossy(&ack_out.stderr)
    );
    let pre_state = read_attention_state(&env).expect("ack writes state");

    // Re-mount with a newer post so a buggy --since-advances-cursor
    // implementation would visibly progress the cursor.
    env.reset_mocks().await;
    env.mock_baseline("bot-id-i3", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-i3").await;
    env.mock_channel_posts(
        "chan-id-i3",
        &[
            ("post-i3-a", "user-1", "alice", "first", 1_700_000_000_000),
            ("post-i3-b", "user-2", "bob", "second", 1_700_000_001_000),
        ],
    )
    .await;

    let out = run_chanvoy(&env, &["--json", "read", "bravo-team", "--since", "60"]).await;
    assert!(
        out.status.success(),
        "read --since must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let post_state = read_attention_state(&env).expect("state file still present");
    assert_eq!(
        pre_state, post_state,
        "read --since without --advance must not mutate the attention state"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// The emitted `?since=` boundary, asserted against the request the
/// daemon actually sent.
///
/// The tests above mount wiremock with a path-only matcher, so the
/// query string is ignored and a `--since` read is proven only from the
/// response side: given posts, the daemon returns them. That leaves the
/// request side unproven — a build that emitted no `since` at all, or a
/// boundary computed in seconds instead of milliseconds, or one derived
/// from the wrong end of the window, would satisfy every assertion in
/// this file while sending a query that cannot mean what the operator
/// asked for.
///
/// This test reads the recorded request instead. A `--since 60` (bare
/// integer = minutes) must emit a millisecond boundary one hour behind
/// the moment of the call, and the post sitting inside that window must
/// come back.
///
/// It matters because the symptom this file documents — `check` sees
/// posts while a time-window read comes back empty — is exactly what a
/// wrong boundary looks like from an operator's seat, and the response
/// side cannot tell the two apart.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn read_since_emits_a_millisecond_window_boundary() {
    let env = TestEnv::new("per-032-since-query-boundary").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-i4", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-i4").await;

    // A post whose create_at is inside the requested window, so the
    // request-side assertion is not carried by an empty response.
    let now_millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the unix epoch")
        .as_millis() as i64;
    let inside_window = now_millis - 5 * 60 * 1000;
    env.mock_channel_posts(
        "chan-id-i4",
        &[("post-i4-a", "user-1", "alice", "inside", inside_window)],
    )
    .await;

    let daemon = spawn_daemon(&env).await;

    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the unix epoch")
        .as_millis() as i64;
    let out = run_chanvoy(&env, &["--json", "read", "bravo-team", "--since", "60"]).await;
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the unix epoch")
        .as_millis() as i64;
    assert!(
        out.status.success(),
        "read --since 60 must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let messages: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).expect("read --json parses");
    assert_eq!(
        messages.len(),
        1,
        "the post inside the window must be returned; got {messages:?}"
    );

    // The request side: find the posts fetch and read its `since`.
    let requests = env
        .mock
        .received_requests()
        .await
        .expect("wiremock records requests");
    let posts_request = requests
        .iter()
        .rev()
        .find(|r| r.url.path() == "/api/v4/channels/chan-id-i4/posts")
        .expect("the daemon fetched channel posts");
    let since_raw = posts_request
        .url
        .query_pairs()
        .find(|(k, _)| k == "since")
        .map(|(_, v)| v.into_owned())
        .expect("a time-window read must emit a `since` query parameter");
    let since: i64 = since_raw
        .parse()
        .unwrap_or_else(|e| panic!("`since={since_raw}` must be an integer: {e}"));

    // One hour behind the call, in milliseconds. The bounds are the
    // clock readings taken either side of the call, so this cannot go
    // green on a boundary derived from a fixed or stale timestamp.
    let window_millis = 60 * 60 * 1000;
    assert!(
        since >= before - window_millis && since <= after - window_millis,
        "`--since 60` must emit a millisecond boundary one hour behind the \
         call: got {since}, expected within [{}, {}]",
        before - window_millis,
        after - window_millis
    );

    // Seconds-vs-milliseconds is the failure this pins down: a boundary
    // in seconds is ~1000x too small and would read as 1970 to the
    // provider, quietly widening every window to all of history.
    assert!(
        since > 1_000_000_000_000,
        "the boundary must be in milliseconds, not seconds: got {since}"
    );
    assert!(
        since < inside_window,
        "the emitted boundary must precede the post that has to be \
         returned: boundary={since}, post create_at={inside_window}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}
