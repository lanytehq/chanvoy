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
//! ## Investigation outcome (2026-05-12)
//!
//! End-to-end code trace (CLI → daemon dispatch → core
//! `read_channel_since_secs` → MM `/channels/{id}/posts?since={millis}`)
//! shows no client-side cursor coupling: the dispatch path does not
//! reference `AttentionState` and the response is forwarded unaltered.
//! Live probes from the current bravo-devlead bot identity against
//! multiple channels with stale cursors could not reproduce the
//! original symptom. The leading hypothesis is that the PR #26
//! symptom traced to a Mattermost-side permission interaction
//! (sandbox / lower-capability identity returning a filtered post set
//! from `/posts?since=`), not a chanvoy code bug. Same pattern has
//! been observed with codex agents running without escalated
//! permissions — see `feedback_chanvoy_mm_symptoms_sandbox_permission_factor`
//! in agent memory.
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
