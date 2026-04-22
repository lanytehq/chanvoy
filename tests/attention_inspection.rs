//! PER-008B: attention-state inspection-surface tests.
//!
//! Exercises the `chanvoy attention list` and `chanvoy attention show`
//! commands end-to-end through the real daemon binary. The central
//! reviewer ask — surfaced by devrev on 2026-04-21 — is the
//! **non-mutation invariant**: adding these commands leaves daemon
//! state identical before and after. These tests assert that
//! invariant by byte-comparing the persisted attention-state file
//! across CLI invocations, not just by code-reading the read-only
//! claim.
//!
//! D1 staleness shape (cxotech + devrev alignment, 2026-04-21):
//! - Staleness is a CACHED value on `ChannelCursorState`, not a
//!   per-call probe. `attention list` reads it locally.
//! - `last_known_stale` flips true on `check_channel`'s
//!   AnchorNotFound / AnchorChannelMismatch path, false on a
//!   successful probe, and is cleared on any cursor-write path.
//! - `last_checked_at` (cxotech's refinement) captures "how fresh
//!   is this staleness verdict?" so operators can tell freshly-
//!   verified cursors from never-checked ones.
//!
//! Shared harness primitives (`TestEnv`, spawn/stop helpers, mocks)
//! live in `tests/common/mod.rs`; this file only adds the PER-008B-
//! specific test scenarios on top of them.

#![allow(dead_code)]

mod common;

use common::{
    read_attention_state, read_attention_state_bytes, run_chanvoy, spawn_daemon,
    stop_daemon_cleanly, TestEnv,
};

/// `attention list` on a daemon with no tracked channels surfaces an
/// empty channels list + `no_anchor` mentions, JSON and text both.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn attention_list_cold_state_is_empty() {
    let env = TestEnv::new("per-008b-list-cold").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-b1", "agent-bravo-devlead", "team-id-456")
        .await;

    let daemon = spawn_daemon(&env).await;

    let out = run_chanvoy(&env, &["--json", "attention", "list"]).await;
    assert!(
        out.status.success(),
        "attention list must exit 0 on cold state, stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("json report parses");
    assert_eq!(parsed["profile"].as_str(), Some(env.profile_name.as_str()));
    assert_eq!(
        parsed["channels"].as_array().map(|a| a.len()),
        Some(0),
        "cold state has no tracked channels"
    );
    assert_eq!(
        parsed["mentions"]["source"].as_str(),
        Some("no_anchor"),
        "cold state has no mention cursor"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// After a successful `chanvoy post`, `attention list` shows the
/// channel with `source=post_cursor` and the post id as `newest_seen`.
/// `attention show` on the tracked channel returns matching detail.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn attention_list_and_show_after_post() {
    let env = TestEnv::new("per-008b-after-post").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-b2", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-b2").await;
    env.mock_post_create("post-id-b2-xyz").await;

    let daemon = spawn_daemon(&env).await;
    let post_out = run_chanvoy(&env, &["post", "bravo-team", "hello"]).await;
    assert!(
        post_out.status.success(),
        "post must succeed, stderr={}",
        String::from_utf8_lossy(&post_out.stderr)
    );

    let list_out = run_chanvoy(&env, &["--json", "attention", "list"]).await;
    assert!(list_out.status.success());
    let list: serde_json::Value =
        serde_json::from_slice(&list_out.stdout).expect("json list parses");
    let channels = list["channels"].as_array().expect("channels array");
    assert_eq!(channels.len(), 1, "exactly one tracked channel after post");
    let entry = &channels[0];
    assert_eq!(entry["channel"].as_str(), Some("bravo-team"));
    assert_eq!(entry["source"].as_str(), Some("post_cursor"));
    assert_eq!(entry["newest_seen"].as_str(), Some("post-id-b2-xyz"));
    assert!(
        entry["last_checked_at"].is_null(),
        "last_checked_at is null before any check pass (cursor-write path sets it to None)"
    );

    let show_out = run_chanvoy(&env, &["--json", "attention", "show", "bravo-team"]).await;
    assert!(show_out.status.success());
    let show: serde_json::Value =
        serde_json::from_slice(&show_out.stdout).expect("json show parses");
    assert_eq!(show["channel"]["source"].as_str(), Some("post_cursor"));
    assert_eq!(
        show["channel"]["newest_seen"].as_str(),
        Some("post-id-b2-xyz")
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `attention list` surfaces profile `monitored_channels` that have
/// no persisted cursor as `no_anchor` rows — the tracked-but-
/// uncursored state is explicitly part of AC #1 / #6 (devrev finding
/// 2026-04-22). Without this, operators configuring monitored_channels
/// would see those channels missing from `attention list` until a
/// post or seed cursors them.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn attention_list_includes_monitored_but_uncursored_channels() {
    let env = TestEnv::new("per-008b-monitored-uncursored").await;
    // Profile declares two monitored channels; neither has a cursor yet.
    env.write_profile_with_monitored(
        "agent-bravo-devlead",
        "org-lanytehq",
        &["bravo-team", "per-008"],
    );
    env.mock_baseline("bot-id-b7", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("per-009", "chan-id-b7-per009")
        .await;
    env.mock_post_create("post-id-b7").await;

    let daemon = spawn_daemon(&env).await;

    // Post to a channel NOT in monitored_channels so the list spans the
    // union: monitored ∪ cursored = {bravo-team, per-008, per-009}.
    let post_out = run_chanvoy(&env, &["post", "per-009", "hi"]).await;
    assert!(
        post_out.status.success(),
        "post must succeed, stderr={}",
        String::from_utf8_lossy(&post_out.stderr)
    );

    let out = run_chanvoy(&env, &["--json", "attention", "list"]).await;
    assert!(out.status.success());
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json list parses");
    let channels = parsed["channels"].as_array().expect("channels array");
    assert_eq!(
        channels.len(),
        3,
        "expected all three channels (bravo-team, per-008 monitored-uncursored; per-009 cursored); got {channels:?}"
    );
    let by_name: std::collections::BTreeMap<&str, &serde_json::Value> = channels
        .iter()
        .map(|c| (c["channel"].as_str().unwrap(), c))
        .collect();
    assert_eq!(
        by_name["bravo-team"]["source"].as_str(),
        Some("no_anchor"),
        "monitored-but-uncursored channel must surface as no_anchor"
    );
    assert_eq!(
        by_name["per-008"]["source"].as_str(),
        Some("no_anchor"),
        "monitored-but-uncursored channel must surface as no_anchor"
    );
    assert_eq!(
        by_name["per-009"]["source"].as_str(),
        Some("post_cursor"),
        "cursored channel (not in monitored_channels) still surfaces"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// `attention show` on an untracked channel returns a `no_anchor`
/// entry with null cursor fields, not an error.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn attention_show_untracked_channel_is_no_anchor() {
    let env = TestEnv::new("per-008b-show-untracked").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-b3", "agent-bravo-devlead", "team-id-456")
        .await;

    let daemon = spawn_daemon(&env).await;

    let show_out = run_chanvoy(&env, &["--json", "attention", "show", "untracked"]).await;
    assert!(
        show_out.status.success(),
        "show on untracked channel must exit 0 (it's a normal state, not an error); stderr={}",
        String::from_utf8_lossy(&show_out.stderr)
    );
    let show: serde_json::Value =
        serde_json::from_slice(&show_out.stdout).expect("json show parses");
    assert_eq!(show["channel"]["channel"].as_str(), Some("untracked"));
    assert_eq!(show["channel"]["source"].as_str(), Some("no_anchor"));
    assert!(show["channel"]["newest_seen"].is_null());

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// After `check` detects a stale anchor (`AnchorNotFound` path),
/// `attention list` surfaces the channel with `source=stale_cursor`
/// and a populated `last_checked_at`. This is the cached-staleness
/// shape D1 alignment pinned.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn attention_list_shows_stale_cursor_after_check() {
    let env = TestEnv::new("per-008b-stale-after-check").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-b4", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-b4").await;
    env.mock_post_create("post-id-b4").await;

    let daemon = spawn_daemon(&env).await;

    // Establish a cursor via post.
    let post_out = run_chanvoy(&env, &["post", "bravo-team", "establish"]).await;
    assert!(post_out.status.success());

    // List while fresh: source is post_cursor, no staleness verdict yet.
    let list1 = run_chanvoy(&env, &["--json", "attention", "list"]).await;
    assert!(list1.status.success());
    let list1_json: serde_json::Value = serde_json::from_slice(&list1.stdout).unwrap();
    assert_eq!(
        list1_json["channels"][0]["source"].as_str(),
        Some("post_cursor")
    );

    // Flip the anchor to 404 so the next `check` detects staleness.
    env.mock_post_lookup("post-id-b4", "chan-id-b4", false)
        .await;
    let check_out = run_chanvoy(&env, &["--json", "check", "bravo-team"]).await;
    // check returns exit 1 on no-new-messages (stale path falls there)
    assert_eq!(check_out.status.code(), Some(1));
    let check_json: serde_json::Value = serde_json::from_slice(&check_out.stdout).unwrap();
    assert_eq!(check_json["anchor_source"].as_str(), Some("stale_cursor"));

    // List should now show source=stale_cursor and a last_checked_at.
    let list2 = run_chanvoy(&env, &["--json", "attention", "list"]).await;
    assert!(list2.status.success());
    let list2_json: serde_json::Value = serde_json::from_slice(&list2.stdout).unwrap();
    let entry = &list2_json["channels"][0];
    assert_eq!(entry["source"].as_str(), Some("stale_cursor"));
    assert!(
        entry["last_checked_at"].as_i64().unwrap_or(0) > 0,
        "last_checked_at must be populated after a check pass; entry={entry}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// devrev's explicit ask: assert the non-mutation invariant in tests,
/// not just by code reading. Snapshot the persisted attention-state
/// file bytes before and after `attention list` + `attention show`
/// invocations; assert byte-equal. The `attention` prefix must not
/// write to daemon state.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn attention_commands_do_not_mutate_state_file() {
    let env = TestEnv::new("per-008b-nonmutation").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-b5", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-b5").await;
    env.mock_post_create("post-id-b5").await;

    let daemon = spawn_daemon(&env).await;

    // Seed some non-trivial state so the snapshot has content worth
    // comparing: one post_cursor, recorded via `chanvoy post`.
    let post_out = run_chanvoy(&env, &["post", "bravo-team", "seed"]).await;
    assert!(post_out.status.success());

    let snapshot_before =
        read_attention_state_bytes(&env).expect("state file exists after seed post");
    let parsed_before = read_attention_state(&env).expect("state parses");
    assert!(
        parsed_before.channels.contains_key("bravo-team"),
        "pre-snapshot has the seeded channel cursor"
    );

    // Run the full inspection surface: list + show (tracked + untracked).
    for args in [
        &["--json", "attention", "list"][..],
        &["--json", "attention", "show", "bravo-team"][..],
        &["--json", "attention", "show", "untracked-channel"][..],
        &["attention", "list"][..], // text output path too
        &["attention", "show", "bravo-team"][..],
    ] {
        let out = run_chanvoy(&env, args).await;
        assert!(
            out.status.success(),
            "attention invocation must exit 0; args={args:?} stderr={}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let snapshot_after = read_attention_state_bytes(&env).expect("state file still present");
    assert_eq!(
        snapshot_before, snapshot_after,
        "attention list/show must NOT mutate the persisted state file \
         (non-mutation invariant, PER-008B AC #5)"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// Text output for `attention list` after a post is human-readable
/// (doesn't just format-panic), contains the key columns, and
/// includes the channel row with its post-cursor source.
#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn attention_list_text_output_renders() {
    let env = TestEnv::new("per-008b-text-output").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-b6", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id-b6").await;
    env.mock_post_create("post-id-b6").await;

    let daemon = spawn_daemon(&env).await;
    let _ = run_chanvoy(&env, &["post", "bravo-team", "hi"]).await;

    let out = run_chanvoy(&env, &["attention", "list"]).await;
    assert!(
        out.status.success(),
        "attention list (text) must exit 0, stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("CHANNEL"),
        "text output has header row; stdout={stdout}"
    );
    assert!(
        stdout.contains("bravo-team"),
        "text output lists the tracked channel; stdout={stdout}"
    );
    assert!(
        stdout.contains("post_cursor"),
        "text output surfaces source discriminator; stdout={stdout}"
    );
    assert!(
        stdout.contains("mentions:"),
        "text output surfaces mentions sibling; stdout={stdout}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}
