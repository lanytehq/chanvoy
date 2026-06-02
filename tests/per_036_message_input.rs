//! PER-036: message-body input shapes — `--message-file` + `-` stdin.
//!
//! End-to-end through the real daemon binary against a wiremock
//! Mattermost server. The falsifiable assertion is the **posted body**:
//! the `message` field of the recorded `POST /api/v4/posts` must match
//! the file / stdin / positional source byte-for-byte.
//!
//! Covered:
//! - `post --message-file` posts the file contents verbatim (AC #1).
//! - `post -` posts piped stdin verbatim (AC #2).
//! - positional `<message>` still works unchanged (AC #4).
//! - all four message-writing verbs wire the resolver: `post`,
//!   `dm send`, legacy `dm <user> <msg>`, `notify` (AC #3).
//! - mutex + no-source rejections at the CLI layer, including the
//!   legacy `dm` manual-parse path (AC #5/#6).
//! - over-length MM rejection is normalized to name the char count and
//!   `Posts.MaxPostSize` (AC #8).
//!
//! CLI-layer resolver unit tests (file/stdin/TTY/UTF-8 diagnostics)
//! live in `chanvoy-cli` (AC #7/#9); these exercise the end-to-end
//! wiring + posted-body fidelity.

#![allow(dead_code)]

mod common;

use common::{
    posted_message_body, run_chanvoy, run_chanvoy_with_stdin, spawn_daemon, stop_daemon_cleanly,
    TestEnv,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

const MULTILINE_BODY: &str =
    "# Release notes\n\nLine with `backtick`, $VAR, and ! — plus \"quotes\".\n\n- bullet\n";

// ----------------------------------------------------------------------
// post: file / stdin / positional fidelity
// ----------------------------------------------------------------------

#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_message_file_posts_file_contents_verbatim() {
    let env = TestEnv::new("per-036-post-file").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id").await;
    env.mock_post_create("post-1").await;

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("body.md");
    std::fs::write(&file, MULTILINE_BODY).unwrap();

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "post",
            "bravo-team",
            "--message-file",
            file.to_str().unwrap(),
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "post --message-file must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        posted_message_body(&env).await,
        MULTILINE_BODY,
        "posted body must match the file byte-for-byte (trailing newline preserved)"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_stdin_dash_posts_piped_body() {
    let env = TestEnv::new("per-036-post-stdin").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id").await;
    env.mock_post_create("post-2").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy_with_stdin(
        &env,
        &["post", "bravo-team", "-"],
        MULTILINE_BODY.as_bytes(),
    )
    .await;
    assert!(
        out.status.success(),
        "post - (stdin) must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(posted_message_body(&env).await, MULTILINE_BODY);

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn post_positional_message_unchanged() {
    // AC #4: existing positional form still works (no breaking change).
    let env = TestEnv::new("per-036-post-positional").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id").await;
    env.mock_post_create("post-3").await;

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(&env, &["post", "bravo-team", "plain literal message"]).await;
    assert!(out.status.success(), "positional post must exit 0");
    assert_eq!(posted_message_body(&env).await, "plain literal message");

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// All four verbs wire the resolver (AC #3)
// ----------------------------------------------------------------------

#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn dm_send_message_file_posts_file_contents() {
    let env = TestEnv::new("per-036-dmsend-file").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    mock_dm(&env, "alice", "alice-id", "dm-chan-id").await;
    env.mock_post_create("dm-post-1").await;

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("dm.md");
    std::fs::write(&file, MULTILINE_BODY).unwrap();

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "dm",
            "send",
            "alice",
            "--message-file",
            file.to_str().unwrap(),
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "dm send --message-file must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(posted_message_body(&env).await, MULTILINE_BODY);

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn legacy_dm_message_file_posts_file_contents() {
    // The legacy `dm <user> <msg>` external-subcommand path parses
    // --message-file by hand; prove it wires the resolver.
    let env = TestEnv::new("per-036-dmlegacy-file").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    mock_dm(&env, "bob", "bob-id", "dm-chan-bob").await;
    env.mock_post_create("dm-post-2").await;

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("dm.md");
    std::fs::write(&file, MULTILINE_BODY).unwrap();

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &["dm", "bob", "--message-file", file.to_str().unwrap()],
    )
    .await;
    assert!(
        out.status.success(),
        "legacy dm --message-file must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(posted_message_body(&env).await, MULTILINE_BODY);

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn notify_message_file_includes_file_contents() {
    let env = TestEnv::new("per-036-notify-file").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    // notify posts to the fixed agent-notifications channel on the
    // primary team.
    env.mock_channel_lookup("agent-notifications", "notif-chan-id")
        .await;
    env.mock_post_create("notif-post-1").await;

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("n.md");
    std::fs::write(&file, "deploy finished\nsee logs\n").unwrap();

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "notify",
            "agent-bravo-devrev",
            "--message-file",
            file.to_str().unwrap(),
        ],
    )
    .await;
    assert!(
        out.status.success(),
        "notify --message-file must exit 0; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    // notify prepends "@<bot> **[notify]** "; the file body is included.
    let body = posted_message_body(&env).await;
    assert!(
        body.contains("deploy finished\nsee logs\n"),
        "notify body must include the file contents; got: {body}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// Over-length: deferred to MM, diagnostic normalized (AC #8)
// ----------------------------------------------------------------------

#[tokio::test]
#[ignore = "integration: run via make test-integration"]
async fn over_length_post_normalizes_diagnostic() {
    let env = TestEnv::new("per-036-overlength").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("bravo-team", "chan-id").await;
    // MM rejects the post as too long.
    Mock::given(method("POST"))
        .and(path("/api/v4/posts"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "status_code": 400,
            "id": "api.post.create_post.message_length.app_error",
            "message": "Post message exceeds the maximum permitted length.",
        })))
        .mount(&env.mock)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("big.md");
    let big = "x".repeat(20000);
    std::fs::write(&file, &big).unwrap();

    let daemon = spawn_daemon(&env).await;
    let out = run_chanvoy(
        &env,
        &[
            "post",
            "bravo-team",
            "--message-file",
            file.to_str().unwrap(),
        ],
    )
    .await;
    assert!(!out.status.success(), "over-length post must exit non-zero");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("20000"),
        "diagnostic must name the received character count; got: {stderr}"
    );
    assert!(
        stderr.contains("Posts.MaxPostSize"),
        "diagnostic must point at the server length setting; got: {stderr}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

// ----------------------------------------------------------------------
// CLI-layer rejections (no daemon needed — resolver runs pre-RPC)
// ----------------------------------------------------------------------

#[tokio::test]
async fn post_mutex_positional_and_file_rejected() {
    let env = TestEnv::new("per-036-post-mutex").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(
        &env,
        &[
            "post",
            "bravo-team",
            "literal",
            "--message-file",
            "/tmp/whatever.md",
        ],
    )
    .await;
    assert!(
        !out.status.success(),
        "positional + --message-file must reject"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("more than one message source"),
        "must name the mutex conflict; got: {stderr}"
    );
}

#[tokio::test]
async fn legacy_dm_mutex_positional_and_file_rejected() {
    // The legacy dm manual-parse path must enforce the same mutex.
    let env = TestEnv::new("per-036-dmlegacy-mutex").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(
        &env,
        &["dm", "bob", "hello", "--message-file", "/tmp/whatever.md"],
    )
    .await;
    assert!(
        !out.status.success(),
        "legacy dm positional + --message-file must reject"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("more than one message source"),
        "legacy dm must enforce the mutex; got: {stderr}"
    );
}

#[tokio::test]
async fn legacy_dm_duplicate_message_file_rejected() {
    // devrev PR #38: the manual-parse path must reject a repeated
    // --message-file rather than last-wins.
    let env = TestEnv::new("per-036-dmlegacy-dup").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(
        &env,
        &[
            "dm",
            "bob",
            "--message-file",
            "/tmp/a.md",
            "--message-file",
            "/tmp/b.md",
        ],
    )
    .await;
    assert!(
        !out.status.success(),
        "duplicate --message-file must reject"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("more than once"),
        "must reject the repeated flag; got: {stderr}"
    );
}

#[tokio::test]
async fn post_message_file_non_regular_rejected() {
    // devrev PR #38: a non-regular file (here a directory) is refused
    // before any read, end-to-end through the CLI.
    let env = TestEnv::new("per-036-post-special").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let dir = tempfile::tempdir().unwrap();

    let out = run_chanvoy(
        &env,
        &[
            "post",
            "bravo-team",
            "--message-file",
            dir.path().to_str().unwrap(),
        ],
    )
    .await;
    assert!(
        !out.status.success(),
        "non-regular --message-file must reject"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not a regular file"),
        "must name the non-regular-file cause; got: {stderr}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn post_message_file_symlink_refused() {
    // ADR-0016: a symlinked --message-file is refused end-to-end, even
    // when its target is a valid regular file. Fail closed.
    let env = TestEnv::new("per-036-post-symlink").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("real.md");
    std::fs::write(&target, "legit content\n").unwrap();
    let link = dir.path().join("notes.md");
    std::os::unix::fs::symlink(&target, &link).unwrap();

    let out = run_chanvoy(
        &env,
        &[
            "post",
            "bravo-team",
            "--message-file",
            link.to_str().unwrap(),
        ],
    )
    .await;
    assert!(
        !out.status.success(),
        "symlinked --message-file must reject"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("symlink"),
        "must name the symlink cause; got: {stderr}"
    );
}

#[tokio::test]
async fn notify_no_source_rejected() {
    let env = TestEnv::new("per-036-notify-nosource").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let out = run_chanvoy(&env, &["notify", "agent-bravo-devrev"]).await;
    assert!(
        !out.status.success(),
        "notify with no message body must reject"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("no message body"),
        "must name the no-source error; got: {stderr}"
    );
}

// ----------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------

/// Mocks for a direct message to `username`: user-id lookup + the
/// `POST /channels/direct` that returns the DM channel id. The bot's own
/// id comes from `mock_baseline`'s `/users/me`.
async fn mock_dm(env: &TestEnv, username: &str, user_id: &str, dm_channel_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/users/username/{username}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"id": user_id, "username": username})),
        )
        .mount(&env.mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/channels/direct"))
        .respond_with(
            ResponseTemplate::new(201).set_body_json(serde_json::json!({"id": dm_channel_id})),
        )
        .mount(&env.mock)
        .await;
}
