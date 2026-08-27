//! Hermetic held-wait / JSONL stream proofs.

#![allow(dead_code)]

use std::os::unix::fs::{symlink, PermissionsExt};
use std::process::Stdio;
use std::time::Duration;

mod common;

use chanvoy_core::{
    rpc_error, rpc_request, JsonRpcRequest, WaitFollowEvent, WaitFollowEventKind, WaitFollowMode,
    WaitFollowV1Params, WAIT_FOLLOW_V1_METHOD,
};
use common::{run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

const POST_0: &str = "postid00000000000000000000";
const POST_1: &str = "postid00000000000000000001";
const POST_2: &str = "postid00000000000000000002";

async fn wait_for_armed_file(path: &std::path::Path) -> WaitFollowEvent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if let Ok(raw) = std::fs::read_to_string(path) {
            if let Some((line, _)) = raw.split_once('\n') {
                if let Ok(event) = serde_json::from_str::<WaitFollowEvent>(line) {
                    if event.validate().is_ok() && event.mode() == WaitFollowMode::Armed {
                        return event;
                    }
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "newline-terminated armed record missing"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn fake_old_daemon(env: &TestEnv) -> tokio::task::JoinHandle<()> {
    let listener = UnixListener::bind(env.socket_path()).expect("bind old daemon");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("request");
        let request: JsonRpcRequest = serde_json::from_str(line.trim_end()).expect("decode");
        assert_eq!(request.method, WAIT_FOLLOW_V1_METHOD);
        let response = rpc_error(request.id, -32601, "unknown method wait_follow_v1");
        writer
            .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
            .await
            .expect("response");
    })
}

async fn fake_invalid_event_daemon(env: &TestEnv) -> tokio::task::JoinHandle<()> {
    let listener = UnixListener::bind(env.socket_path()).expect("bind skewed daemon");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("request");
        let request: JsonRpcRequest = serde_json::from_str(line.trim_end()).expect("decode");
        assert_eq!(request.method, WAIT_FOLLOW_V1_METHOD);
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "wait_follow_v1.event",
            "params": {
                "schema": "wait_follow_v1.event",
                "wait_id": "wait_0123456789abcdef0123456789abcdef",
                "mode": "live",
                "tip": POST_2,
                "truncated": false,
                "messages": [{
                    "id": POST_1,
                    "user_id": "userid00000000000000000001",
                    "username": "reviewer-one",
                    "message": "invalid tip lineage",
                    "create_at": 1_700_000_000_001i64,
                    "root_id": POST_1
                }]
            }
        });
        writer
            .write_all(format!("{}\n", serde_json::to_string(&notification).unwrap()).as_bytes())
            .await
            .expect("notification");
    })
}

fn posts_body(channel_id: &str, posts: &[(&str, &str, &str, i64)]) -> serde_json::Value {
    serde_json::json!({
        "posts": posts.iter().map(|(id, user_id, message, create_at)| {
            (
                (*id).to_string(),
                serde_json::json!({
                    "id": id,
                    "channel_id": channel_id,
                    "user_id": user_id,
                    "message": message,
                    "create_at": create_at,
                    "root_id": "",
                }),
            )
        }).collect::<serde_json::Map<_, _>>()
    })
}

async fn mount_after(
    env: &TestEnv,
    channel_id: &str,
    after: &str,
    posts: &[(&str, &str, &str, i64)],
) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/channels/{channel_id}/posts")))
        .and(query_param("after", after))
        .and(query_param("page", "0"))
        .and(query_param("per_page", "200"))
        .respond_with(ResponseTemplate::new(200).set_body_json(posts_body(channel_id, posts)))
        .mount(&env.mock)
        .await;
}

#[tokio::test]
#[ignore = "integration: held wait stream"]
async fn follow_emits_backlog_without_rearming_then_terminal() {
    let env = TestEnv::new("per-043-follow-backlog").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("brief-per-043", "channel-per-043")
        .await;
    env.mock_post_lookup(POST_0, "channel-per-043", true).await;
    env.mock_user_lookup("user-1", "reviewer-one").await;
    env.mock_user_lookup("user-2", "reviewer-two").await;
    mount_after(
        &env,
        "channel-per-043",
        POST_0,
        &[
            (POST_1, "user-1", "first", 1_700_000_000_001),
            (POST_2, "user-2", "second", 1_700_000_000_001),
        ],
    )
    .await;
    mount_after(
        &env,
        "channel-per-043",
        POST_1,
        &[(POST_2, "user-2", "second", 1_700_000_000_001)],
    )
    .await;
    mount_after(&env, "channel-per-043", POST_2, &[]).await;

    let daemon = spawn_daemon(&env).await;
    let out = env.runtime_dir().join("follow.jsonl");
    let output = run_chanvoy(
        &env,
        &[
            "wait",
            "brief-per-043",
            "--follow",
            "--out",
            out.to_str().unwrap(),
            "--after",
            POST_0,
            "--timeout",
            "1s",
        ],
    )
    .await;
    assert_eq!(output.status.code(), Some(1), "{output:?}");

    let raw = std::fs::read_to_string(&out).expect("follow jsonl");
    let events: Vec<WaitFollowEvent> = raw
        .lines()
        .map(|line| serde_json::from_str(line).expect("event json"))
        .collect();
    assert!(events.iter().all(|event| event.validate().is_ok()), "{raw}");
    assert_eq!(events.len(), 4, "{raw}");
    assert_eq!(events[0].mode(), WaitFollowMode::Armed);
    assert!(events[0].messages().is_empty());
    assert_eq!(events[1].mode(), WaitFollowMode::Backlog);
    assert_eq!(events[1].messages()[0].id, POST_1);
    assert_eq!(events[1].tip(), Some(POST_1));
    assert_eq!(events[2].mode(), WaitFollowMode::Backlog);
    assert_eq!(events[2].messages()[0].id, POST_2);
    assert_eq!(events[2].tip(), Some(POST_2));
    assert_eq!(events[3].mode(), WaitFollowMode::Deadman);
    assert!(events[3].tip().is_none());
    let wait_id = &events[0].wait_id;
    assert!(events.iter().all(|event| &event.wait_id == wait_id));
    assert_eq!(
        std::fs::metadata(&out).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: held wait sink preflight"]
async fn follow_refuses_symlink_sink_before_daemon_admission() {
    let env = TestEnv::new("per-043-follow-symlink").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let target = env.runtime_dir().join("target.jsonl");
    std::fs::write(&target, b"sentinel\n").unwrap();
    let link = env.runtime_dir().join("follow.jsonl");
    symlink(&target, &link).unwrap();

    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-043",
            "--follow",
            "--out",
            link.to_str().unwrap(),
            "--timeout",
            "1s",
        ],
    )
    .await;
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error_class"], "sink");
    assert!(value["message"].as_str().unwrap().contains("symlink"));
    assert_eq!(std::fs::read(&target).unwrap(), b"sentinel\n");
}

#[tokio::test]
#[ignore = "integration: held wait sink permissions"]
async fn follow_refuses_existing_sink_that_is_not_mode_0600() {
    let env = TestEnv::new("per-043-follow-mode").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let out = env.runtime_dir().join("follow.jsonl");
    std::fs::write(&out, b"sentinel\n").unwrap();
    std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o640)).unwrap();
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-043",
            "--follow",
            "--out",
            out.to_str().unwrap(),
            "--timeout",
            "1s",
        ],
    )
    .await;
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error_class"], "sink");
    assert_eq!(std::fs::read(&out).unwrap(), b"sentinel\n");
}

#[tokio::test]
#[ignore = "integration: held wait CLI input"]
async fn bare_follow_without_explicit_sink_is_exit_two() {
    let env = TestEnv::new("per-043-follow-no-sink").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-043",
            "--follow",
            "--timeout",
            "1s",
        ],
    )
    .await;
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error_class"], "sink");
}

#[tokio::test]
#[ignore = "integration: held wait capability skew"]
async fn old_daemon_is_hard_capability_without_fallback() {
    let env = TestEnv::new("per-043-follow-capability").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let server = fake_old_daemon(&env).await;
    let out = env.runtime_dir().join("capability.jsonl");
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-043",
            "--follow",
            "--out",
            out.to_str().unwrap(),
            "--timeout",
            "1s",
        ],
    )
    .await;
    server.await.expect("old daemon");
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error_class"], "capability");
    assert!(!value["timeout"].as_bool().unwrap());
    assert!(std::fs::read_to_string(out).unwrap().is_empty());
}

#[tokio::test]
#[ignore = "integration: held wait skewed event validation"]
async fn skewed_invalid_event_is_refused_before_sink_output() {
    let env = TestEnv::new("per-043-follow-invalid-event").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let server = fake_invalid_event_daemon(&env).await;
    let out = env.runtime_dir().join("invalid-event.jsonl");
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-043",
            "--follow",
            "--out",
            out.to_str().unwrap(),
            "--timeout",
            "1s",
        ],
    )
    .await;
    server.await.expect("skewed daemon");
    assert_eq!(output.status.code(), Some(2));
    assert!(std::fs::read_to_string(out).unwrap().is_empty());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["timeout"], false);
    assert!(value["message"]
        .as_str()
        .unwrap_or_default()
        .contains("invalid held wait stream record"));
}

#[tokio::test]
#[ignore = "integration: held wait client EOF"]
async fn client_eof_releases_the_held_owner() {
    let env = TestEnv::new("per-043-follow-eof").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("brief-per-043", "channel-per-043-eof")
        .await;
    env.mock_channel_posts("channel-per-043-eof", &[]).await;
    let daemon = spawn_daemon(&env).await;

    let mut stream = UnixStream::connect(env.socket_path())
        .await
        .expect("connect");
    let request = rpc_request(
        WAIT_FOLLOW_V1_METHOD,
        serde_json::to_value(WaitFollowV1Params {
            channel: "brief-per-043".into(),
            timeout_secs: 30,
            team: None,
            contains: None,
            pattern: None,
            after: None,
            replace_wait_id: None,
        })
        .unwrap(),
    );
    stream
        .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
        .await
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let notification: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
    assert_eq!(notification["params"]["mode"], "armed");
    drop(reader);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let output = run_chanvoy(
            &env,
            &["--json", "wait", "brief-per-043", "--timeout", "1s"],
        )
        .await;
        if output.status.code() == Some(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "held owner not released after client EOF: {output:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: held wait SIGINT"]
async fn sigint_writes_canceled_and_releases_the_held_owner() {
    let env = TestEnv::new("per-043-follow-sigint").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("brief-per-043", "channel-per-043-sigint")
        .await;
    env.mock_channel_posts("channel-per-043-sigint", &[]).await;
    let daemon = spawn_daemon(&env).await;
    let out = env.runtime_dir().join("sigint.jsonl");
    let follow = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "wait",
            "brief-per-043",
            "--follow",
            "--out",
            out.to_str().unwrap(),
            "--timeout",
            "30s",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn follow");
    let pid = follow.id().expect("pid") as libc::pid_t;
    wait_for_armed_file(&out).await;
    assert_eq!(unsafe { libc::kill(pid, libc::SIGINT) }, 0);
    let output = tokio::time::timeout(Duration::from_secs(5), follow.wait_with_output())
        .await
        .expect("SIGINT exit")
        .expect("follow output");
    assert_eq!(output.status.code(), Some(130), "{output:?}");
    let raw = std::fs::read_to_string(&out).unwrap();
    let events: Vec<WaitFollowEvent> = raw
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(events.iter().all(|event| event.validate().is_ok()), "{raw}");
    assert_eq!(events.last().unwrap().mode(), WaitFollowMode::Canceled);

    let output = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-043", "--timeout", "1s"],
    )
    .await;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: held wait mid-stream sink failure"]
async fn broken_stdout_sink_exits_two_and_releases_the_held_owner() {
    let env = TestEnv::new("per-043-follow-broken-stdout").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("brief-per-043", "channel-per-043-broken")
        .await;
    env.mock_channel_posts("channel-per-043-broken", &[]).await;
    let daemon = spawn_daemon(&env).await;
    let mut follow = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "wait",
            "brief-per-043",
            "--follow",
            "--follow-stdout",
            "--timeout",
            "30s",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn follow");
    let stdout = follow.stdout.take().expect("stdout");
    let mut reader = BufReader::new(stdout);
    let mut armed = String::new();
    reader.read_line(&mut armed).await.unwrap();
    let event: WaitFollowEvent = serde_json::from_str(armed.trim_end()).unwrap();
    assert_eq!(event.mode(), WaitFollowMode::Armed);
    drop(reader);

    env.mock.reset().await;
    env.mock_channel_posts(
        "channel-per-043-broken",
        &[(POST_1, "user-1", "reviewer-one", "wake", 1_700_000_000_001)],
    )
    .await;
    let output = tokio::time::timeout(Duration::from_secs(6), follow.wait_with_output())
        .await
        .expect("sink failure exit")
        .expect("follow output");
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("sink"),
        "{output:?}"
    );

    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("brief-per-043", "channel-per-043-broken")
        .await;
    let output = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-043", "--timeout", "1s"],
    )
    .await;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: held wait failed terminal"]
async fn provider_failure_writes_failed_terminal_before_exit_two() {
    let env = TestEnv::new("per-043-follow-failed").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("brief-per-043", "channel-per-043-failed")
        .await;
    env.mock_channel_posts("channel-per-043-failed", &[]).await;
    let daemon = spawn_daemon(&env).await;
    let out = env.runtime_dir().join("failed.jsonl");
    let follow = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "wait",
            "brief-per-043",
            "--follow",
            "--out",
            out.to_str().unwrap(),
            "--timeout",
            "30s",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn follow");
    wait_for_armed_file(&out).await;
    env.mock.reset().await;
    Mock::given(method("GET"))
        .and(path("/api/v4/channels/channel-per-043-failed/posts"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&env.mock)
        .await;
    let output = tokio::time::timeout(Duration::from_secs(6), follow.wait_with_output())
        .await
        .expect("failed terminal exit")
        .expect("follow output");
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let raw = std::fs::read_to_string(&out).unwrap();
    let events: Vec<WaitFollowEvent> = raw
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.last().unwrap().mode(), WaitFollowMode::Failed);
    assert!(events.iter().all(|event| event.validate().is_ok()), "{raw}");
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: held wait ownership"]
async fn replacing_follow_writes_terminal_line_and_releases_owner() {
    let env = TestEnv::new("per-043-follow-replace").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("brief-per-043", "channel-per-043-replace")
        .await;
    env.mock_channel_posts("channel-per-043-replace", &[]).await;
    let daemon = spawn_daemon(&env).await;
    let out = env.runtime_dir().join("replace.jsonl");

    let follow = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "wait",
            "brief-per-043",
            "--follow",
            "--out",
            out.to_str().unwrap(),
            "--timeout",
            "30s",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn follow");

    let wait_id = wait_for_armed_file(&out).await.wait_id;

    let replacement_out = env.runtime_dir().join("replacement.jsonl");
    let replacement = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "wait",
            "brief-per-043",
            "--follow",
            "--out",
            replacement_out.to_str().unwrap(),
            "--timeout",
            "1s",
            "--replace-wait",
            &wait_id,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn replacement");

    let follow_output = tokio::time::timeout(Duration::from_secs(5), follow.wait_with_output())
        .await
        .expect("follow exits after replacement")
        .expect("follow output");
    assert_eq!(follow_output.status.code(), Some(2));
    let raw = std::fs::read_to_string(&out).unwrap();
    let events: Vec<WaitFollowEvent> = raw
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(events.iter().all(|event| event.validate().is_ok()), "{raw}");
    assert_eq!(events.last().unwrap().mode(), WaitFollowMode::Replaced);
    assert!(events.last().unwrap().messages().is_empty());
    assert!(matches!(
        &events.last().unwrap().kind,
        WaitFollowEventKind::Replaced {
            replaced_by_wait_id
        } if !replaced_by_wait_id.is_empty()
    ));

    let replacement_output =
        tokio::time::timeout(Duration::from_secs(5), replacement.wait_with_output())
            .await
            .expect("replacement deadman")
            .expect("replacement output");
    assert_eq!(replacement_output.status.code(), Some(1));
    let replacement_raw = std::fs::read_to_string(replacement_out).unwrap();
    let replacement_events: Vec<WaitFollowEvent> = replacement_raw
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        replacement_events
            .iter()
            .all(|event| event.validate().is_ok()),
        "{replacement_raw}"
    );
    assert!(matches!(
        &replacement_events[0].kind,
        WaitFollowEventKind::Armed {
            replaced_wait_id: Some(replaced)
        } if replaced == &wait_id
    ));
    assert!(stop_daemon_cleanly(&env, daemon).await);
}
