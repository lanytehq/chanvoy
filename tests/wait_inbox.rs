//! Hermetic `wait --inbox` proofs.

#![allow(dead_code)]

use std::process::Stdio;
use std::time::Duration;

mod common;

use chanvoy_core::{
    canonical_dm_name, rpc_error, rpc_request, InboxCursorV1, JsonRpcRequest, JsonRpcResponse,
    Message, WaitFollowMode, WaitInboxFollowEvent, POST_ID_NOT_INBOX_CURSOR,
    WAIT_INBOX_FOLLOW_V1_EVENT_METHOD, WAIT_INBOX_FOLLOW_V1_METHOD, WAIT_INBOX_FOLLOW_V2_METHOD,
    WAIT_INBOX_HELP, WAIT_INBOX_V1_METHOD,
};
use common::{run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

const BOT_ID: &str = "userid00000000000000000000";
const BOT_USER: &str = "agent-bravo-devlead";
const PEER_A_ID: &str = "userid00000000000000000001";
const PEER_A: &str = "dave-3leaps";
const PEER_B_ID: &str = "userid00000000000000000002";
const PEER_B: &str = "agent-cxotech";
const POST_A: &str = "postid0000000000000000000a";
const POST_B: &str = "postid0000000000000000000b";
const POST_SELF: &str = "postid0000000000000000000s";
const DM_A: &str = "dmid000000000000000000000a";
const DM_B: &str = "dmid000000000000000000000b";

fn dm_a() -> String {
    canonical_dm_name(BOT_ID, PEER_A_ID)
}

fn dm_b() -> String {
    canonical_dm_name(BOT_ID, PEER_B_ID)
}

async fn raw_rpc(
    socket_path: std::path::PathBuf,
    method: &str,
    params: serde_json::Value,
) -> JsonRpcResponse {
    let mut stream = UnixStream::connect(socket_path).await.expect("connect");
    let request = rpc_request(method, params);
    stream
        .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
        .await
        .expect("write");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read");
    serde_json::from_str(line.trim_end()).expect("decode")
}

async fn fake_old_daemon(env: &TestEnv, method_name: &'static str) -> JoinHandle<()> {
    let listener = UnixListener::bind(env.socket_path()).expect("bind old daemon");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("request");
        let request: JsonRpcRequest = serde_json::from_str(line.trim_end()).expect("decode");
        assert_eq!(request.method, method_name);
        let response = rpc_error(request.id, -32601, format!("unknown method {method_name}"));
        writer
            .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
            .await
            .expect("response");
    })
}

async fn mount_catalog(env: &TestEnv) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {
                "id": DM_A,
                "name": dm_a(),
                "type": "D",
                "last_post_at": 1_780_000_000_100i64
            },
            {
                "id": DM_B,
                "name": dm_b(),
                "type": "D",
                "last_post_at": 1_780_000_000_050i64
            },
            {
                "id": "public00000000000000000001",
                "name": "town-square",
                "type": "O",
                "last_post_at": 1
            }
        ])))
        .mount(&env.mock)
        .await;
    env.mock_user_lookup(PEER_A_ID, PEER_A).await;
    env.mock_user_lookup(PEER_B_ID, PEER_B).await;
}

#[tokio::test]
#[ignore = "integration: wait --inbox CLI refuse shapes"]
async fn cli_refuses_post_id_after_and_selector_mixes_before_provider() {
    let env = TestEnv::new("wait-inbox-cli-refuse").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");

    let post_id = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--inbox",
            "--after",
            POST_A,
            "--timeout",
            "1s",
        ],
    )
    .await;
    assert_eq!(post_id.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&post_id.stdout).unwrap();
    assert_eq!(value["error_class"], "input");
    assert!(value["message"]
        .as_str()
        .unwrap_or_default()
        .contains(POST_ID_NOT_INBOX_CURSOR));

    for args in [
        vec![
            "--json",
            "wait",
            "--inbox",
            "--dm",
            PEER_A,
            "--timeout",
            "1s",
        ],
        vec![
            "--json",
            "wait",
            "--inbox",
            "town-square",
            "--timeout",
            "1s",
        ],
        vec![
            "--json",
            "wait",
            "--inbox",
            "--team",
            "org-lanytehq",
            "--timeout",
            "1s",
        ],
    ] {
        let output = run_chanvoy(&env, &args).await;
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["error_class"], "input");
    }
}

#[tokio::test]
#[ignore = "integration: wait --inbox capability skew"]
async fn old_daemon_is_hard_capability() {
    let env = TestEnv::new("wait-inbox-cap").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    let _server = fake_old_daemon(&env, WAIT_INBOX_V1_METHOD).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let output = run_chanvoy(&env, &["--json", "wait", "--inbox", "--timeout", "1s"]).await;
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error_class"], "capability");
    assert!(value["message"]
        .as_str()
        .unwrap_or_default()
        .contains(WAIT_INBOX_V1_METHOD));
}

#[tokio::test]
#[ignore = "integration: wait --inbox follow capability skew"]
async fn old_follow_daemon_is_hard_capability() {
    let env = TestEnv::new("wait-inbox-follow-cap").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    let _server = fake_old_daemon(&env, WAIT_INBOX_FOLLOW_V1_METHOD).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--inbox",
            "--timeout",
            "1s",
            "--follow",
            "--follow-stdout",
        ],
    )
    .await;
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error_class"], "capability");
}

#[tokio::test]
#[ignore = "integration: wait --inbox follow coalesce capability skew"]
async fn old_coalesce_follow_daemon_is_hard_capability() {
    let env = TestEnv::new("wait-inbox-follow-coalesce-cap").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    let _server = fake_old_daemon(&env, WAIT_INBOX_FOLLOW_V2_METHOD).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--inbox",
            "--timeout",
            "1s",
            "--follow",
            "--follow-stdout",
            "--coalesce",
            "5s",
        ],
    )
    .await;
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error_class"], "capability");
}

#[tokio::test]
#[ignore = "integration: wait --inbox refuses disconnected WS before catalog"]
async fn disconnected_ws_refuses_before_catalog() {
    let env = TestEnv::new("wait-inbox-ws-admit").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&env.mock)
        .await;
    let daemon = spawn_daemon(&env).await;
    let output = run_chanvoy(&env, &["--json", "wait", "--inbox", "--timeout", "2s"]).await;
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["timeout"], false);
    assert_eq!(value["error_class"], "provider");
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: wait --inbox cursor validated before provider I/O"]
async fn bad_cursors_do_not_touch_catalog() {
    let env = TestEnv::new("wait-inbox-cursor-pre-io").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&env.mock)
        .await;
    let daemon = spawn_daemon(&env).await;
    let foreign = InboxCursorV1::empty("other-profile", BOT_ID)
        .encode()
        .unwrap();
    for after in ["inv1.not-valid-base64", foreign.as_str(), "inv1."] {
        let output = run_chanvoy(
            &env,
            &[
                "--json",
                "wait",
                "--inbox",
                "--after",
                after,
                "--timeout",
                "1s",
            ],
        )
        .await;
        assert_eq!(output.status.code(), Some(2), "{after}");
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["timeout"], false);
    }
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[test]
fn help_names_inbox_without_channel_id() {
    let output = std::process::Command::new(common::CHANVOY_BIN)
        .args(["wait", "--help"])
        .output()
        .expect("wait help");
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains(WAIT_INBOX_HELP), "{help}");
    assert!(!help.contains("1024"), "caps must not be help SLAs: {help}");
}

fn sample_cursor(post_id: &str, ts: i64) -> String {
    InboxCursorV1::empty("wait-inbox-follow-cli", BOT_ID)
        .advance(ts, post_id)
        .unwrap()
        .encode()
        .unwrap()
}

fn sample_live(wait_id: &str, post_id: &str, cursor: &str) -> WaitInboxFollowEvent {
    WaitInboxFollowEvent::message(
        wait_id,
        WaitFollowMode::Live,
        PEER_A.to_string(),
        dm_a(),
        cursor.to_string(),
        Message {
            id: post_id.into(),
            user_id: PEER_A_ID.into(),
            username: PEER_A.into(),
            message: "hello".into(),
            create_at: 1_780_000_000_100,
            root_id: post_id.into(),
            mention_user_ids: None,
        },
        false,
    )
    .expect("live inbox record")
}

async fn fake_inbox_follow_daemon(
    env: &TestEnv,
    events: Vec<WaitInboxFollowEvent>,
    pause_before_last: Option<Duration>,
    hang: bool,
) -> JoinHandle<()> {
    let listener = UnixListener::bind(env.socket_path()).expect("bind follow daemon");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("request");
        let request: JsonRpcRequest = serde_json::from_str(line.trim_end()).expect("decode");
        assert_eq!(request.method, WAIT_INBOX_FOLLOW_V1_METHOD);
        let last = events.len().saturating_sub(1);
        for (index, event) in events.into_iter().enumerate() {
            if index == last {
                if let Some(delay) = pause_before_last {
                    tokio::time::sleep(delay).await;
                }
            }
            let notification = json!({
                "jsonrpc": "2.0",
                "method": WAIT_INBOX_FOLLOW_V1_EVENT_METHOD,
                "params": event,
            });
            writer
                .write_all(
                    format!("{}\n", serde_json::to_string(&notification).unwrap()).as_bytes(),
                )
                .await
                .expect("event");
            writer.flush().await.expect("flush");
        }
        if hang {
            let mut buf = String::new();
            let _ = reader.read_line(&mut buf).await;
        }
    })
}

async fn wait_for_inbox_mode(path: &std::path::Path, mode: WaitFollowMode) -> WaitInboxFollowEvent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if let Ok(raw) = std::fs::read_to_string(path) {
            for line in raw.lines() {
                if let Ok(event) = serde_json::from_str::<WaitInboxFollowEvent>(line) {
                    if event.validate().is_ok() && event.mode() == mode {
                        return event;
                    }
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "missing {mode:?} inbox follow record"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
#[ignore = "integration: wait --inbox follow SIGINT preserves sink-acked cursor"]
async fn sigint_after_one_inbox_line_keeps_acked_cursor() {
    let env = TestEnv::new("wait-inbox-sigint").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    let cursor = sample_cursor(POST_A, 1_780_000_000_100);
    let wait_id = "wait_inbox_sigint_0000000000000001";
    let _server = fake_inbox_follow_daemon(
        &env,
        vec![
            WaitInboxFollowEvent::armed(wait_id, None),
            sample_live(wait_id, POST_A, &cursor),
        ],
        None,
        true,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let out = env.runtime_dir().join("inbox-sigint.jsonl");
    let follow = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "wait",
            "--inbox",
            "--follow",
            "--out",
            out.to_str().unwrap(),
            "--timeout",
            "30s",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn inbox follow");
    let pid = follow.id().expect("pid") as libc::pid_t;
    let live = wait_for_inbox_mode(&out, WaitFollowMode::Live).await;
    assert_eq!(live.inbox_cursor(), Some(cursor.as_str()));
    assert_eq!(unsafe { libc::kill(pid, libc::SIGINT) }, 0);
    let output = tokio::time::timeout(Duration::from_secs(5), follow.wait_with_output())
        .await
        .expect("SIGINT exit")
        .expect("follow output");
    assert_eq!(output.status.code(), Some(130), "{output:?}");
    let raw = std::fs::read_to_string(&out).unwrap();
    let events: Vec<WaitInboxFollowEvent> = raw
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(events.iter().all(|event| event.validate().is_ok()), "{raw}");
    let last = events.last().unwrap();
    assert_eq!(last.mode(), WaitFollowMode::Canceled);
    assert_eq!(last.inbox_cursor(), Some(cursor.as_str()));
}

#[tokio::test]
#[ignore = "integration: wait --inbox follow sink failure after one line"]
async fn broken_stdout_after_one_inbox_line_exits_two() {
    let env = TestEnv::new("wait-inbox-sink").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    let first = sample_cursor(POST_A, 1_780_000_000_100);
    let second = sample_cursor(POST_B, 1_780_000_000_200);
    let wait_id = "wait_inbox_sink_00000000000000001";
    let _server = fake_inbox_follow_daemon(
        &env,
        vec![
            WaitInboxFollowEvent::armed(wait_id, None),
            sample_live(wait_id, POST_A, &first),
            sample_live(wait_id, POST_B, &second),
        ],
        Some(Duration::from_millis(250)),
        true,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut follow = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "wait",
            "--inbox",
            "--follow",
            "--follow-stdout",
            "--timeout",
            "30s",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn inbox follow");
    let stdout = follow.stdout.take().expect("stdout");
    let mut reader = BufReader::new(stdout);
    let mut armed = String::new();
    reader.read_line(&mut armed).await.unwrap();
    let event: WaitInboxFollowEvent = serde_json::from_str(armed.trim_end()).unwrap();
    assert_eq!(event.mode(), WaitFollowMode::Armed);
    let mut live = String::new();
    reader.read_line(&mut live).await.unwrap();
    let event: WaitInboxFollowEvent = serde_json::from_str(live.trim_end()).unwrap();
    assert_eq!(event.mode(), WaitFollowMode::Live);
    assert_eq!(event.inbox_cursor(), Some(first.as_str()));
    drop(reader);
    let output = tokio::time::timeout(Duration::from_secs(6), follow.wait_with_output())
        .await
        .expect("sink failure exit")
        .expect("follow output");
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("sink"),
        "{output:?}"
    );
}
