//! Hermetic `wait --inbox` proofs.

#![allow(dead_code)]

use std::time::Duration;

mod common;

use chanvoy_core::{
    canonical_dm_name, rpc_error, rpc_request, InboxCursorV1, JsonRpcRequest, JsonRpcResponse,
    POST_ID_NOT_INBOX_CURSOR, WAIT_INBOX_FOLLOW_V1_METHOD, WAIT_INBOX_HELP, WAIT_INBOX_V1_METHOD,
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
