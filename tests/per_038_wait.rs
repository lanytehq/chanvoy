//! PER-038 process and protocol-boundary evidence.
//!
//! These cases deliberately run through a real `chanvoy` child process. The
//! fake-daemon rows model an old JSON-RPC peer without requiring a second
//! checkout, while the real-daemon rows cover the new wait engine and the
//! legacy `wait_channel` wire method it still serves.

#![allow(dead_code)]

use std::path::PathBuf;

mod common;

use chanvoy_core::{rpc_error, rpc_request, JsonRpcRequest, JsonRpcResponse};
use common::{run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;

#[derive(Clone, Copy)]
enum FakeReply {
    UnknownMethod,
    Input,
    Timeout,
}

/// Bind the profile socket and answer a finite sequence of JSON-RPC calls.
/// A method list makes the skew rows assert the actual wire shape rather than
/// merely checking the CLI's final status.
async fn fake_daemon(env: &TestEnv, expected: Vec<(&'static str, FakeReply)>) -> JoinHandle<()> {
    let socket_path = env.socket_path();
    let listener = UnixListener::bind(&socket_path).expect("bind fake daemon socket");
    let path = socket_path.to_path_buf();
    tokio::spawn(async move {
        for (method, reply) in expected {
            let (stream, _) = listener.accept().await.expect("accept fake daemon client");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .await
                .expect("read fake daemon request");
            let request: JsonRpcRequest =
                serde_json::from_str(line.trim_end()).expect("decode fake daemon request");
            assert_eq!(request.method, method, "unexpected JSON-RPC method");
            let response = match reply {
                FakeReply::UnknownMethod => rpc_error(request.id, -32601, "unknown method"),
                FakeReply::Input => {
                    rpc_error(request.id, -32007, "contains exceeds 256 UTF-8 bytes")
                }
                FakeReply::Timeout => rpc_error(request.id, -32005, "no matching messages"),
            };
            writer
                .write_all(
                    format!(
                        "{}\n",
                        serde_json::to_string(&response).expect("encode fake daemon response")
                    )
                    .as_bytes(),
                )
                .await
                .expect("write fake daemon response");
        }
        let _ = std::fs::remove_file(path);
    })
}

async fn raw_rpc(socket_path: PathBuf, method: &str, params: serde_json::Value) -> JsonRpcResponse {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .expect("connect real daemon for legacy RPC");
    let request = rpc_request(method, params);
    stream
        .write_all(
            format!(
                "{}\n",
                serde_json::to_string(&request).expect("encode legacy RPC request")
            )
            .as_bytes(),
        )
        .await
        .expect("write legacy RPC request");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .expect("read legacy RPC response");
    serde_json::from_str(line.trim_end()).expect("decode legacy RPC response")
}

async fn mount_wait_channel(
    env: &TestEnv,
    channel: &str,
    channel_id: &str,
    anchor: Option<&str>,
    posts: &[(&str, &str, &str, &str, i64)],
) {
    env.mock_baseline("bot-id-per038", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(channel, channel_id).await;
    if let Some(anchor) = anchor {
        env.mock_post_lookup(anchor, channel_id, true).await;
    }
    env.mock_channel_posts(channel_id, posts).await;
}

/// AC-W0 + AC-W1: a real process returns one JSON payload on match, and a
/// real process returns structured timeout JSON with exit 1 on clean silence.
#[tokio::test]
#[ignore = "integration: PER-038 process outcome matrix"]
async fn wait_process_match_and_deadman_shapes() {
    let env = TestEnv::new("per-038-process-match").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_wait_channel(
        &env,
        "brief-per-038",
        "chan-id-per038-match",
        Some("anchor-match"),
        &[(
            "match-post",
            "reviewer-id",
            "reviewer",
            "ASSENT: exact tip is green",
            1_780_000_000_100,
        )],
    )
    .await;
    let daemon = spawn_daemon(&env).await;

    let matched = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-038",
            "--timeout",
            "5s",
            "--contains",
            "ASSENT",
            "--after",
            "anchor-match",
        ],
    )
    .await;
    assert_eq!(matched.status.code(), Some(0), "match must exit 0");
    let matched_json: serde_json::Value =
        serde_json::from_slice(&matched.stdout).expect("match JSON payload");
    assert_eq!(matched_json["channel"], "brief-per-038");
    assert_eq!(matched_json["messages"][0]["id"], "match-post");
    assert_eq!(
        matched_json["messages"][0]["message"],
        "ASSENT: exact tip is green"
    );
    assert!(matched_json.get("timeout").is_none());
    assert!(String::from_utf8_lossy(&matched.stderr).is_empty());
    assert!(stop_daemon_cleanly(&env, daemon).await);

    let env = TestEnv::new("per-038-process-deadman").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_wait_channel(
        &env,
        "brief-per-038",
        "chan-id-per038-deadman",
        Some("anchor-deadman"),
        &[],
    )
    .await;
    let daemon = spawn_daemon(&env).await;
    let timed_out = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-038",
            "--timeout",
            "1s",
            "--contains",
            "ASSENT",
            "--after",
            "anchor-deadman",
        ],
    )
    .await;
    assert_eq!(
        timed_out.status.code(),
        Some(1),
        "clean deadman must exit 1"
    );
    let timeout_json: serde_json::Value =
        serde_json::from_slice(&timed_out.stdout).expect("timeout JSON payload");
    assert_eq!(timeout_json["timeout"], true);
    assert!(timeout_json.get("error_class").is_none());
    assert!(String::from_utf8_lossy(&timed_out.stderr).is_empty());
    assert!(stop_daemon_cleanly(&env, daemon).await);

    let env = TestEnv::new("per-038-process-human").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_wait_channel(
        &env,
        "brief-per-038",
        "chan-id-per038-human",
        Some("anchor-human"),
        &[(
            "human-match",
            "reviewer-id",
            "reviewer",
            "ASSENT: human payload",
            1_780_000_000_200,
        )],
    )
    .await;
    let daemon = spawn_daemon(&env).await;
    let human = run_chanvoy(
        &env,
        &[
            "wait",
            "brief-per-038",
            "--timeout",
            "5s",
            "--contains",
            "ASSENT",
            "--after",
            "anchor-human",
        ],
    )
    .await;
    assert_eq!(human.status.code(), Some(0), "human match must exit 0");
    let stdout = String::from_utf8_lossy(&human.stdout);
    let stderr = String::from_utf8_lossy(&human.stderr);
    assert!(
        stdout.contains("id=human-match"),
        "human output has id: {stdout}"
    );
    assert!(
        stdout.contains("ASSENT: human payload"),
        "human output has body: {stdout}"
    );
    assert!(
        stderr.contains("waiting for new message"),
        "human stderr has progress: {stderr}"
    );
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

/// AC-W2: a hard input refusal is process-visible as exit 2, JSON with
/// `timeout:false`, and no misleading timeout payload.
#[tokio::test]
#[ignore = "integration: PER-038 process outcome matrix"]
async fn wait_process_hard_input_is_exit_two() {
    let env = TestEnv::new("per-038-process-hard").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let over_limit = "x".repeat(257);
    let server = fake_daemon(&env, vec![("wait_channel_v2", FakeReply::Input)]).await;
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-038",
            "--timeout",
            "5s",
            "--contains",
            &over_limit,
        ],
    )
    .await;
    server.await.expect("fake daemon completed");
    assert_eq!(output.status.code(), Some(2), "hard input must exit 2");
    let error_json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("hard-error JSON payload");
    assert_eq!(error_json["timeout"], false);
    assert_eq!(error_json["error_class"], "input");
    assert!(error_json["message"]
        .as_str()
        .is_some_and(|message| message.contains("256")));
    assert!(String::from_utf8_lossy(&output.stderr).is_empty());
}

/// New CLI → old daemon, advanced wait: the CLI must refuse rather than
/// silently falling back to legacy semantics that cannot honor the filter.
#[tokio::test]
#[ignore = "integration: PER-038 protocol skew matrix"]
async fn wait_skew_new_cli_advanced_old_daemon_is_hard_input() {
    let env = TestEnv::new("per-038-skew-advanced-old").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let server = fake_daemon(&env, vec![("wait_channel_v2", FakeReply::UnknownMethod)]).await;
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-038",
            "--timeout",
            "1s",
            "--contains",
            "ASSENT",
        ],
    )
    .await;
    server.await.expect("fake old daemon completed");
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["timeout"], false);
    assert_eq!(value["error_class"], "input");
    assert!(value["message"]
        .as_str()
        .is_some_and(|message| message.contains("does not support filtered wait")));
}

/// New CLI → old daemon, bare wait: method-not-found may use the legacy
/// method, and a legacy timeout remains the clean exit-1 contract.
#[tokio::test]
#[ignore = "integration: PER-038 protocol skew matrix"]
async fn wait_skew_new_cli_bare_old_daemon_uses_legacy_fallback() {
    let env = TestEnv::new("per-038-skew-bare-old").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let server = fake_daemon(
        &env,
        vec![
            ("wait_channel_v2", FakeReply::UnknownMethod),
            ("wait_channel", FakeReply::Timeout),
        ],
    )
    .await;
    let output = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-038", "--timeout", "1s"],
    )
    .await;
    server.await.expect("fake old daemon completed");
    assert_eq!(output.status.code(), Some(1));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["timeout"], true);
}

/// Old CLI → new daemon: the legacy RPC remains accepted by the new daemon.
/// The empty mocked channel supplies a successful REST observation before the
/// three-second legacy deadman expires, proving a clean -32005 timeout.
#[tokio::test]
#[ignore = "integration: PER-038 protocol skew matrix"]
async fn wait_skew_legacy_rpc_new_daemon_remains_compatible() {
    let env = TestEnv::new("per-038-skew-legacy-new").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_wait_channel(&env, "brief-per-038", "chan-id-per038-legacy", None, &[]).await;
    let daemon = spawn_daemon(&env).await;
    let response = raw_rpc(
        env.socket_path(),
        "wait_channel",
        json!({
            "channel": "brief-per-038",
            "timeout_minutes": 1,
            "timeout_secs": 3,
            "team": null
        }),
    )
    .await;
    assert_eq!(
        response.error.as_ref().map(|error| error.code),
        Some(-32005)
    );
    assert!(response.result.is_none());
    assert!(stop_daemon_cleanly(&env, daemon).await);
}
