//! Hermetic PER-041 MCP face (stdio pipes against a fake daemon).

#![allow(dead_code)]

use std::time::Duration;

mod common;

use chanvoy_core::{rpc_error, rpc_result, JsonRpcRequest, WAIT_CHANNEL_V3_METHOD};
use common::{run_chanvoy_with_stdin, TestEnv};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

async fn fake_daemon_ok(env: &TestEnv, method: &'static str, result: serde_json::Value) {
    let socket_path = env.socket_path();
    let listener = UnixListener::bind(&socket_path).expect("bind fake daemon");
    let path = socket_path.clone();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("read");
        let request: JsonRpcRequest = serde_json::from_str(line.trim_end()).expect("decode");
        assert_eq!(request.method, method);
        let response = rpc_result(request.id, result);
        writer
            .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
            .await
            .expect("write");
        let _ = std::fs::remove_file(path);
    });
}

async fn fake_daemon_unknown(env: &TestEnv, method: &'static str) {
    let socket_path = env.socket_path();
    let listener = UnixListener::bind(&socket_path).expect("bind fake daemon");
    let path = socket_path.clone();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("read");
        let request: JsonRpcRequest = serde_json::from_str(line.trim_end()).expect("decode");
        assert_eq!(request.method, method);
        let response = rpc_error(request.id, -32601, "unknown method");
        writer
            .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
            .await
            .expect("write");
        let _ = std::fs::remove_file(path);
    });
}

fn rpc_line(id: u64, method: &str, params: serde_json::Value) -> String {
    format!(
        "{}\n",
        serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .unwrap()
    )
}

fn parse_stdout_json(stdout: &[u8]) -> serde_json::Value {
    let text = String::from_utf8_lossy(stdout);
    let line = text.lines().next().expect("one protocol line");
    serde_json::from_str(line).expect("stdout is JSON-RPC")
}

#[tokio::test]
async fn mcp_stdio_tools_list_is_deterministic_and_stdout_pure() {
    let env = TestEnv::new("per-041-list").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let output = run_chanvoy_with_stdin(
        &env,
        &["mcp"],
        rpc_line(1, "tools/list", json!({})).as_bytes(),
    )
    .await;
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value = parse_stdout_json(&output.stdout);
    let names: Vec<&str> = value["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        ["whoami", "read_channel", "show", "thread", "wait", "post"]
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("test-token-value"));
    assert!(!stdout.contains(&env.server_url()));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("test-token-value"));
}

#[tokio::test]
async fn mcp_stdio_whoami_dispatches_daemon() {
    let env = TestEnv::new("per-041-whoami").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    fake_daemon_ok(
        &env,
        "whoami",
        json!({"id":"u1","username":"agent-bravo-devlead","is_bot":true}),
    )
    .await;
    let output = run_chanvoy_with_stdin(
        &env,
        &["mcp"],
        rpc_line(2, "tools/call", json!({"name":"whoami","arguments":{}})).as_bytes(),
    )
    .await;
    assert!(output.status.success());
    let value = parse_stdout_json(&output.stdout);
    assert_eq!(value["result"]["isError"], false);
    assert_eq!(
        value["result"]["structuredContent"]["result"]["username"],
        "agent-bravo-devlead"
    );
    let text: serde_json::Value =
        serde_json::from_str(value["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(text, value["result"]["structuredContent"]["result"]);
}

#[tokio::test]
async fn mcp_stdio_wait_old_daemon_is_capability() {
    let env = TestEnv::new("per-041-cap").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    fake_daemon_unknown(&env, WAIT_CHANNEL_V3_METHOD).await;
    let output = run_chanvoy_with_stdin(
        &env,
        &["mcp"],
        rpc_line(
            3,
            "tools/call",
            json!({
                "name": "wait",
                "arguments": {"mode":"single","channel":"brief-per-041","timeout_secs":5}
            }),
        )
        .as_bytes(),
    )
    .await;
    assert!(output.status.success());
    let value = parse_stdout_json(&output.stdout);
    assert_eq!(value["result"]["isError"], true);
    assert_eq!(
        value["result"]["structuredContent"]["error"]["class"],
        "capability"
    );
    assert_eq!(
        value["result"]["structuredContent"]["error"]["timeout"],
        false
    );
    let message = value["result"]["structuredContent"]["error"]["message"]
        .as_str()
        .unwrap();
    assert!(message.contains("wait_channel_v3"));
    assert!(!message.contains("wait_channel_v2"));
}

#[tokio::test]
async fn mcp_stdio_unknown_tool_is_protocol_error() {
    let env = TestEnv::new("per-041-unknown").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let output = run_chanvoy_with_stdin(
        &env,
        &["mcp"],
        rpc_line(4, "tools/call", json!({"name":"notify","arguments":{}})).as_bytes(),
    )
    .await;
    assert!(output.status.success());
    let value = parse_stdout_json(&output.stdout);
    assert_eq!(value["error"]["code"], -32602);
}

#[tokio::test]
async fn mcp_listen_refuses_non_loopback_bind() {
    let env = TestEnv::new("per-041-bind").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let output = run_chanvoy_with_stdin(&env, &["mcp", "--listen", "0.0.0.0:9"], b"").await;
    assert!(!output.status.success());
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.contains("127.0.0.1"), "{err}");
}

#[tokio::test]
async fn mcp_stdio_daemon_eof_during_wait_is_hard_fail() {
    let env = TestEnv::new("per-041-eof").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let socket_path = env.socket_path();
    let listener = UnixListener::bind(&socket_path).expect("bind");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        drop(stream);
    });

    let output = tokio::time::timeout(
        Duration::from_secs(5),
        run_chanvoy_with_stdin(
            &env,
            &["mcp"],
            rpc_line(
                8,
                "tools/call",
                json!({
                    "name":"wait",
                    "arguments":{"mode":"single","channel":"ops","timeout_secs":20}
                }),
            )
            .as_bytes(),
        ),
    )
    .await
    .expect("daemon EOF must not hang");
    assert!(output.status.success(), "tool error is a JSON-RPC result");
    let value = parse_stdout_json(&output.stdout);
    assert_eq!(value["result"]["isError"], true);
    assert_eq!(
        value["result"]["structuredContent"]["error"]["timeout"],
        false
    );
    assert_ne!(
        value["result"]["structuredContent"]["error"]["class"],
        "deadman"
    );
}
