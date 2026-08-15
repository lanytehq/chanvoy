//! Hermetic PER-041 MCP face (stdio pipes against a fake daemon).

#![allow(dead_code)]

use std::time::Duration;

mod common;

use chanvoy_core::{rpc_error, rpc_result, JsonRpcRequest, WAIT_CHANNEL_V3_METHOD};
use common::{run_chanvoy_with_stdin, TestEnv};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Child;

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

/// Session-style MCP: write one request, keep stdin open until a response line.
async fn run_mcp_keep_stdin(env: &TestEnv, request: &str) -> serde_json::Value {
    let mut child = spawn_mcp(env);
    {
        let mut stdin = child.stdin.take().expect("stdin");
        stdin.write_all(request.as_bytes()).await.expect("write");
        stdin.flush().await.expect("flush");
        // Keep the handle until after we read stdout so EOF cannot
        // cancel the in-flight daemon call.
        let stdout = child.stdout.take().expect("stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("response line");
        drop(stdin);
        let _ = child.wait().await;
        serde_json::from_str(line.trim()).expect("json-rpc line")
    }
}

fn spawn_mcp(env: &TestEnv) -> Child {
    env.chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .arg("mcp")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn mcp")
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
    let value = run_mcp_keep_stdin(
        &env,
        &rpc_line(2, "tools/call", json!({"name":"whoami","arguments":{}})),
    )
    .await;
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
    let value = run_mcp_keep_stdin(
        &env,
        &rpc_line(
            3,
            "tools/call",
            json!({
                "name": "wait",
                "arguments": {"mode":"single","channel":"brief-per-041","timeout_secs":5}
            }),
        ),
    )
    .await;
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
    let env = TestEnv::new("per-041-deof").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let socket_path = env.socket_path();
    let listener = UnixListener::bind(&socket_path).expect("bind");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        drop(stream);
    });

    let value = tokio::time::timeout(
        Duration::from_secs(5),
        run_mcp_keep_stdin(
            &env,
            &rpc_line(
                8,
                "tools/call",
                json!({
                    "name":"wait",
                    "arguments":{"mode":"single","channel":"ops","timeout_secs":20}
                }),
            ),
        ),
    )
    .await
    .expect("daemon EOF must not hang");
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

#[tokio::test]
async fn mcp_stdio_client_eof_closes_uds_and_admits_later_wait() {
    let env = TestEnv::new("per-041-cancel").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let socket_path = env.socket_path();
    let listener = UnixListener::bind(&socket_path).expect("bind");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("first accept");
        let (reader, _writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("first request");
        let first: JsonRpcRequest = serde_json::from_str(line.trim_end()).expect("decode");
        assert_eq!(first.method, WAIT_CHANNEL_V3_METHOD);
        // Peer-close: MCP must drop the UDS when stdin EOFs.
        let n = tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut String::new()))
            .await
            .expect("uds must close after stdin EOF")
            .expect("read after cancel");
        assert_eq!(n, 0, "first wait connection must be dropped");

        let (stream, _) = listener.accept().await.expect("second accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("second request");
        let second: JsonRpcRequest = serde_json::from_str(line.trim_end()).expect("decode");
        assert_eq!(second.method, WAIT_CHANNEL_V3_METHOD);
        let response = rpc_result(
            second.id,
            json!({"channel":"ops","messages":[{"id":"p","user_id":"u","username":"n","message":"hi","create_at":1,"root_id":"p"}]}),
        );
        writer
            .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
            .await
            .expect("write match");
    });

    let mut first = spawn_mcp(&env);
    {
        let mut stdin = first.stdin.take().expect("stdin");
        stdin
            .write_all(
                rpc_line(
                    1,
                    "tools/call",
                    json!({
                        "name":"wait",
                        "arguments":{"mode":"single","channel":"ops","timeout_secs":30}
                    }),
                )
                .as_bytes(),
            )
            .await
            .expect("write wait");
        drop(stdin);
    }
    let first_out = tokio::time::timeout(Duration::from_secs(5), first.wait_with_output())
        .await
        .expect("cancelled mcp must exit")
        .expect("collect");
    assert!(
        !first_out.status.success(),
        "stdin EOF during wait is a hard fail"
    );
    let cancelled = parse_stdout_json(&first_out.stdout);
    assert_eq!(cancelled["result"]["isError"], true);
    assert_eq!(
        cancelled["result"]["structuredContent"]["error"]["timeout"],
        false
    );

    let second = run_mcp_keep_stdin(
        &env,
        &rpc_line(
            2,
            "tools/call",
            json!({
                "name":"wait",
                "arguments":{"mode":"single","channel":"ops","timeout_secs":5}
            }),
        ),
    )
    .await;
    assert_eq!(second["result"]["isError"], false);
    assert_eq!(
        second["result"]["structuredContent"]["result"]["channel"],
        "ops"
    );
    server.await.expect("fake daemon");
}

#[tokio::test]
async fn mcp_stdio_lists_and_dispatches_every_tool() {
    let env = TestEnv::new("per-041-all").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let socket_path = env.socket_path();
    let listener = UnixListener::bind(&socket_path).expect("bind");
    tokio::spawn(async move {
        let expected = [
            "whoami",
            "read_channel",
            "get_post",
            "read_thread",
            "wait_channel_v3",
            "post_message",
        ];
        for method in expected {
            let (stream, _) = listener.accept().await.expect("accept");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read");
            let request: JsonRpcRequest = serde_json::from_str(line.trim_end()).expect("decode");
            assert_eq!(request.method, method);
            let result = match method {
                "whoami" => json!({"id":"u","username":"bot"}),
                "read_channel" | "read_thread" => json!([]),
                "get_post" => {
                    json!({"id":"p","user_id":"u","username":"n","message":"m","create_at":1,"root_id":"p"})
                }
                "wait_channel_v3" => {
                    json!({"channel":"ops","messages":[{"id":"p","user_id":"u","username":"n","message":"m","create_at":1,"root_id":"p"}]})
                }
                "post_message" => json!({"id":"new"}),
                _ => unreachable!(),
            };
            let response = rpc_result(request.id, result);
            writer
                .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
                .await
                .expect("write");
        }
    });

    let calls = [
        ("whoami", json!({})),
        ("read_channel", json!({"channel":"ops","since_secs":30})),
        ("show", json!({"channel":"ops","post_id":"p"})),
        ("thread", json!({"channel":"ops","post_id":"p"})),
        (
            "wait",
            json!({"mode":"single","channel":"ops","timeout_secs":5}),
        ),
        ("post", json!({"channel":"ops","message":"hi"})),
    ];
    for (name, arguments) in calls {
        let value = run_mcp_keep_stdin(
            &env,
            &rpc_line(1, "tools/call", json!({"name":name,"arguments":arguments})),
        )
        .await;
        assert_eq!(value["result"]["isError"], false, "{name}");
    }
}
