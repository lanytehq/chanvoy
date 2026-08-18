//! Hermetic PER-040 single-waiter ownership.

#![allow(dead_code)]

use std::path::PathBuf;
use std::time::Duration;

mod common;

use chanvoy_core::{
    rpc_error, rpc_error_with_data, rpc_request, JsonRpcRequest, JsonRpcResponse,
    RPC_WAIT_ALREADY_ACTIVE, WAIT_CHANNEL_V3_METHOD,
};
use common::{run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;

#[derive(Clone)]
enum FakeReply {
    UnknownMethod,
    AlreadyActive,
}

async fn fake_daemon(env: &TestEnv, expected: Vec<(&'static str, FakeReply)>) -> JoinHandle<()> {
    let socket_path = env.socket_path();
    let listener = UnixListener::bind(&socket_path).expect("bind fake daemon socket");
    let path = socket_path.to_path_buf();
    tokio::spawn(async move {
        for (method, reply) in expected {
            let (stream, _) = listener.accept().await.expect("accept");
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read request");
            let request: JsonRpcRequest =
                serde_json::from_str(line.trim_end()).expect("decode request");
            assert_eq!(request.method, method, "unexpected JSON-RPC method");
            let response = match reply {
                FakeReply::UnknownMethod => {
                    rpc_error(request.id, -32601, "unknown method wait_channel_v3")
                }
                FakeReply::AlreadyActive => rpc_error_with_data(
                    request.id,
                    RPC_WAIT_ALREADY_ACTIVE,
                    "wait already active on this profile daemon",
                    Some(json!({
                        "class": "wait_already_active",
                        "team": "org-lanytehq",
                        "channel": "brief-per-040",
                        "existing_wait_id": "wait_0123456789abcdef0123456789abcdef",
                        "started_at_ms": 1786480000000i64
                    })),
                ),
            };
            writer
                .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
                .await
                .expect("write response");
        }
        let _ = std::fs::remove_file(path);
    })
}

async fn raw_rpc(socket_path: PathBuf, method: &str, params: serde_json::Value) -> JsonRpcResponse {
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

async fn mount_empty_channel(env: &TestEnv, channel: &str, channel_id: &str) {
    env.mock_baseline("bot-id-per040", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup(channel, channel_id).await;
    env.mock_channel_posts(channel_id, &[]).await;
}

#[tokio::test]
#[ignore = "integration: PER-040 process outcome matrix"]
async fn wait_v3_old_daemon_is_hard_capability() {
    let env = TestEnv::new("per-040-skew-old").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let server = fake_daemon(
        &env,
        vec![(WAIT_CHANNEL_V3_METHOD, FakeReply::UnknownMethod)],
    )
    .await;
    let output = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-040", "--timeout", "1s"],
    )
    .await;
    server.await.expect("fake daemon completed");
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["timeout"], false);
    assert_eq!(value["error_class"], "capability");
    let message = value["message"].as_str().unwrap_or_default();
    assert!(message.contains("wait_channel_v3"), "{message}");
    assert!(!message.contains("wait_channel_v2"));
}

#[tokio::test]
#[ignore = "integration: PER-040 process outcome matrix"]
async fn wait_already_active_is_exit_two_redacted_json() {
    let env = TestEnv::new("per-040-already-active").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let server = fake_daemon(
        &env,
        vec![(WAIT_CHANNEL_V3_METHOD, FakeReply::AlreadyActive)],
    )
    .await;
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-040",
            "--timeout",
            "5s",
            "--contains",
            "SECRET-FILTER",
        ],
    )
    .await;
    server.await.expect("fake daemon completed");
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["timeout"], false);
    assert_eq!(value["error"]["class"], "wait_already_active");
    assert_eq!(
        value["error"]["existing_wait_id"],
        "wait_0123456789abcdef0123456789abcdef"
    );
    let dumped = value.to_string();
    assert!(
        !dumped.contains("SECRET-FILTER"),
        "conflict JSON must not echo the filter: {dumped}"
    );
}

#[tokio::test]
#[ignore = "integration: PER-040 process outcome matrix"]
async fn same_channel_second_wait_is_refused_on_live_daemon() {
    let env = TestEnv::new("per-040-live-refuse").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_empty_channel(&env, "brief-per-040", "chan-id-per040").await;
    let daemon = spawn_daemon(&env).await;

    let mut first = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(["--json", "wait", "brief-per-040", "--timeout", "20s"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn first wait");
    tokio::time::sleep(Duration::from_millis(400)).await;

    let second = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-040", "--timeout", "5s"],
    )
    .await;
    assert_eq!(second.status.code(), Some(2), "second wait must refuse");
    let value: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(value["timeout"], false);
    assert_eq!(value["error"]["class"], "wait_already_active");
    assert!(value["error"]["existing_wait_id"]
        .as_str()
        .is_some_and(|id| id.starts_with("wait_")));

    let _ = first.start_kill();
    let _ = first.wait().await;
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: PER-040 process outcome matrix"]
async fn v2_rpc_cannot_replace_but_gains_refuse_default() {
    let env = TestEnv::new("per-040-v2-refuse").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_empty_channel(&env, "brief-per-040", "chan-id-per040-v2").await;
    let daemon = spawn_daemon(&env).await;

    let mut first = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(["wait", "brief-per-040", "--timeout", "20s"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn first wait");
    tokio::time::sleep(Duration::from_millis(400)).await;

    let response = raw_rpc(
        env.socket_path(),
        "wait_channel_v2",
        json!({
            "channel": "brief-per-040",
            "timeout_secs": 3,
            "team": null,
            "contains": null,
            "pattern": null,
            "after": null
        }),
    )
    .await;
    assert_eq!(
        response.error.as_ref().map(|e| e.code),
        Some(RPC_WAIT_ALREADY_ACTIVE)
    );
    assert_eq!(
        response
            .error
            .as_ref()
            .and_then(|e| e.data.as_ref())
            .and_then(|d| d.get("class"))
            .and_then(|c| c.as_str()),
        Some("wait_already_active")
    );

    let _ = first.start_kill();
    let _ = first.wait().await;
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: PER-040 process outcome matrix"]
async fn replace_wait_displaces_old_process() {
    let env = TestEnv::new("per-040-replace-exact").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_empty_channel(&env, "brief-per-040", "chan-id-per040-replace").await;
    let daemon = spawn_daemon(&env).await;

    let first = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(["--json", "wait", "brief-per-040", "--timeout", "30s"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn first wait");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let conflict = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-040", "--timeout", "3s"],
    )
    .await;
    assert_eq!(conflict.status.code(), Some(2));
    let conflict_json: serde_json::Value = serde_json::from_slice(&conflict.stdout).unwrap();
    let wait_id = conflict_json["error"]["existing_wait_id"]
        .as_str()
        .expect("existing wait id")
        .to_string();

    let mut replacement = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "--json",
            "wait",
            "brief-per-040",
            "--timeout",
            "20s",
            "--replace-wait",
            &wait_id,
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn replace wait");

    let first_out = first.wait_with_output().await.expect("first wait exit");
    assert_eq!(first_out.status.code(), Some(2), "old waiter must exit 2");
    let first_json: serde_json::Value = serde_json::from_slice(&first_out.stdout).unwrap();
    assert_eq!(first_json["timeout"], false);
    assert_eq!(first_json["error"]["class"], "wait_replaced");
    assert_eq!(first_json["error"]["wait_id"], wait_id);

    tokio::time::sleep(Duration::from_millis(200)).await;
    let third = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-040", "--timeout", "2s"],
    )
    .await;
    assert_eq!(third.status.code(), Some(2));
    let third_json: serde_json::Value = serde_json::from_slice(&third.stdout).unwrap();
    assert_eq!(
        third_json["error"]["class"], "wait_already_active",
        "replacement must own the key after old cleanup: {third_json}"
    );
    assert_ne!(third_json["error"]["existing_wait_id"], wait_id);

    let _ = replacement.start_kill();
    let _ = replacement.wait().await;
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: PER-040 process outcome matrix"]
async fn bad_replace_tokens_leave_active_waiter() {
    let env = TestEnv::new("per-040-replace-bad-tokens").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_empty_channel(&env, "brief-a", "chan-id-a").await;
    mount_empty_channel(&env, "brief-b", "chan-id-b").await;
    let daemon = spawn_daemon(&env).await;

    let mut first = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(["--json", "wait", "brief-a", "--timeout", "30s"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn first wait");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let conflict = run_chanvoy(&env, &["--json", "wait", "brief-a", "--timeout", "2s"]).await;
    let live_id = serde_json::from_slice::<serde_json::Value>(&conflict.stdout).unwrap()["error"]
        ["existing_wait_id"]
        .as_str()
        .unwrap()
        .to_string();

    let stale = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-a",
            "--timeout",
            "2s",
            "--replace-wait",
            "wait_0123456789abcdef0123456789abcdef",
        ],
    )
    .await;
    let malformed = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-a",
            "--timeout",
            "2s",
            "--replace-wait",
            "not-a-wait-id",
        ],
    )
    .await;
    let other_channel = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-b",
            "--timeout",
            "2s",
            "--replace-wait",
            &live_id,
        ],
    )
    .await;
    for (label, out) in [
        ("stale", stale),
        ("malformed", malformed),
        ("other-channel", other_channel),
    ] {
        assert_eq!(out.status.code(), Some(2), "{label}");
        let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(value["timeout"], false, "{label}");
        assert_eq!(
            value["error"]["class"], "wait_conflict_changed",
            "{label} => {value}"
        );
    }

    let still = run_chanvoy(&env, &["--json", "wait", "brief-a", "--timeout", "2s"]).await;
    let still_json: serde_json::Value = serde_json::from_slice(&still.stdout).unwrap();
    assert_eq!(still_json["error"]["class"], "wait_already_active");
    assert_eq!(still_json["error"]["existing_wait_id"], live_id);
    assert!(
        first.try_wait().unwrap().is_none(),
        "old waiter still armed"
    );

    let absent = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-b",
            "--timeout",
            "2s",
            "--replace-wait",
            "wait_ffffffffffffffffffffffffffffffff",
        ],
    )
    .await;
    let absent_json: serde_json::Value = serde_json::from_slice(&absent.stdout).unwrap();
    assert_eq!(absent_json["error"]["class"], "wait_conflict_changed");

    let _ = first.start_kill();
    let _ = first.wait().await;
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: PER-040 process outcome matrix"]
async fn client_disconnect_releases_wait_immediately() {
    let env = TestEnv::new("per-040-disconnect").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_empty_channel(&env, "brief-per-040", "chan-id-per040-disc").await;
    let daemon = spawn_daemon(&env).await;

    let mut first = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(["--json", "wait", "brief-per-040", "--timeout", "30s"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn first wait");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let held = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-040", "--timeout", "2s"],
    )
    .await;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&held.stdout).unwrap()["error"]["class"],
        "wait_already_active"
    );

    let _ = first.start_kill();
    let _ = first.wait().await;

    let mut value = serde_json::Value::Null;
    let mut successor_status = None;
    for _ in 0..20 {
        let successor = run_chanvoy(
            &env,
            &["--json", "wait", "brief-per-040", "--timeout", "2s"],
        )
        .await;
        successor_status = successor.status.code();
        value = serde_json::from_slice(&successor.stdout).unwrap();
        if value["error"]["class"] != "wait_already_active" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_ne!(
        value["error"]["class"], "wait_already_active",
        "disconnect must release the key: {value}"
    );
    assert!(
        successor_status == Some(1) || value["timeout"] == true,
        "successor should deadman on empty channel, got {value} status={successor_status:?}"
    );
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: PER-040 process outcome matrix"]
async fn replacing_client_disconnect_does_not_strand_key() {
    let env = TestEnv::new("per-040-replace-disconnect").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_empty_channel(&env, "brief-per-040", "chan-id-per040-rdisc").await;
    let daemon = spawn_daemon(&env).await;

    let mut first = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(["--json", "wait", "brief-per-040", "--timeout", "30s"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn first wait");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let conflict = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-040", "--timeout", "2s"],
    )
    .await;
    let wait_id = serde_json::from_slice::<serde_json::Value>(&conflict.stdout).unwrap()["error"]
        ["existing_wait_id"]
        .as_str()
        .unwrap()
        .to_string();

    let mut replacer = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "--json",
            "wait",
            "brief-per-040",
            "--timeout",
            "20s",
            "--replace-wait",
            &wait_id,
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn replace wait");
    tokio::time::sleep(Duration::from_millis(150)).await;
    let _ = replacer.start_kill();
    let _ = replacer.wait().await;
    let _ = first.start_kill();
    let _ = first.wait().await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let successor = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-040", "--timeout", "2s"],
    )
    .await;
    let value: serde_json::Value = serde_json::from_slice(&successor.stdout).unwrap();
    assert_ne!(
        value["error"]["class"], "wait_already_active",
        "stranded reservation after replacer disconnect: {value}"
    );
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: PER-040 process outcome matrix"]
async fn v3_unknown_field_and_oversize_channel_are_input() {
    let env = TestEnv::new("per-040-strict-v3").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_empty_channel(&env, "brief-per-040", "chan-id-per040-strict").await;
    let daemon = spawn_daemon(&env).await;

    let extra = raw_rpc(
        env.socket_path(),
        WAIT_CHANNEL_V3_METHOD,
        json!({
            "channel": "brief-per-040",
            "timeout_secs": 3,
            "force": true
        }),
    )
    .await;
    assert_eq!(extra.error.as_ref().map(|e| e.code), Some(-32007));

    let oversize = raw_rpc(
        env.socket_path(),
        WAIT_CHANNEL_V3_METHOD,
        json!({
            "channel": "a".repeat(257),
            "timeout_secs": 3
        }),
    )
    .await;
    assert_eq!(oversize.error.as_ref().map(|e| e.code), Some(-32007));
    let message = oversize
        .error
        .as_ref()
        .map(|e| e.message.as_str())
        .unwrap_or_default();
    assert!(message.contains("256"), "{message}");

    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: PER-040 process outcome matrix"]
async fn invalid_or_foreign_after_cannot_replace_active_wait() {
    let env = TestEnv::new("per-040-replace-bad-after").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_empty_channel(&env, "brief-per-040", "chan-id-per040-badafter").await;
    env.mock_post_lookup("missing-anchor", "chan-id-per040-badafter", false)
        .await;
    env.mock_post_lookup("foreign-anchor", "chan-id-other", true)
        .await;
    let daemon = spawn_daemon(&env).await;

    let mut first = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(["--json", "wait", "brief-per-040", "--timeout", "30s"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn first wait");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let conflict = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-040", "--timeout", "2s"],
    )
    .await;
    let live_id = serde_json::from_slice::<serde_json::Value>(&conflict.stdout).unwrap()["error"]
        ["existing_wait_id"]
        .as_str()
        .unwrap()
        .to_string();

    for (label, after) in [("missing", "missing-anchor"), ("foreign", "foreign-anchor")] {
        let out = run_chanvoy(
            &env,
            &[
                "--json",
                "wait",
                "brief-per-040",
                "--timeout",
                "2s",
                "--replace-wait",
                &live_id,
                "--after",
                after,
            ],
        )
        .await;
        assert_eq!(out.status.code(), Some(2), "{label}");
        let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(value["timeout"], false, "{label} => {value}");
        assert_eq!(
            value["error_class"], "input",
            "{label} must be caller-input, not ownership: {value}"
        );
        assert_ne!(
            value["error"]["class"], "wait_already_active",
            "{label} must not be reported as already-active: {value}"
        );
        assert_ne!(
            value["error"]["class"], "wait_replaced",
            "{label} must not replace: {value}"
        );
    }

    let still = run_chanvoy(
        &env,
        &["--json", "wait", "brief-per-040", "--timeout", "2s"],
    )
    .await;
    let still_json: serde_json::Value = serde_json::from_slice(&still.stdout).unwrap();
    assert_eq!(still_json["error"]["class"], "wait_already_active");
    assert_eq!(still_json["error"]["existing_wait_id"], live_id);
    assert!(
        first.try_wait().unwrap().is_none(),
        "invalid --after must not displace the live waiter"
    );

    let _ = first.start_kill();
    let _ = first.wait().await;
    assert!(stop_daemon_cleanly(&env, daemon).await);
}
