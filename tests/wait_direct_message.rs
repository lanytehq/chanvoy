//! Hermetic `wait --dm` proofs.

#![allow(dead_code)]

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

mod common;

use chanvoy_core::{
    canonical_dm_name, rpc_error, rpc_request, JsonRpcRequest, JsonRpcResponse, WaitFollowEvent,
    WaitFollowMode, NOT_A_WAITABLE_PEER, WAIT_DM_FOLLOW_V1_METHOD, WAIT_DM_HELP, WAIT_DM_V1_METHOD,
};
use common::{force_kill_child, run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

const BOT_ID: &str = "userid00000000000000000000";
const BOT_USER: &str = "agent-bravo-devlead";
const PEER_ID: &str = "userid00000000000000000001";
const PEER_USER: &str = "dave-3leaps";
const POST_0: &str = "postid00000000000000000000";
const POST_1: &str = "postid00000000000000000001";
const POST_2: &str = "postid00000000000000000002";
const DM_ID: &str = "dmid0000000000000000000001";

fn dm_name() -> String {
    canonical_dm_name(BOT_ID, PEER_ID)
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

async fn mount_peer_user(env: &TestEnv) {
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/users/username/{PEER_USER}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": PEER_ID,
            "username": PEER_USER
        })))
        .mount(&env.mock)
        .await;
}

async fn mount_direct_channel(env: &TestEnv) {
    Mock::given(method("POST"))
        .and(path("/api/v4/channels/direct"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": DM_ID,
            "name": "not-the-canonical-name"
        })))
        .mount(&env.mock)
        .await;
}

async fn follow_until_armed_peer(env: &TestEnv, out: &Path, needle: &str, extra: &[&str]) {
    let child = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "wait",
            "--dm",
            PEER_USER,
            "--timeout",
            "30s",
            "--follow",
            "--out",
            out.to_str().unwrap(),
        ])
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn follow");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        if let Ok(raw) = std::fs::read_to_string(out) {
            if follow_has_valid_armed(&raw) && follow_has_valid_peer(&raw, needle) {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "follow stream missing valid armed record or peer message; out={}",
            std::fs::read_to_string(out).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    force_kill_child(child, "follow waiter").await;
    let raw = std::fs::read_to_string(out).expect("follow jsonl");
    assert!(follow_has_valid_armed(&raw), "armed record missing: {raw}");
    assert!(
        follow_has_valid_peer(&raw, needle),
        "valid peer emission missing: {raw}"
    );
}

fn follow_has_valid_armed(raw: &str) -> bool {
    raw.lines().any(|line| {
        serde_json::from_str::<WaitFollowEvent>(line)
            .ok()
            .is_some_and(|event| event.validate().is_ok() && event.mode() == WaitFollowMode::Armed)
    })
}

fn follow_has_valid_peer(raw: &str, needle: &str) -> bool {
    raw.lines().any(|line| {
        let Ok(event) = serde_json::from_str::<WaitFollowEvent>(line) else {
            return false;
        };
        if event.validate().is_err() {
            return false;
        }
        matches!(event.mode(), WaitFollowMode::Backlog | WaitFollowMode::Live)
            && event
                .messages()
                .first()
                .is_some_and(|message| message.message == needle)
    })
}

fn assert_not_a_peer(output: &std::process::Output) {
    assert_eq!(
        output.status.code(),
        Some(2),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json");
    assert_eq!(value["timeout"], false);
    assert_eq!(value["error_class"], "input");
    let message = value["message"].as_str().unwrap_or_default();
    assert!(
        message.contains(NOT_A_WAITABLE_PEER),
        "diagnostic must be one not-a-peer class: {message}"
    );
}

#[tokio::test]
#[ignore = "integration: wait --dm CLI refuse shapes"]
async fn cli_refuses_uuid_user_id_dm_name_self_and_mixes_before_provider() {
    let env = TestEnv::new("wait-dm-cli-refuse").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");

    for bad in [
        "550e8400-e29b-41d4-a716-446655440000",
        PEER_ID,
        &dm_name(),
        BOT_USER,
    ] {
        let output = run_chanvoy(&env, &["--json", "wait", "--dm", bad, "--timeout", "1s"]).await;
        assert_not_a_peer(&output);
    }

    let mixed_team = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--dm",
            PEER_USER,
            "--team",
            "org-lanytehq",
            "--timeout",
            "1s",
        ],
    )
    .await;
    assert_eq!(mixed_team.status.code(), Some(2));
    let team_json: serde_json::Value = serde_json::from_slice(&mixed_team.stdout).unwrap();
    assert_eq!(team_json["error_class"], "input");
    assert!(team_json["message"]
        .as_str()
        .unwrap_or_default()
        .contains("--team"));

    let mixed_fan = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--dm",
            PEER_USER,
            "--channel",
            "org-lanytehq/a",
            "--channel",
            "org-lanytehq/b",
            "--timeout",
            "1s",
        ],
    )
    .await;
    assert_eq!(mixed_fan.status.code(), Some(2));

    let mixed_positional = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "release-floor",
            "--dm",
            PEER_USER,
            "--timeout",
            "1s",
        ],
    )
    .await;
    assert_eq!(mixed_positional.status.code(), Some(2));

    let mixed_after_channel = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--dm",
            PEER_USER,
            "--after-channel",
            "org-lanytehq/release-floor=postid00000000000000000000",
            "--timeout",
            "1s",
        ],
    )
    .await;
    assert_eq!(mixed_after_channel.status.code(), Some(2));
    let after_json: serde_json::Value =
        serde_json::from_slice(&mixed_after_channel.stdout).unwrap();
    assert_eq!(after_json["error_class"], "input");
    assert!(after_json["message"]
        .as_str()
        .unwrap_or_default()
        .contains("--after-channel"));
}

#[tokio::test]
#[ignore = "integration: wait --dm capability skew"]
async fn old_daemon_is_hard_capability_without_fallback() {
    let env = TestEnv::new("wait-dm-capability").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    let server = fake_old_daemon(&env, WAIT_DM_V1_METHOD).await;
    let output = run_chanvoy(
        &env,
        &["--json", "wait", "--dm", PEER_USER, "--timeout", "1s"],
    )
    .await;
    server.await.expect("fake daemon completed");
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["timeout"], false);
    assert_eq!(value["error_class"], "capability");
    let message = value["message"].as_str().unwrap_or_default();
    assert!(message.contains("wait_dm_v1"), "{message}");
    assert!(!message.contains("wait_channel_v3"));
}

#[tokio::test]
#[ignore = "integration: wait --dm follow capability skew"]
async fn old_daemon_follow_is_hard_capability() {
    let env = TestEnv::new("wait-dm-follow-capability").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    let server = fake_old_daemon(&env, WAIT_DM_FOLLOW_V1_METHOD).await;
    let out = env.runtime_dir().join("follow.jsonl");
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--dm",
            PEER_USER,
            "--timeout",
            "1s",
            "--follow",
            "--out",
            out.to_str().unwrap(),
        ],
    )
    .await;
    server.await.expect("fake daemon completed");
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error_class"], "capability");
}

#[tokio::test]
#[ignore = "integration: wait --dm match, unknown, and computed dm_name"]
async fn wait_dm_match_names_peer_and_unknown_is_not_a_peer() {
    let env = TestEnv::new("wait-dm-match").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    mount_peer_user(&env).await;
    mount_direct_channel(&env).await;
    env.mock_post_lookup(POST_0, DM_ID, true).await;
    env.mock_channel_posts(
        DM_ID,
        &[(
            POST_1,
            PEER_ID,
            PEER_USER,
            "poke from peer",
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
            "--dm",
            PEER_USER,
            "--timeout",
            "5s",
            "--after",
            POST_0,
        ],
    )
    .await;
    assert_eq!(
        matched.status.code(),
        Some(0),
        "stderr={}",
        String::from_utf8_lossy(&matched.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&matched.stdout).unwrap();
    assert_eq!(value["peer_username"], PEER_USER);
    assert_eq!(value["dm_name"], dm_name());
    assert_ne!(value["dm_name"], "not-the-canonical-name");
    assert_eq!(value["messages"][0]["id"], POST_1);
    assert!(stop_daemon_cleanly(&env, daemon).await);

    let env = TestEnv::new("wait-dm-unknown").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/username/no-such-user"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "status_code": 404,
            "message": "Not Found"
        })))
        .mount(&env.mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/channels/direct"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&env.mock)
        .await;
    let daemon = spawn_daemon(&env).await;
    let unknown = run_chanvoy(
        &env,
        &["--json", "wait", "--dm", "no-such-user", "--timeout", "2s"],
    )
    .await;
    assert_not_a_peer(&unknown);
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: wait --dm create-time inaccessible peer"]
async fn direct_create_403_is_not_a_peer_and_does_not_acquire() {
    let env = TestEnv::new("wait-dm-create-403").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    mount_peer_user(&env).await;
    Mock::given(method("POST"))
        .and(path("/api/v4/channels/direct"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "status_code": 403,
            "message": "forbidden"
        })))
        .expect(1)
        .mount(&env.mock)
        .await;
    let daemon = spawn_daemon(&env).await;
    let denied = run_chanvoy(
        &env,
        &["--json", "wait", "--dm", PEER_USER, "--timeout", "3s"],
    )
    .await;
    assert_not_a_peer(&denied);

    env.reset_mocks().await;
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    mount_peer_user(&env).await;
    mount_direct_channel(&env).await;
    env.mock_post_lookup(POST_0, DM_ID, true).await;
    env.mock_channel_posts(
        DM_ID,
        &[(
            POST_1,
            PEER_ID,
            PEER_USER,
            "poke from peer",
            1_780_000_000_100,
        )],
    )
    .await;
    let matched = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--dm",
            PEER_USER,
            "--timeout",
            "5s",
            "--after",
            POST_0,
        ],
    )
    .await;
    assert_eq!(
        matched.status.code(),
        Some(0),
        "failed create must not leave an owner; stderr={}",
        String::from_utf8_lossy(&matched.stderr)
    );
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: wait --dm create-time 404"]
async fn direct_create_404_is_not_a_peer() {
    let env = TestEnv::new("wait-dm-create-404").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    mount_peer_user(&env).await;
    Mock::given(method("POST"))
        .and(path("/api/v4/channels/direct"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "status_code": 404,
            "message": "not found"
        })))
        .expect(1)
        .mount(&env.mock)
        .await;
    let daemon = spawn_daemon(&env).await;
    let denied = run_chanvoy(
        &env,
        &["--json", "wait", "--dm", PEER_USER, "--timeout", "3s"],
    )
    .await;
    assert_not_a_peer(&denied);
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: wait --dm ignores bot-authored posts"]
async fn wait_dm_skips_bot_authored_post_and_matches_peer() {
    let env = TestEnv::new("wait-dm-self-ignore").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    mount_peer_user(&env).await;
    mount_direct_channel(&env).await;
    env.mock_post_lookup(POST_0, DM_ID, true).await;
    env.mock_channel_posts(
        DM_ID,
        &[
            (POST_1, BOT_ID, BOT_USER, "own dm", 1_780_000_000_050),
            (
                POST_2,
                PEER_ID,
                PEER_USER,
                "poke from peer",
                1_780_000_000_100,
            ),
        ],
    )
    .await;
    let daemon = spawn_daemon(&env).await;
    let matched = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--dm",
            PEER_USER,
            "--timeout",
            "5s",
            "--after",
            POST_0,
        ],
    )
    .await;
    assert_eq!(
        matched.status.code(),
        Some(0),
        "stderr={}",
        String::from_utf8_lossy(&matched.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&matched.stdout).unwrap();
    assert_eq!(value["messages"][0]["id"], POST_2);
    assert_eq!(value["messages"][0]["username"], PEER_USER);
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: wait --dm racing post recovered by backfill"]
async fn wait_dm_recovers_post_that_races_direct_create() {
    let env = TestEnv::new("wait-dm-race").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    mount_peer_user(&env).await;
    Mock::given(method("POST"))
        .and(path("/api/v4/channels/direct"))
        .respond_with(
            ResponseTemplate::new(201)
                .set_delay(Duration::from_millis(250))
                .set_body_json(json!({
                    "id": DM_ID,
                    "name": "not-the-canonical-name"
                })),
        )
        .mount(&env.mock)
        .await;
    env.mock_post_lookup(POST_0, DM_ID, true).await;
    env.mock_channel_posts(
        DM_ID,
        &[(POST_1, PEER_ID, PEER_USER, "raced poke", 1_780_000_000_100)],
    )
    .await;
    let daemon = spawn_daemon(&env).await;
    let matched = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--dm",
            PEER_USER,
            "--timeout",
            "5s",
            "--after",
            POST_0,
        ],
    )
    .await;
    assert_eq!(
        matched.status.code(),
        Some(0),
        "stderr={}",
        String::from_utf8_lossy(&matched.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&matched.stdout).unwrap();
    assert_eq!(value["messages"][0]["message"], "raced poke");
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: wait --dm follow racing post"]
async fn wait_dm_follow_recovers_racing_post() {
    let env = TestEnv::new("wait-dm-follow-race").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    mount_peer_user(&env).await;
    Mock::given(method("POST"))
        .and(path("/api/v4/channels/direct"))
        .respond_with(
            ResponseTemplate::new(201)
                .set_delay(Duration::from_millis(250))
                .set_body_json(json!({"id": DM_ID, "name": "ignored"})),
        )
        .mount(&env.mock)
        .await;
    env.mock_post_lookup(POST_0, DM_ID, true).await;
    env.mock_channel_posts(
        DM_ID,
        &[(
            POST_1,
            PEER_ID,
            PEER_USER,
            "follow raced poke",
            1_780_000_000_100,
        )],
    )
    .await;
    let daemon = spawn_daemon(&env).await;
    let out = env.runtime_dir().join("dm-follow-race.jsonl");
    follow_until_armed_peer(&env, &out, "follow raced poke", &["--after", POST_0]).await;
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: wait --dm ownership collides with positional DM"]
async fn positional_dm_and_username_wait_share_owner() {
    let env = TestEnv::new("wait-dm-owner").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    env.mock_channel_lookup(&dm_name(), DM_ID).await;
    env.mock_channel_posts(DM_ID, &[]).await;
    mount_peer_user(&env).await;
    mount_direct_channel(&env).await;
    let daemon = spawn_daemon(&env).await;

    let first = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(["--json", "wait", &dm_name(), "--timeout", "30s"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn positional dm wait");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let second = run_chanvoy(
        &env,
        &["--json", "wait", "--dm", PEER_USER, "--timeout", "5s"],
    )
    .await;
    assert_eq!(second.status.code(), Some(2), "username wait must collide");
    let value: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(value["error"]["class"], "wait_already_active");
    let wait_id = value["error"]["existing_wait_id"]
        .as_str()
        .expect("existing wait id")
        .to_string();

    let replacement = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args([
            "--json",
            "wait",
            "--dm",
            PEER_USER,
            "--timeout",
            "3s",
            "--replace-wait",
            &wait_id,
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn replace wait");

    let first_out = first.wait_with_output().await.expect("displaced waiter");
    assert_eq!(first_out.status.code(), Some(2), "old waiter must exit 2");
    let first_json: serde_json::Value = serde_json::from_slice(&first_out.stdout).unwrap();
    assert_eq!(first_json["timeout"], false);
    assert_eq!(first_json["error"]["class"], "wait_replaced");
    assert_eq!(first_json["error"]["wait_id"], wait_id);
    assert!(first_json["error"]["replaced_by_wait_id"]
        .as_str()
        .is_some_and(|id| id.starts_with("wait_")));

    let replaced = replacement.wait_with_output().await.expect("replacement");
    assert_eq!(
        replaced.status.code(),
        Some(1),
        "replacement must clean-deadman; stdout={} stderr={}",
        String::from_utf8_lossy(&replaced.stdout),
        String::from_utf8_lossy(&replaced.stderr)
    );
    let replaced_json: serde_json::Value = serde_json::from_slice(&replaced.stdout).unwrap();
    assert_eq!(replaced_json["timeout"], true);
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: wait --dm deadline starts at RPC entry"]
async fn expired_deadline_refuses_before_direct_create() {
    let env = TestEnv::new("wait-dm-deadline").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    Mock::given(method("GET"))
        .and(path(format!("/api/v4/users/username/{PEER_USER}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(2))
                .set_body_json(json!({"id": PEER_ID, "username": PEER_USER})),
        )
        .mount(&env.mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v4/channels/direct"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&env.mock)
        .await;
    let daemon = spawn_daemon(&env).await;
    let output = run_chanvoy(
        &env,
        &["--json", "wait", "--dm", PEER_USER, "--timeout", "1s"],
    )
    .await;
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["timeout"], false);
    assert_ne!(value["error_class"], "input");
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: wait --dm help text"]
async fn wait_help_documents_dm() {
    let env = TestEnv::new("wait-dm-help").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    let output = run_chanvoy(&env, &["wait", "--help"]).await;
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains(WAIT_DM_HELP), "{help}");
    assert!(help.contains("--dm"), "{help}");
}

#[tokio::test]
#[ignore = "integration: wait --dm follow peer wake"]
async fn wait_dm_follow_emits_peer() {
    let env = TestEnv::new("wait-dm-follow-wake").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    mount_peer_user(&env).await;
    mount_direct_channel(&env).await;
    env.mock_post_lookup(POST_0, DM_ID, true).await;
    env.mock_channel_posts(
        DM_ID,
        &[(
            POST_1,
            PEER_ID,
            PEER_USER,
            "poke from peer",
            1_780_000_000_100,
        )],
    )
    .await;
    let daemon = spawn_daemon(&env).await;
    let out = env.runtime_dir().join("dm-follow.jsonl");
    follow_until_armed_peer(&env, &out, "poke from peer", &["--after", POST_0]).await;
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
#[ignore = "integration: wait --dm extreme timeout is hard input"]
async fn extreme_timeout_is_hard_input_for_oneshot_and_follow() {
    let env = TestEnv::new("wait-dm-extreme-timeout").await;
    env.write_default_profile(BOT_USER, "org-lanytehq");
    env.mock_baseline(BOT_ID, BOT_USER, "team-id-456").await;
    Mock::given(method("POST"))
        .and(path("/api/v4/channels/direct"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&env.mock)
        .await;
    let daemon = spawn_daemon(&env).await;

    let oneshot = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--dm",
            PEER_USER,
            "--timeout",
            "18446744073709551615s",
        ],
    )
    .await;
    assert_eq!(oneshot.status.code(), Some(2));
    let oneshot_json: serde_json::Value = serde_json::from_slice(&oneshot.stdout).unwrap();
    assert_eq!(oneshot_json["timeout"], false);
    assert_eq!(oneshot_json["error_class"], "input");
    assert!(oneshot_json["message"]
        .as_str()
        .unwrap_or_default()
        .contains("deadline"));

    let out = env.runtime_dir().join("extreme-follow.jsonl");
    let follow = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--dm",
            PEER_USER,
            "--timeout",
            "18446744073709551615s",
            "--follow",
            "--out",
            out.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(follow.status.code(), Some(2));
    let follow_json: serde_json::Value = serde_json::from_slice(&follow.stdout).unwrap();
    assert_eq!(follow_json["error_class"], "input");

    let rpc = raw_rpc(
        env.socket_path(),
        WAIT_DM_V1_METHOD,
        json!({
            "username": PEER_USER,
            "timeout_secs": u64::MAX
        }),
    )
    .await;
    assert_eq!(rpc.error.as_ref().map(|e| e.code), Some(-32007));

    let rpc_follow = raw_rpc(
        env.socket_path(),
        WAIT_DM_FOLLOW_V1_METHOD,
        json!({
            "username": PEER_USER,
            "timeout_secs": u64::MAX
        }),
    )
    .await;
    assert_eq!(rpc_follow.error.as_ref().map(|e| e.code), Some(-32007));

    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[test]
fn rpc_request_helper_compiles_for_wait_dm() {
    let request = rpc_request(
        WAIT_DM_V1_METHOD,
        json!({"username": PEER_USER, "timeout_secs": 1}),
    );
    assert_eq!(request.method, WAIT_DM_V1_METHOD);
    let _ = Duration::from_secs(1);
}
