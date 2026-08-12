//! Hermetic PER-039 fan-in wait (`wait_channels_v1`).
//!
//! CLI validation, capability skew, all-or-nothing resolve, first-match
//! source honesty, clean deadman vs hard error, cursor neutrality, and
//! single-channel v2 compatibility.

#![allow(dead_code)]

mod common;

use chanvoy_core::{rpc_error, JsonRpcRequest, WAIT_CHANNELS_V1_METHOD};
use common::{read_attention_state_bytes, run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::task::JoinHandle;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

async fn mock_teams(env: &TestEnv, teams: &[(&str, &str)]) {
    let body: Vec<serde_json::Value> = teams
        .iter()
        .map(|(id, name)| {
            json!({
                "id": id,
                "name": name,
                "display_name": name,
            })
        })
        .collect();
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me/teams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&env.mock)
        .await;
}

struct ArmFixture<'a> {
    name: &'a str,
    id: &'a str,
    after: Option<&'a str>,
    posts: &'a [(&'a str, &'a str, &'a str, &'a str, i64)],
}

async fn mount_two_arms(env: &TestEnv, a: ArmFixture<'_>, b: ArmFixture<'_>) {
    env.mock_baseline("bot-id-per039", "agent-bravo-devlead", "team-id-456")
        .await;
    mock_teams(env, &[("team-id-456", "org-lanytehq")]).await;
    env.mock_channel_lookup(a.name, a.id).await;
    env.mock_channel_lookup(b.name, b.id).await;
    if let Some(anchor) = a.after {
        env.mock_post_lookup(anchor, a.id, true).await;
    }
    if let Some(anchor) = b.after {
        env.mock_post_lookup(anchor, b.id, true).await;
    }
    env.mock_channel_posts(a.id, a.posts).await;
    env.mock_channel_posts(b.id, b.posts).await;
}

async fn fake_daemon_unknown(env: &TestEnv) -> JoinHandle<()> {
    let socket_path = env.socket_path();
    let listener = UnixListener::bind(&socket_path).expect("bind fake daemon");
    let path = socket_path.to_path_buf();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("read request");
        let request: JsonRpcRequest =
            serde_json::from_str(line.trim_end()).expect("decode request");
        assert_eq!(request.method, WAIT_CHANNELS_V1_METHOD);
        let response = rpc_error(request.id, -32601, "unknown method");
        writer
            .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
            .await
            .expect("write");
        let _ = std::fs::remove_file(path);
    })
}

#[tokio::test]
async fn ac_f1_cli_refuses_mixed_bare_and_counts() {
    let env = TestEnv::new("per-039-cli-shape").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");

    let mixed = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "brief-per-039",
            "--channel",
            "org-lanytehq/a",
            "--channel",
            "org-lanytehq/b",
            "--timeout",
            "2s",
        ],
    )
    .await;
    assert_eq!(mixed.status.code(), Some(2));
    let mixed_json: serde_json::Value = serde_json::from_slice(&mixed.stdout).unwrap();
    assert_eq!(mixed_json["timeout"], false);
    assert_eq!(mixed_json["error_class"], "input");
    assert_eq!(mixed_json["mode"], "fan_in");

    let one = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--channel",
            "org-lanytehq/a",
            "--timeout",
            "2s",
        ],
    )
    .await;
    assert_eq!(one.status.code(), Some(2));

    let bare = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--channel",
            "brief-a",
            "--channel",
            "org-lanytehq/b",
            "--timeout",
            "2s",
        ],
    )
    .await;
    assert_eq!(bare.status.code(), Some(2));
    let bare_json: serde_json::Value = serde_json::from_slice(&bare.stdout).unwrap();
    assert!(bare_json["message"]
        .as_str()
        .is_some_and(|m| m.contains("team/channel")));

    let unmatched = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--channel",
            "org-lanytehq/a",
            "--channel",
            "org-lanytehq/b",
            "--after-channel",
            "org-lanytehq/z=post1",
            "--timeout",
            "2s",
        ],
    )
    .await;
    assert_eq!(unmatched.status.code(), Some(2));
}

#[tokio::test]
async fn ac_f1_old_daemon_is_hard_capability_no_fallback() {
    let env = TestEnv::new("per-039-capability").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    let server = fake_daemon_unknown(&env).await;
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--channel",
            "org-lanytehq/release-floor",
            "--channel",
            "org-lanytehq/feature-brief",
            "--timeout",
            "2s",
        ],
    )
    .await;
    server.await.expect("fake daemon finished");
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["timeout"], false);
    assert_eq!(value["error_class"], "capability");
    let message = value["message"].as_str().unwrap_or("");
    assert!(
        message.contains("multi-channel wait") && message.contains("auto-setup"),
        "capability diagnostic must name cycle guidance: {message}"
    );
    assert!(
        !message.contains("wait_channel_v2"),
        "must not mention a v2 fallback: {message}"
    );
}

#[tokio::test]
async fn ac_f4_first_match_names_source_channel() {
    let env = TestEnv::new("per-039-match").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_two_arms(
        &env,
        ArmFixture {
            name: "release-floor",
            id: "chan-rel",
            after: Some("anchor-rel"),
            posts: &[],
        },
        ArmFixture {
            name: "feature-brief",
            id: "chan-feat",
            after: Some("anchor-feat"),
            posts: &[(
                "win-post",
                "reviewer-id",
                "reviewer",
                "ASSENT: fan-in match",
                1_780_000_000_300,
            )],
        },
    )
    .await;
    let daemon = spawn_daemon(&env).await;
    let matched = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--channel",
            "org-lanytehq/release-floor",
            "--channel",
            "org-lanytehq/feature-brief",
            "--after-channel",
            "org-lanytehq/release-floor=anchor-rel",
            "--after-channel",
            "org-lanytehq/feature-brief=anchor-feat",
            "--contains",
            "ASSENT",
            "--timeout",
            "5s",
        ],
    )
    .await;
    assert_eq!(matched.status.code(), Some(0), "{:?}", matched);
    let body: serde_json::Value = serde_json::from_slice(&matched.stdout).expect("json");
    assert_eq!(body["mode"], "fan_in");
    assert_eq!(body["matched_channel"]["channel"], "feature-brief");
    assert_eq!(body["matched_channel"]["team"], "org-lanytehq");
    assert_eq!(body["messages"].as_array().map(|a| a.len()), Some(1));
    assert_eq!(body["messages"][0]["id"], "win-post");
    assert!(body.get("timeout").is_none());
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn ac_f4_human_output_leads_with_matched_selector() {
    let env = TestEnv::new("per-039-human").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_two_arms(
        &env,
        ArmFixture {
            name: "release-floor",
            id: "chan-rel-h",
            after: Some("anchor-rel-h"),
            posts: &[],
        },
        ArmFixture {
            name: "feature-brief",
            id: "chan-feat-h",
            after: Some("anchor-feat-h"),
            posts: &[(
                "human-win",
                "reviewer-id",
                "reviewer",
                "ASSENT: human",
                1_780_000_000_400,
            )],
        },
    )
    .await;
    let daemon = spawn_daemon(&env).await;
    let human = run_chanvoy(
        &env,
        &[
            "wait",
            "--channel",
            "org-lanytehq/release-floor",
            "--channel",
            "org-lanytehq/feature-brief",
            "--after-channel",
            "org-lanytehq/release-floor=anchor-rel-h",
            "--after-channel",
            "org-lanytehq/feature-brief=anchor-feat-h",
            "--contains",
            "ASSENT",
            "--timeout",
            "5s",
        ],
    )
    .await;
    assert_eq!(human.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&human.stdout);
    let matched_at = stdout.find("matched org-lanytehq/feature-brief");
    let body_at = stdout.find("ASSENT: human");
    assert!(matched_at.is_some(), "source first: {stdout}");
    assert!(body_at.is_some(), "body present: {stdout}");
    assert!(
        matched_at.unwrap() < body_at.unwrap(),
        "matched selector must lead the post body: {stdout}"
    );
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn ac_f6_clean_deadman_is_exit_one_timeout_true() {
    let env = TestEnv::new("per-039-deadman").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_two_arms(
        &env,
        ArmFixture {
            name: "release-floor",
            id: "chan-rel-d",
            after: Some("anchor-rel-d"),
            posts: &[],
        },
        ArmFixture {
            name: "feature-brief",
            id: "chan-feat-d",
            after: Some("anchor-feat-d"),
            posts: &[],
        },
    )
    .await;
    let daemon = spawn_daemon(&env).await;
    let timed = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--channel",
            "org-lanytehq/release-floor",
            "--channel",
            "org-lanytehq/feature-brief",
            "--after-channel",
            "org-lanytehq/release-floor=anchor-rel-d",
            "--after-channel",
            "org-lanytehq/feature-brief=anchor-feat-d",
            "--contains",
            "ASSENT",
            "--timeout",
            "2s",
        ],
    )
    .await;
    assert_eq!(timed.status.code(), Some(1));
    let body: serde_json::Value = serde_json::from_slice(&timed.stdout).unwrap();
    assert_eq!(body["timeout"], true);
    assert_eq!(body["mode"], "fan_in");
    assert_eq!(body["timeout_secs"], 2);
    assert!(body.get("error_class").is_none());
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn ac_f2_missing_arm_fails_closed() {
    let env = TestEnv::new("per-039-all-or-nothing").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-per039-miss", "agent-bravo-devlead", "team-id-456")
        .await;
    mock_teams(&env, &[("team-id-456", "org-lanytehq")]).await;
    env.mock_channel_lookup("release-floor", "chan-rel-m").await;
    Mock::given(method("GET"))
        .and(path(
            "/api/v4/teams/team-id-456/channels/name/does-not-exist",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "status_code": 404,
            "message": "not found"
        })))
        .mount(&env.mock)
        .await;
    env.mock_channel_posts("chan-rel-m", &[]).await;
    let daemon = spawn_daemon(&env).await;
    let output = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--channel",
            "org-lanytehq/release-floor",
            "--channel",
            "org-lanytehq/does-not-exist",
            "--timeout",
            "5s",
        ],
    )
    .await;
    assert_eq!(output.status.code(), Some(2));
    let body: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["timeout"], false);
    let message = body["message"].as_str().unwrap_or("");
    assert!(
        message.contains("org-lanytehq/does-not-exist"),
        "must name the failing selector: {message}"
    );
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn ac_f8_attention_bytes_unchanged() {
    let env = TestEnv::new("per-039-no-mutate").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    mount_two_arms(
        &env,
        ArmFixture {
            name: "release-floor",
            id: "chan-rel-n",
            after: Some("anchor-rel-n"),
            posts: &[],
        },
        ArmFixture {
            name: "feature-brief",
            id: "chan-feat-n",
            after: Some("anchor-feat-n"),
            posts: &[],
        },
    )
    .await;
    let marker = br#"{"cursors":{},"updated_at":1}"#;
    std::fs::write(env.state_path(), marker).expect("seed state");
    let before = read_attention_state_bytes(&env);
    let daemon = spawn_daemon(&env).await;
    let _ = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--channel",
            "org-lanytehq/release-floor",
            "--channel",
            "org-lanytehq/feature-brief",
            "--after-channel",
            "org-lanytehq/release-floor=anchor-rel-n",
            "--after-channel",
            "org-lanytehq/feature-brief=anchor-feat-n",
            "--timeout",
            "2s",
        ],
    )
    .await;
    let after = read_attention_state_bytes(&env);
    assert_eq!(before, after, "wait must not mutate attention state");
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn ac_f9_cross_team_explicit_selectors() {
    let env = TestEnv::new("per-039-cross-team").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-per039-xt", "agent-bravo-devlead", "team-id-456")
        .await;
    mock_teams(
        &env,
        &[
            ("team-id-456", "org-lanytehq"),
            ("team-id-3l", "org-3leaps"),
        ],
    )
    .await;
    env.mock_channel_lookup_for_team("team-id-456", "release-floor", "chan-rel-x")
        .await;
    env.mock_channel_lookup_for_team("team-id-3l", "ops-updates", "chan-ops-x")
        .await;
    env.mock_post_lookup("anchor-rel-x", "chan-rel-x", true)
        .await;
    env.mock_post_lookup("anchor-ops-x", "chan-ops-x", true)
        .await;
    env.mock_channel_posts("chan-rel-x", &[]).await;
    env.mock_channel_posts(
        "chan-ops-x",
        &[(
            "xt-win",
            "reviewer-id",
            "reviewer",
            "ASSENT: cross-team",
            1_780_000_000_500,
        )],
    )
    .await;
    let daemon = spawn_daemon(&env).await;
    let matched = run_chanvoy(
        &env,
        &[
            "--json",
            "wait",
            "--channel",
            "org-lanytehq/release-floor",
            "--channel",
            "org-3leaps/ops-updates",
            "--after-channel",
            "org-lanytehq/release-floor=anchor-rel-x",
            "--after-channel",
            "org-3leaps/ops-updates=anchor-ops-x",
            "--contains",
            "ASSENT",
            "--timeout",
            "5s",
        ],
    )
    .await;
    assert_eq!(matched.status.code(), Some(0), "{:?}", matched);
    let body: serde_json::Value = serde_json::from_slice(&matched.stdout).unwrap();
    assert_eq!(body["matched_channel"]["team"], "org-3leaps");
    assert_eq!(body["matched_channel"]["channel"], "ops-updates");
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn single_channel_v2_shape_unchanged() {
    let env = TestEnv::new("per-039-v2-compat").await;
    env.write_default_profile("agent-bravo-devlead", "org-lanytehq");
    env.mock_baseline("bot-id-per039-v2", "agent-bravo-devlead", "team-id-456")
        .await;
    env.mock_channel_lookup("brief-per-038", "chan-v2").await;
    env.mock_post_lookup("anchor-v2", "chan-v2", true).await;
    env.mock_channel_posts(
        "chan-v2",
        &[(
            "v2-post",
            "reviewer-id",
            "reviewer",
            "ASSENT: still single",
            1_780_000_000_600,
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
            "anchor-v2",
        ],
    )
    .await;
    assert_eq!(matched.status.code(), Some(0), "{:?}", matched);
    let body: serde_json::Value = serde_json::from_slice(&matched.stdout).unwrap();
    assert_eq!(body["channel"], "brief-per-038");
    assert_eq!(body["messages"][0]["id"], "v2-post");
    assert!(body.get("mode").is_none());
    assert!(body.get("matched_channel").is_none());
    assert!(stop_daemon_cleanly(&env, daemon).await);
}
