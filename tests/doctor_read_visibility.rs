//! CHAN-TASK-003 — hermetic `chanvoy doctor` probes.
//!
//! Covers server-time Date observation, redacted identity failures, optional
//! channel resolve, JSON shape, and attention-state non-mutation. Pure skew
//! arithmetic lives in `chanvoy_core::doctor` unit tests.

#![allow(dead_code)]

mod common;

use common::{read_attention_state_bytes, run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

/// Happy path: Date header present → clock healthy; identity pass; channel resolve.
#[tokio::test]
async fn doctor_reports_healthy_clock_and_channel() {
    let env = TestEnv::new("doctor-healthy").await;
    env.write_default_profile("agent-test", "org-lanytehq");
    mount_whoami_with_date(&env, "bot-id", "agent-test", 0).await;
    mount_primary_team(&env).await;
    env.mock_channel_lookup("ops-updates", "chan-ops").await;

    let daemon = spawn_daemon(&env).await;
    let before = read_attention_state_bytes(&env);

    let output = run_chanvoy(&env, &["--json", "doctor", "ops-updates"]).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stdout={stdout} stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let after = read_attention_state_bytes(&env);
    assert_eq!(
        before, after,
        "doctor must not mutate the attention state file"
    );

    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(v["exit_code"], 0, "report={stdout}");
    assert_eq!(v["identity"]["ok"], true);
    assert_eq!(v["identity"]["username"], "agent-test");
    assert_eq!(v["clock"]["verdict"], "healthy");
    assert_eq!(v["clock"]["source"], "http_date");
    assert!(v["clock"]["server_ms"].as_i64().is_some());
    assert_eq!(v["channel"]["check"], "pass");
    assert_eq!(v["channel"]["resolved_name"], "ops-updates");
    let dumped = stdout.to_lowercase();
    assert!(!dumped.contains("request_id"));
    assert!(!dumped.contains("detailed_error"));
    assert!(!dumped.contains("app_error"));

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// Local clock far ahead of Date header → suspected_ahead + guidance.
#[tokio::test]
async fn doctor_reports_suspected_ahead_when_date_is_old() {
    let env = TestEnv::new("doctor-ahead").await;
    env.write_default_profile("agent-test", "org-lanytehq");
    // Date two minutes in the past → residual >> 30s.
    mount_whoami_with_date(&env, "bot-id", "agent-test", -120).await;
    mount_primary_team(&env).await;

    let daemon = spawn_daemon(&env).await;
    let output = run_chanvoy(&env, &["--json", "doctor"]).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(v["exit_code"], 1);
    assert_eq!(v["clock"]["verdict"], "suspected_ahead");
    assert_eq!(v["clock"]["check"], "warn");
    let guidance = v["clock"]["guidance"].as_str().unwrap_or("");
    assert!(
        guidance.contains("--after") && guidance.contains("check"),
        "guidance={guidance}"
    );

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// Unparseable Date header → clock unavailable (never healthy greenwash).
///
/// Wiremock injects a real `Date` on responses by default, so the
/// "header omitted" path is hard to hermetically force; invalid Date is
/// the durable unavailable case under test.
#[tokio::test]
async fn doctor_clock_unavailable_on_invalid_date_header() {
    let env = TestEnv::new("doctor-bad-date").await;
    env.write_default_profile("agent-test", "org-lanytehq");
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Date", "not-a-valid-http-date")
                .set_body_json(serde_json::json!({
                    "id": "bot-id",
                    "username": "agent-test",
                    "is_bot": true,
                    "nickname": null,
                    "email": null,
                })),
        )
        .mount(&env.mock)
        .await;
    mount_primary_team(&env).await;

    let daemon = spawn_daemon(&env).await;
    let output = run_chanvoy(&env, &["--json", "doctor"]).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(v["clock"]["verdict"], "unavailable");
    assert_eq!(v["clock"]["check"], "unavailable");
    assert!(v["clock"]["delta_ms"].is_null() || v["clock"].get("delta_ms").is_none());
    assert_eq!(v["identity"]["ok"], true);

    let _ = stop_daemon_cleanly(&env, daemon).await;
}

/// Profile bot_username ≠ whoami username → identity fail (no greenwash).
#[tokio::test]
async fn doctor_identity_mismatches_profile_bot() {
    let env = TestEnv::new("doctor-id-mismatch").await;
    env.write_default_profile("agent-expected", "org-lanytehq");
    // Provider authenticates as a different bot than the profile claims.
    mount_whoami_with_date(&env, "bot-id", "agent-actual", 0).await;
    mount_primary_team(&env).await;

    let output = run_chanvoy(&env, &["--json", "doctor"]).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={stdout} stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(v["identity"]["ok"], false);
    assert_eq!(v["identity"]["status_class"], "identity_mismatch");
    assert_eq!(v["identity"]["username"], "agent-actual");
    let reason = v["identity"]["reason"].as_str().unwrap_or("");
    assert!(reason.contains("agent-expected"), "reason={reason}");
}

/// 401 identity → fail + redacted reason; clock unavailable; exit 2.
///
/// No daemon: doctor still runs the direct core whoami probe and reports
/// daemon unreachable separately.
#[tokio::test]
async fn doctor_identity_401_is_hard_fail_redacted() {
    let env = TestEnv::new("doctor-401").await;
    env.write_default_profile("agent-test", "org-lanytehq");
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "id": "api.context.session_expired.app_error",
            "message": "Invalid or expired session, please login again.",
            "detailed_error": "token leaked-secret-should-not-appear",
            "request_id": "req-should-not-leak",
            "status_code": 401
        })))
        .mount(&env.mock)
        .await;

    let output = run_chanvoy(&env, &["--json", "doctor"]).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={stdout} stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(v["exit_code"], 2);
    assert_eq!(v["identity"]["ok"], false);
    assert_eq!(v["identity"]["status_class"], "credential_or_forbidden");
    assert!(!stdout.contains("leaked-secret"));
    assert!(!stdout.contains("req-should-not-leak"));
    assert!(!stdout.contains("detailed_error"));
}

/// Channel resolve surfaces status classes without provider bodies.
#[tokio::test]
async fn doctor_channel_http_classes_redacted() {
    for (status, class_substr, exit) in [(403, "credential_or_forbidden", 2), (429, "throttled", 1)]
    {
        let env = TestEnv::new(&format!("doctor-ch-{status}")).await;
        env.write_default_profile("agent-test", "org-lanytehq");
        mount_whoami_with_date(&env, "bot-id", "agent-test", 0).await;
        mount_primary_team(&env).await;
        Mock::given(method("GET"))
            .and(path("/api/v4/teams/team-id-456/channels/name/secret-chan"))
            .respond_with(
                ResponseTemplate::new(status).set_body_json(serde_json::json!({
                    "message": "provider body must not leak",
                    "request_id": "nope",
                })),
            )
            .mount(&env.mock)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v4/users/me/teams"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": "team-id-456", "name": "org-lanytehq"}
            ])))
            .mount(&env.mock)
            .await;

        let daemon = spawn_daemon(&env).await;
        let output = run_chanvoy(&env, &["--json", "doctor", "secret-chan"]).await;
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert_eq!(
            output.status.code(),
            Some(exit),
            "status={status} stdout={stdout} stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
        let sc = v["channel"]["status_class"].as_str().unwrap_or("");
        assert_eq!(
            sc, class_substr,
            "status={status} status_class={sc} full={stdout}"
        );
        assert!(!stdout.contains("must not leak"));
        assert!(!stdout.contains("\"request_id\""));
        let _ = stop_daemon_cleanly(&env, daemon).await;
    }
}

/// Human output mentions catch-up guidance on suspected_ahead.
#[tokio::test]
async fn doctor_human_points_at_catch_up_on_skew() {
    let env = TestEnv::new("doctor-human-skew").await;
    env.write_default_profile("agent-test", "org-lanytehq");
    mount_whoami_with_date(&env, "bot-id", "agent-test", -180).await;
    mount_primary_team(&env).await;

    let daemon = spawn_daemon(&env).await;
    let output = run_chanvoy(&env, &["doctor"]).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("suspected_ahead") || stdout.contains("guidance:"),
        "stdout={stdout}"
    );
    assert!(
        stdout.contains("--after") || stdout.contains("check"),
        "stdout={stdout}"
    );
    let _ = stop_daemon_cleanly(&env, daemon).await;
}

async fn mount_primary_team(env: &TestEnv) {
    Mock::given(method("GET"))
        .and(path("/api/v4/teams/name/org-lanytehq"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"id": "team-id-456", "name": "org-lanytehq"})),
        )
        .mount(&env.mock)
        .await;
}

async fn mount_whoami_with_date(
    env: &TestEnv,
    bot_id: &str,
    bot_username: &str,
    date_offset_secs: i64,
) {
    let server_date = http_date_now_offset_secs(date_offset_secs);
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Date", server_date.as_str())
                .set_body_json(serde_json::json!({
                    "id": bot_id,
                    "username": bot_username,
                    "is_bot": true,
                    "nickname": null,
                    "email": null,
                })),
        )
        .mount(&env.mock)
        .await;
}

/// Format an HTTP-date roughly `offset_secs` from now (UTC).
fn http_date_now_offset_secs(offset_secs: i64) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let ts = now + offset_secs;
    let dt = chrono::DateTime::from_timestamp(ts, 0).expect("timestamp");
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}
