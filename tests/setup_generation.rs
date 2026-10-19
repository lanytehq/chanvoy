//! Hermetic setup generation gates and persistence-preservation oracles.
#![allow(dead_code)]
mod common;

use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chanvoy_core::{JsonRpcRequest, Profile};
use common::{run_chanvoy, TestEnv};
use rsfulmen::schema_validation::{validate_instance_with_schema_file, FileSchemaOptions};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

async fn environment(name: &str) -> TestEnv {
    let mut env = TestEnv::new(name).await;
    env.write_default_profile("agent-test", "org-test");
    let mut profile: Profile =
        toml::from_str(&std::fs::read_to_string(env.profile_path()).unwrap()).unwrap();
    profile.role = "devlead".into();
    profile.scope = "test".into();
    std::fs::write(
        env.profile_path(),
        toml::to_string_pretty(&profile).unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(env.profile_path(), std::fs::Permissions::from_mode(0o600)).unwrap();
    env.mock_baseline_for_team("test-bot-id", "agent-test", "test-team-id", "org-test")
        .await;
    env.set_extra_env("LANYTE_AGENT_ROLE", "devlead");
    env.set_extra_env("LANYTE_AGENT_SCOPE", "test");
    env.set_extra_env("LANYTE_MM_URL", &env.server_url());
    env
}

async fn cli_pin(env: &TestEnv) -> Value {
    let result = run_chanvoy(env, &["--json", "version", "--extended"]).await;
    assert!(result.status.success());
    serde_json::from_slice::<Value>(&result.stdout).unwrap()["cli"].clone()
}

#[derive(Debug, PartialEq, Eq)]
struct FileState {
    path: PathBuf,
    bytes: Option<Vec<u8>>,
    stamp: Option<(u64, u64, u32, i64, i64)>,
}

fn file_state(path: &Path, socket: bool) -> FileState {
    let metadata = std::fs::symlink_metadata(path).ok();
    FileState {
        path: path.to_path_buf(),
        bytes: if socket {
            None
        } else {
            std::fs::read(path).ok()
        },
        stamp: metadata.map(|m| (m.dev(), m.ino(), m.uid(), m.mtime(), m.mtime_nsec())),
    }
}

fn persistence(env: &TestEnv) -> Vec<FileState> {
    [
        (env.profile_path(), false),
        (env.socket_path(), true),
        (
            env.runtime_dir().join(format!("{}.pid", env.profile_name)),
            false,
        ),
        (env.state_path(), false),
        (env.config_dir().join("active_profile"), false),
    ]
    .into_iter()
    .map(|(path, socket)| file_state(&path, socket))
    .collect()
}

struct Fixture {
    task: tokio::task::JoinHandle<()>,
    calls: Arc<Mutex<Vec<String>>>,
    waits: Arc<Mutex<Vec<String>>>,
    _replacement: Arc<Mutex<Option<std::os::unix::net::UnixListener>>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn fixture(env: &TestEnv, binary: Value, degraded: bool, unavailable: bool) -> Fixture {
    fixture_with_owner(env, binary, degraded, unavailable, "agent-test")
}

fn fixture_with_owner(
    env: &TestEnv,
    binary: Value,
    degraded: bool,
    unavailable: bool,
    owner: &str,
) -> Fixture {
    fixture_controlled(env, binary, degraded, unavailable, owner, false)
}

fn fixture_controlled(
    env: &TestEnv,
    binary: Value,
    degraded: bool,
    unavailable: bool,
    owner: &str,
    swap_socket: bool,
) -> Fixture {
    let listener = UnixListener::bind(env.socket_path()).unwrap();
    // This fixture is the test process, not a detached daemon. Do not attach a
    // daemon_guard: that guard would signal the pid named by this fixture.
    std::fs::write(
        env.runtime_dir().join(format!("{}.pid", env.profile_name)),
        std::process::id().to_string(),
    )
    .unwrap();
    std::fs::write(env.state_path(), b"synthetic cursor bytes").unwrap();
    std::fs::write(
        env.config_dir().join("active_profile"),
        b"other-test-profile",
    )
    .unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let waits = Arc::new(Mutex::new(vec!["synthetic-held-wait".into()]));
    let called = calls.clone();
    let held = waits.clone();
    let profile = env.profile_name.clone();
    let socket = env.socket_path();
    let state = env.state_path();
    let server_url = env.server_url();
    let owner = owner.to_string();
    let replacement = Arc::new(Mutex::new(None));
    let replacement_owner = replacement.clone();
    let swap_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let called = called.clone();
            let held = held.clone();
            let profile = profile.clone();
            let socket = socket.clone();
            let state = state.clone();
            let server_url = server_url.clone();
            let binary = binary.clone();
            let owner = owner.clone();
            let replacement_owner = replacement_owner.clone();
            let swap_once = swap_once.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                    return;
                }
                let request: JsonRpcRequest = serde_json::from_str(&line).unwrap();
                called.lock().unwrap().push(request.method.clone());
                if swap_socket
                    && request.method == "daemon_status"
                    && !swap_once.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    // External-actor fault injection on a test-owned endpoint;
                    // preserve the old listener so absence is not death proof.
                    std::fs::remove_file(&socket).unwrap();
                    *replacement_owner.lock().unwrap() =
                        Some(std::os::unix::net::UnixListener::bind(&socket).unwrap());
                }
                let result = match request.method.as_str() {
                    "daemon_observation" | "daemon_status" if unavailable => {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        return;
                    }
                    "daemon_observation" | "daemon_status" => json!({
                        "profile_name": profile, "socket_path": socket,
                        "mattermost_username": owner, "mattermost_ok": !degraded,
                        "remote_probe": if degraded { "timeout" } else { "verified" },
                        "identity_refused": false, "mattermost_identity_drift": false,
                        "ws_observation_admission_closed": false, "ws_connection_state": "healthy",
                        "ws_reconnect_count": 0, "binary": binary
                    }),
                    "profile_status" => {
                        json!({"profile_name":profile,"role":"devlead","scope":"test",
                        "provider":"mattermost","bot_username":"agent-test","server_url":server_url,"socket_path":socket})
                    }
                    "seed_cursors" => {
                        std::fs::write(state, b"seed effect must be detected").unwrap();
                        json!({"outcomes":[]})
                    }
                    "shutdown" => {
                        held.lock().unwrap().clear();
                        json!({})
                    }
                    method => panic!("unexpected fixture method {method}"),
                };
                let response = chanvoy_core::rpc_result(request.id, result);
                let _ = reader
                    .get_mut()
                    .write_all(
                        format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes(),
                    )
                    .await;
            });
        }
    });
    Fixture {
        task,
        calls,
        waits,
        _replacement: replacement,
    }
}

fn validate_generation(report: &Value) {
    let keys = [
        "cli_binary",
        "daemon_binary",
        "generation_match",
        "generation_reason",
        "cycled",
        "reasons",
    ];
    let mut fragment = serde_json::Map::new();
    for key in keys {
        fragment.insert(
            key.into(),
            report
                .get(key)
                .unwrap_or_else(|| panic!("missing {key}"))
                .clone(),
        );
    }
    let schema = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/setup-generation/generation.schema.json");
    let issues = validate_instance_with_schema_file(
        &schema,
        &Value::Object(fragment),
        FileSchemaOptions::default(),
    )
    .unwrap();
    assert!(issues.is_empty(), "{issues:?}");
}

async fn refuses_preserving(
    name: &str,
    kind: &str,
    degraded: bool,
    unavailable: bool,
    human: bool,
) {
    let env = environment(name).await;
    let mut binary = cli_pin(&env).await;
    match kind {
        "commit_mismatch" => binary["commit"] = json!("b".repeat(40)),
        "dirty_mismatch" => {
            binary["dirty"] = json!(!binary["dirty"].as_bool().expect("known CLI dirty flag"))
        }
        "dirty_unknown" => binary["dirty"] = Value::Null,
        "pin_missing" => binary = Value::Null,
        "commit_missing" => {
            binary.as_object_mut().unwrap().remove("commit");
        }
        "commit_null" => binary["commit"] = Value::Null,
        "pin_invalid" => binary = json!("unusable-provider-body-marker"),
        _ => panic!("unknown fixture kind"),
    }
    let server = fixture(&env, binary, degraded, unavailable);
    let before = persistence(&env);
    let wait_before = server.waits.lock().unwrap().clone();
    let process_before = sysprims_proc::get_process(std::process::id())
        .unwrap()
        .start_time_unix_ms;
    let args: &[&str] = if human {
        &["auto-setup", "--activate"]
    } else {
        &["--json", "auto-setup", "--activate"]
    };
    let output = tokio::time::timeout(Duration::from_secs(15), run_chanvoy(&env, args))
        .await
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(6),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(persistence(&env), before);
    assert_eq!(*server.waits.lock().unwrap(), wait_before);
    assert_eq!(
        sysprims_proc::get_process(std::process::id())
            .unwrap()
            .start_time_unix_ms,
        process_before
    );
    assert!(!server
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|m| m == "seed_cursors" || m == "shutdown"));
    if human {
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.starts_with("generation mismatch:") || stderr.starts_with("generation unknown:")
        );
        assert!(!stderr.contains("daemon restart") && !stderr.contains("&&"));
    } else {
        assert!(
            output.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        let expected = if unavailable || matches!(kind, "commit_missing" | "commit_null") {
            "pin_missing"
        } else {
            kind
        };
        assert_eq!(report["generation_reason"], expected);
        assert_eq!(report["reasons"], json!([expected]));
        assert_eq!(
            report["generation_match"],
            if matches!(expected, "commit_mismatch" | "dirty_mismatch") {
                json!(false)
            } else {
                Value::Null
            }
        );
        assert_eq!(report["cycled"], false);
        validate_generation(&report);
        assert!(!String::from_utf8_lossy(&output.stdout).contains("unusable-provider-body-marker"));
    }
}

#[tokio::test]
async fn mismatched_healthy_daemon_is_preserved_before_activation_or_seed() {
    refuses_preserving(
        "generation-healthy-mismatch",
        "commit_mismatch",
        false,
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn mismatched_degraded_daemon_cannot_take_preserved_success_path() {
    refuses_preserving(
        "generation-degraded-mismatch",
        "commit_mismatch",
        true,
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn degraded_unavailable_observation_is_pin_missing_not_success() {
    refuses_preserving(
        "generation-degraded-unavailable",
        "pin_missing",
        true,
        true,
        false,
    )
    .await;
}

#[tokio::test]
async fn degraded_malformed_pin_is_pin_invalid_not_missing() {
    refuses_preserving(
        "generation-degraded-malformed",
        "pin_invalid",
        true,
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn missing_dirty_evidence_refuses_and_preserves() {
    refuses_preserving(
        "generation-dirty-unknown",
        "dirty_unknown",
        false,
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn missing_commit_refuses_as_incomplete_pin() {
    refuses_preserving(
        "generation-missing-commit",
        "commit_missing",
        false,
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn null_commit_refuses_as_incomplete_pin() {
    refuses_preserving("generation-null-commit", "commit_null", false, false, false).await;
}

#[tokio::test]
async fn matched_pins_do_not_authorize_reuse_after_candidate_swap() {
    let env = environment("generation-candidate-swap").await;
    let binary = cli_pin(&env).await;
    let fixture = fixture_controlled(&env, binary, false, false, "agent-test", true);
    let paths = [
        env.profile_path(),
        env.state_path(),
        env.config_dir().join("active_profile"),
        env.runtime_dir().join(format!("{}.pid", env.profile_name)),
    ];
    let before: Vec<_> = paths.iter().map(|path| file_state(path, false)).collect();
    let output = run_chanvoy(&env, &["--json", "auto-setup", "--activate"]).await;
    assert_eq!(
        output.status.code(),
        Some(6),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["generation_reason"], "candidate_unconfirmed");
    assert!(report["generation_match"].is_null());
    assert_eq!(report["cycled"], false);
    assert!(!report["message"]
        .as_str()
        .unwrap()
        .contains("Follow the confirmed"));
    validate_generation(&report);
    assert_eq!(
        paths
            .iter()
            .map(|path| file_state(path, false))
            .collect::<Vec<_>>(),
        before
    );
    assert!(!fixture
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|method| matches!(method.as_str(), "shutdown" | "seed_cursors")));
}

#[tokio::test]
async fn legacy_missing_pin_refuses_and_preserves() {
    refuses_preserving("generation-legacy-pin", "pin_missing", false, false, false).await;
}

#[tokio::test]
async fn dirty_mismatch_preserves_every_owned_surface() {
    refuses_preserving(
        "generation-dirty-mismatch",
        "dirty_mismatch",
        false,
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn nonownable_mismatch_names_owner_without_stealing() {
    let env = environment("generation-foreign-mismatch").await;
    let mut binary = cli_pin(&env).await;
    binary["commit"] = json!("b".repeat(40));
    let peer = fixture_with_owner(&env, binary, false, false, "other-fixture-owner");
    let before = persistence(&env);
    let output = run_chanvoy(&env, &["--json", "auto-setup", "--activate"]).await;
    assert_eq!(
        output.status.code(),
        Some(6),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["generation_match"], false);
    assert!(report["message"]
        .as_str()
        .unwrap()
        .contains("other-fixture-owner"));
    assert!(!report["message"]
        .as_str()
        .unwrap()
        .contains("Follow the confirmed"));
    assert_eq!(persistence(&env), before);
    assert!(!peer
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|method| matches!(method.as_str(), "shutdown" | "seed_cursors")));
}

#[tokio::test]
async fn authoritative_team_refusal_precedes_generation_mismatch() {
    let env = environment("generation-team-refusal").await;
    let mut binary = cli_pin(&env).await;
    binary["commit"] = json!("b".repeat(40));
    let peer = fixture(&env, binary, false, false);
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/api/v4/teams/name/org-test"))
        .respond_with(
            wiremock::ResponseTemplate::new(403)
                .set_body_json(json!({"message":"unusable-provider-body-marker"})),
        )
        .with_priority(1)
        .mount(&env.mock)
        .await;
    let before = persistence(&env);
    let output = run_chanvoy(&env, &["--json", "auto-setup", "--activate"]).await;
    assert_eq!(output.status.code(), Some(3));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["cycled"], false);
    validate_generation(&report);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("unusable-provider-body-marker"));
    assert_eq!(persistence(&env), before);
    assert!(!peer
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|method| matches!(method.as_str(), "shutdown" | "seed_cursors")));
}

#[tokio::test]
async fn human_refusal_has_no_stdout_success_or_dead_recipe() {
    refuses_preserving(
        "generation-human-refusal",
        "commit_mismatch",
        false,
        false,
        true,
    )
    .await;
}

#[tokio::test]
async fn matching_generation_reuses_and_status_reports_both_pins() {
    let env = environment("generation-matching").await;
    let pin = cli_pin(&env).await;
    let server = fixture(&env, pin.clone(), false, false);
    let pid_before =
        std::fs::read(env.runtime_dir().join(format!("{}.pid", env.profile_name))).unwrap();
    let output = run_chanvoy(&env, &["--json", "auto-setup"]).await;
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["generation_match"], true);
    assert_eq!(report["generation_reason"], "match");
    assert_eq!(report["cycled"], false);
    validate_generation(&report);
    assert_eq!(
        std::fs::read(env.runtime_dir().join(format!("{}.pid", env.profile_name))).unwrap(),
        pid_before
    );
    assert!(server
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|m| m == "seed_cursors"));
    let status = run_chanvoy(&env, &["--json", "daemon", "status"]).await;
    assert_eq!(status.status.code(), Some(0));
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["cli_binary"], pin);
    assert_eq!(status["binary"], pin);
    assert_eq!(status["generation_match"], true);
}

#[test]
fn schema_rejects_collapsed_tri_state_and_duplicate_reasons() {
    let pin = serde_json::to_value(chanvoy_core::resolve_host_build_info()).unwrap();
    let schema = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/setup-generation/generation.schema.json");
    for (reason, verdict) in [
        ("match", json!(true)),
        ("commit_mismatch", json!(false)),
        ("dirty_mismatch", json!(false)),
        ("pin_missing", Value::Null),
        ("pin_invalid", Value::Null),
        ("dirty_unknown", Value::Null),
        ("candidate_unconfirmed", Value::Null),
    ] {
        let fixture = json!({"cli_binary":pin,"daemon_binary":pin,"generation_match":verdict,
            "generation_reason":reason,"cycled":false,"reasons":[reason]});
        assert!(validate_instance_with_schema_file(
            &schema,
            &fixture,
            FileSchemaOptions::default()
        )
        .unwrap()
        .is_empty());
        let mut collapsed = fixture.clone();
        collapsed["generation_match"] = if reason == "match" {
            Value::Null
        } else {
            json!(true)
        };
        assert!(
            !validate_instance_with_schema_file(&schema, &collapsed, FileSchemaOptions::default())
                .unwrap()
                .is_empty(),
            "collapsed {reason}"
        );
        let mut duplicate = fixture;
        duplicate["reasons"] = json!([reason, reason]);
        assert!(
            !validate_instance_with_schema_file(&schema, &duplicate, FileSchemaOptions::default())
                .unwrap()
                .is_empty(),
            "duplicated {reason}"
        );
    }
}

#[test]
fn schema_rejects_missing_verdict_and_malformed_fragments() {
    let pin = serde_json::to_value(chanvoy_core::resolve_host_build_info()).unwrap();
    let base = json!({"cli_binary":pin,"daemon_binary":pin,"generation_match":true,
        "generation_reason":"match","cycled":false,"reasons":["match"]});
    let schema = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/setup-generation/generation.schema.json");
    let mut fixtures = Vec::new();
    let mut missing = base.clone();
    missing.as_object_mut().unwrap().remove("generation_match");
    fixtures.push(missing);
    for (key, value) in [
        ("generation_match", json!("true")),
        ("generation_reason", json!("new_reason")),
        ("unknown_field", json!(true)),
    ] {
        let mut invalid = base.clone();
        invalid[key] = value;
        fixtures.push(invalid);
    }
    let mut invalid_build = base;
    invalid_build["daemon_binary"]["dirty"] = json!("false");
    fixtures.push(invalid_build);
    for invalid in fixtures {
        assert!(!validate_instance_with_schema_file(
            &schema,
            &invalid,
            FileSchemaOptions::default()
        )
        .unwrap()
        .is_empty());
    }
}

async fn native_refresh_case(fail_successor: bool) {
    let mut env = environment(if fail_successor {
        "generation-refresh-failure"
    } else {
        "generation-refresh-success"
    })
    .await;
    env.mock_empty_memberships("test-team-id").await;
    env.set_extra_env("LEGACY_FIXTURE_TOKEN", "test-token-legacy");
    let mut profile: Profile =
        toml::from_str(&std::fs::read_to_string(env.profile_path()).unwrap()).unwrap();
    profile.env_name = "LEGACY_FIXTURE_TOKEN".into();
    std::fs::write(
        env.profile_path(),
        toml::to_string_pretty(&profile).unwrap(),
    )
    .unwrap();
    let _guard = env.daemon_guard();
    let mut predecessor = common::spawn_daemon(&env).await;
    let predecessor_pid = predecessor.id().unwrap();
    if fail_successor {
        // Sticky operator configuration passes parent credential/team validation,
        // but the new daemon cannot load this absent reduction target. The old
        // daemon already loaded the earlier, valid profile.
        profile.reduce = Some(chanvoy_core::ReducePolicy {
            use_profile: "absent-fixture-target".into(),
        });
        std::fs::write(
            env.profile_path(),
            toml::to_string_pretty(&profile).unwrap(),
        )
        .unwrap();
    }
    let output = run_chanvoy(&env, &["--json", "auto-setup"]).await;
    assert_eq!(
        output.status.code(),
        Some(if fail_successor { 4 } else { 0 }),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(3), predecessor.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["cycled"], !fail_successor);
    assert_eq!(report["predecessor_generation_match"], true);
    assert_eq!(report["reasons"][0], "profile_refresh");
    validate_generation(&report);
    if fail_successor {
        assert_eq!(report["error_code"], "daemon_start");
        assert!(report["generation_match"].is_null());
        assert_eq!(report["generation_reason"], "pin_missing");
        assert_eq!(report["reasons"], json!(["profile_refresh", "pin_missing"]));
        assert!(!env.socket_path().exists());
    } else {
        assert_eq!(report["generation_match"], true);
        assert_eq!(report["reasons"], json!(["profile_refresh", "match"]));
        let successor: u32 =
            std::fs::read_to_string(env.runtime_dir().join(format!("{}.pid", env.profile_name)))
                .unwrap()
                .trim()
                .parse()
                .unwrap();
        assert_ne!(successor, predecessor_pid);
        assert!(sysprims_proc::get_process(successor).is_ok());
        assert!(run_chanvoy(&env, &["daemon", "stop"])
            .await
            .status
            .success());
    }
}

#[tokio::test]
async fn native_profile_refresh_reports_confirmed_predecessor_and_successor() {
    native_refresh_case(false).await;
}

#[tokio::test]
async fn native_stop_success_then_successor_failure_is_not_a_cycle() {
    native_refresh_case(true).await;
}

#[tokio::test]
async fn matching_degraded_generation_keeps_existing_preserved_success() {
    let env = environment("generation-matching-degraded").await;
    let pin = cli_pin(&env).await;
    let server = fixture(&env, pin, true, false);
    let before = persistence(&env);
    let output = run_chanvoy(&env, &["--json", "auto-setup"]).await;
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["generation_match"], true);
    assert_eq!(report["daemon_disposition"], "degraded-remote");
    validate_generation(&report);
    assert_eq!(persistence(&env), before);
    assert!(!server
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|m| m == "seed_cursors"));
}

#[tokio::test]
async fn profile_refresh_mismatch_keeps_established_stop_failure_precedence() {
    let env = environment("generation-refresh-mismatch").await;
    let mut profile: Profile =
        toml::from_str(&std::fs::read_to_string(env.profile_path()).unwrap()).unwrap();
    profile.env_name = "OLD_FIXTURE_TOKEN".into();
    std::fs::write(
        env.profile_path(),
        toml::to_string_pretty(&profile).unwrap(),
    )
    .unwrap();
    let mut binary = cli_pin(&env).await;
    binary["commit"] = json!("b".repeat(40));
    let peer = fixture(&env, binary, false, false);
    let output = run_chanvoy(&env, &["--json", "auto-setup"]).await;
    // Native retirement rejects this integration-test executable as a daemon;
    // Refresh must reach that existing guard, not take generation-only exit 6.
    assert_eq!(
        output.status.code(),
        Some(4),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["error_code"], "daemon_refresh_stop");
    assert_eq!(report["cycled"], false);
    assert_eq!(report["generation_match"], false);
    assert_eq!(report["predecessor_generation_reason"], "commit_mismatch");
    assert_eq!(
        report["reasons"],
        json!(["profile_refresh", "commit_mismatch"])
    );
    validate_generation(&report);
    assert!(!peer
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|method| method == "shutdown"));
}

#[tokio::test]
async fn positive_bot_drift_with_mismatch_keeps_identity_refresh_precedence() {
    let env = environment("generation-bot-refresh-mismatch").await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/api/v4/users/me"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
            json!({"id":"replacement-test-bot", "username":"replacement-test-agent", "is_bot":true})))
        .with_priority(1).mount(&env.mock).await;
    let mut binary = cli_pin(&env).await;
    binary["commit"] = json!("b".repeat(40));
    let peer = fixture(&env, binary, false, false);
    let output = run_chanvoy(&env, &["--json", "auto-setup"]).await;
    assert_eq!(
        output.status.code(),
        Some(4),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["error_code"], "daemon_refresh_stop");
    assert_eq!(report["cycled"], false);
    assert_eq!(
        report["reasons"],
        json!(["bot_identity_refresh", "commit_mismatch"])
    );
    assert_eq!(report["predecessor_generation_match"], false);
    validate_generation(&report);
    let persisted: Profile =
        toml::from_str(&std::fs::read_to_string(env.profile_path()).unwrap()).unwrap();
    assert_eq!(persisted.bot_username, "replacement-test-agent");
    assert!(!peer
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|method| method == "shutdown"));
}
