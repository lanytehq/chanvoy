//! Owned roots/providers/processes for startup deadlines and diagnostic privacy.
#![allow(dead_code)]
mod common;

use common::TestEnv;
use std::{
    os::unix::fs::MetadataExt,
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};
use wiremock::{
    matchers::{header, method, path},
    Mock, ResponseTemplate,
};

fn plain_stderr(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let mut chars = text.chars().peekable();
    let mut plain = String::new();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for code in chars.by_ref() {
                if ('@'..='~').contains(&code) {
                    break;
                }
            }
        } else {
            plain.push(ch);
        }
    }
    plain
}

async fn local_rpc(env: &TestEnv, method: &str) -> chanvoy_core::JsonRpcResponse {
    let mut socket = UnixStream::connect(env.socket_path()).await.unwrap();
    let request = chanvoy_core::rpc_request(method, serde_json::json!({}));
    socket
        .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    BufReader::new(socket).read_line(&mut line).await.unwrap();
    serde_json::from_str(&line).unwrap()
}

fn serve_command(env: &TestEnv, trace: bool) -> tokio::process::Command {
    let mut command = env.chanvoy_command();
    command
        .args(["--profile", &env.profile_name, "--json", "daemon", "serve"])
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if trace {
        command.env("RUST_LOG", "info");
    }
    command
}

fn write_reduced_startup_profiles(env: &mut TestEnv) {
    env.set_extra_env("SYNTHETIC_FAMILY_TOKEN", "synthetic-secret-family-token");
    env.write_named_profile(
        &env.profile_name,
        "agent-synthetic",
        "org-synthetic",
        &env.token_env_name,
        Some("synthetic-family"),
    );
    env.write_named_profile(
        "synthetic-family",
        "agent-synthetic-family",
        "org-synthetic",
        "SYNTHETIC_FAMILY_TOKEN",
        None,
    );
}

#[tokio::test]
async fn bootstrap_uncertainty_and_owned_poison_have_distinct_safe_receipts() {
    for (case, reduced) in [
        ("unreadable", false),
        ("foreign-poison", false),
        ("missing-advertised", false),
        ("own-poison", false),
        ("unreadable", true),
        ("foreign-poison", true),
        ("missing-advertised", true),
    ] {
        let mut env = TestEnv::new("synthetic-bootstrap-receipt").await;
        if reduced {
            write_reduced_startup_profiles(&mut env);
        } else {
            env.write_default_profile("agent-synthetic", "org-synthetic");
        }
        let profile: chanvoy_core::Profile =
            toml::from_str(&std::fs::read_to_string(env.profile_path()).unwrap()).unwrap();
        let path = env
            .chanvoy_runtime_dir()
            .join(format!("{}.bootstrap.json", env.profile_name));
        let nonce = "synthetic-secret-own-nonce";
        if case != "missing-advertised" {
            let body = if case == "unreadable" {
                b"synthetic-secret-invalid-json".to_vec()
            } else {
                let mut state = chanvoy_core::build_bootstrap_state(
                    &profile,
                    "synthetic-secret-id",
                    nonce,
                    std::process::id(),
                )
                .unwrap();
                state.profile_fingerprint = "synthetic-secret-invalid-fingerprint".into();
                state.issued_at = 0;
                if case == "foreign-poison" {
                    state.nonce = "synthetic-secret-foreign-nonce".into();
                }
                serde_json::to_vec(&state).unwrap()
            };
            std::fs::write(&path, body).unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let before = std::fs::read(&path).ok();
        let inode = std::fs::symlink_metadata(&path).ok().map(|m| m.ino());
        let output = serve_command(&env, true)
            .env(chanvoy_core::BOOTSTRAP_NONCE_ENV, nonce)
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        let stderr = plain_stderr(&output.stderr);
        assert!(stderr.contains("bootstrap-identity"));
        assert!(
            stderr.contains(if case == "own-poison" {
                "invalid-input"
            } else {
                "local-unconfirmed"
            }),
            "{case}: {stderr}"
        );
        assert!(!stderr.contains("synthetic-secret"), "{case}: {stderr}");
        assert!(output.stdout.is_empty());
        assert!(!env.socket_path().exists());
        assert!(!env
            .chanvoy_runtime_dir()
            .join(format!("{}.pid", env.profile_name))
            .exists());
        if case == "own-poison" {
            assert!(!path.exists(), "own poison stays single-use");
        } else {
            assert_eq!(std::fs::read(&path).ok(), before);
            assert_eq!(
                std::fs::symlink_metadata(&path).ok().map(|m| m.ino()),
                inode
            );
        }
        assert!(
            env.mock.received_requests().await.unwrap().is_empty(),
            "bootstrap failure must precede manual whoami/bind"
        );
    }
}

#[tokio::test]
async fn dangling_or_nonregular_handoff_refuses_foreground_before_whoami() {
    for (kind, reduced) in [
        ("dangling", false),
        ("directory", false),
        ("dangling", true),
        ("directory", true),
    ] {
        let mut env = TestEnv::new("synthetic-nonregular-handoff").await;
        if reduced {
            write_reduced_startup_profiles(&mut env);
        } else {
            env.write_default_profile("agent-synthetic", "org-synthetic");
        }
        let path = env
            .chanvoy_runtime_dir()
            .join(format!("{}.bootstrap.json", env.profile_name));
        if kind == "dangling" {
            std::os::unix::fs::symlink("synthetic-absent-target", &path).unwrap();
        } else {
            std::fs::create_dir(&path).unwrap();
        }
        let inode = std::fs::symlink_metadata(&path).unwrap().ino();
        let output = serve_command(&env, true).output().await.unwrap();
        assert!(
            env.mock.received_requests().await.unwrap().is_empty(),
            "retained path must not authorize whoami"
        );
        let stderr = plain_stderr(&output.stderr);
        assert!(!output.status.success(), "{kind}: {stderr}");
        assert!(
            stderr.contains("bootstrap-identity") && stderr.contains("local-unconfirmed"),
            "{kind}: {stderr}"
        );
        assert_eq!(std::fs::symlink_metadata(&path).unwrap().ino(), inode);
        assert!(!env.socket_path().exists());
        assert!(!env
            .chanvoy_runtime_dir()
            .join(format!("{}.pid", env.profile_name))
            .exists());
        assert!(
            env.mock.received_requests().await.unwrap().is_empty(),
            "retained path must not authorize whoami"
        );
        assert!(output.stdout.is_empty());
    }
}

#[tokio::test]
async fn retained_foreign_handoff_blocks_spawn_and_doctor_names_path() {
    let env = TestEnv::new("synthetic-retained-handoff").await;
    env.write_default_profile("agent-synthetic", "org-synthetic");
    env.mock_baseline_for_team(
        "synthetic-id",
        "agent-synthetic",
        "synthetic-team",
        "org-synthetic",
    )
    .await;
    let path = env
        .chanvoy_runtime_dir()
        .join(format!("{}.bootstrap.json", env.profile_name));
    let body = b"synthetic-secret-foreign-nonce-and-body";
    std::fs::write(&path, body).unwrap();
    let inode = std::fs::symlink_metadata(&path).unwrap().ino();
    for verb in ["start", "auto-setup"] {
        let args = if verb == "start" {
            vec!["--profile", &env.profile_name, "--json", "daemon", "start"]
        } else {
            vec!["--profile", &env.profile_name, "--json", "auto-setup"]
        };
        let output = tokio::time::timeout(
            Duration::from_secs(8),
            env.chanvoy_command()
                .env("LANYTE_AGENT_ROLE", "bravo-devlead")
                .env("LANYTE_AGENT_SCOPE", "lanytehq")
                .env("LANYTE_MM_URL", env.server_url())
                .env("LANYTE_MM_TEAM", "org-synthetic")
                .args(args)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = plain_stderr(&output.stderr);
        assert!(!output.status.success(), "{stdout} {stderr}");
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["daemon_disposition"], "unresponsive-local");
        assert!(
            (stdout.contains("local-unconfirmed") && stdout.contains(path.to_str().unwrap()))
                || (stderr.contains("local-unconfirmed")
                    && stderr.contains(path.to_str().unwrap())),
            "{verb}: stdout={stdout}; stderr={stderr}"
        );
        assert!(!stdout.contains("synthetic-secret") && !stderr.contains("synthetic-secret"));
        assert_eq!(std::fs::read(&path).unwrap(), body);
        assert_eq!(std::fs::symlink_metadata(&path).unwrap().ino(), inode);
        assert!(!path.with_extension("json.tmp").exists());
        assert!(!env.socket_path().exists());
        assert!(!env
            .chanvoy_runtime_dir()
            .join(format!("{}.pid", env.profile_name))
            .exists());
    }
    let output = env
        .chanvoy_command()
        .args(["--profile", &env.profile_name, "--json", "doctor"])
        .output()
        .await
        .unwrap();
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(receipt["notes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|note| note
            .as_str()
            .is_some_and(|text| text.contains(path.to_str().unwrap())
                && text.contains("ownership/liveness")
                && text.contains("only observes"))));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("synthetic-secret"));
    assert_eq!(std::fs::read(&path).unwrap(), body);
    assert_eq!(std::fs::symlink_metadata(&path).unwrap().ino(), inode);
    // A spawned child would make its own whoami or consume/bind the handoff.
    // All requests here belong to parent start/auto-setup/doctor; producer
    // refusal occurs before the only spawn call in spawn_durable_daemon.
}

#[tokio::test]
async fn unresponsive_existing_socket_is_bounded_and_retained() {
    let env = TestEnv::new("synthetic-startup-existing").await;
    env.write_default_profile("agent-synthetic", "org-synthetic");
    let listener = UnixListener::bind(env.socket_path()).unwrap();
    let pid_path = chanvoy_core::pid_path_for_profile(&env.profile_name);
    // Derive the test path explicitly; never use the parent process's roots.
    let pid_path = env
        .chanvoy_runtime_dir()
        .join(pid_path.file_name().unwrap());
    std::fs::write(&pid_path, std::process::id().to_string()).unwrap();
    let socket_inode = std::fs::symlink_metadata(env.socket_path()).unwrap().ino();
    let pid_inode = std::fs::symlink_metadata(&pid_path).unwrap().ino();
    let pid_body = std::fs::read(&pid_path).unwrap();
    let accepting = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let _socket = socket;
        std::future::pending::<()>().await;
    });
    let started = Instant::now();
    let output = tokio::time::timeout(Duration::from_secs(3), serve_command(&env, true).output())
        .await
        .expect("local startup phase bound")
        .unwrap();
    assert!(!output.status.success());
    let stderr = plain_stderr(&output.stderr);
    assert!(stderr.contains("existing-socket") && stderr.contains("timeout"));
    let completed = stderr
        .lines()
        .find(|line| line.contains("startup phase completed") && line.contains("existing-socket"))
        .expect("local phase receipt");
    let elapsed: u64 = completed
        .split("elapsed_ms=")
        .nth(1)
        .unwrap()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap();
    eprintln!(
        "local phase elapsed_ms={elapsed}; whole fixture elapsed_ms={}",
        started.elapsed().as_millis()
    );
    assert!(
        elapsed < 1500,
        "local phase must not inherit the 2s remote budget: {stderr}"
    );
    assert!(stderr.contains("startup phase begin") && stderr.contains("budget_ms=750"));
    assert_eq!(
        std::fs::symlink_metadata(env.socket_path()).unwrap().ino(),
        socket_inode
    );
    assert_eq!(
        std::fs::symlink_metadata(&pid_path).unwrap().ino(),
        pid_inode
    );
    assert_eq!(std::fs::read(&pid_path).unwrap(), pid_body);
    assert!(
        output.stdout.is_empty(),
        "no false serve receipt or stage JSON"
    );
    accepting.abort();
}

#[tokio::test]
async fn foreground_failures_are_bounded_private_and_do_not_bind() {
    for (status, delay, expected, trace) in [
        (200, 5000, "timeout", true),
        (200, 5000, "timeout", false),
        (401, 0, "authoritative-refusal", true),
        (403, 0, "authoritative-refusal", true),
        (503, 0, "provider-unavailable", true),
        (200, 0, "identity-mismatch", true),
    ] {
        let env = TestEnv::new("synthetic-startup-refusal").await;
        env.write_default_profile("agent-synthetic", "org-synthetic");
        Mock::given(method("GET")).and(path("/api/v4/users/me"))
            .respond_with(ResponseTemplate::new(status).set_delay(Duration::from_millis(delay))
                .set_body_json(serde_json::json!({"id":"synthetic-id",
                    "username":"synthetic-secret-whoami", "message":"synthetic-secret-provider-body"})))
            .mount(&env.mock).await;
        let started = Instant::now();
        let output =
            tokio::time::timeout(Duration::from_secs(6), serve_command(&env, trace).output())
                .await
                .expect("foreground identity phase bound")
                .unwrap();
        let stderr = plain_stderr(&output.stderr);
        assert!(!output.status.success(), "{stderr}");
        assert!(started.elapsed() < Duration::from_secs(4));
        assert!(stderr.contains(expected), "{stderr}");
        assert!(
            !stderr.contains("synthetic-secret"),
            "provider/identity must not escape: {stderr}"
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("synthetic-secret"));
        assert!(
            output.stdout.is_empty(),
            "failure keeps existing stdout shape"
        );
        assert_eq!(stderr.contains("startup phase begin"), trace);
        assert!(!env.socket_path().exists());
        assert!(!env
            .chanvoy_runtime_dir()
            .join(format!("{}.pid", env.profile_name))
            .exists());
        assert_eq!(
            env.mock.received_requests().await.unwrap().len(),
            1,
            "one identity attempt"
        );
    }
}

#[tokio::test]
async fn one_second_foreground_identity_success_keeps_json_on_stdout() {
    let env = TestEnv::new("synthetic-startup-slow-success").await;
    env.write_default_profile("agent-synthetic", "org-synthetic");
    Mock::given(method("GET")).and(path("/api/v4/users/me"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(1))
            .set_body_json(serde_json::json!({"id":"synthetic-id","username":"agent-synthetic","is_bot":true})))
        .mount(&env.mock).await;
    let started = Instant::now();
    let child = serve_command(&env, true).spawn().unwrap();
    while !env.socket_path().exists() {
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "slow authoritative success must bind"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(started.elapsed() >= Duration::from_secs(1));
    let local = tokio::time::timeout(Duration::from_secs(1), local_rpc(&env, "profile_status"))
        .await
        .unwrap();
    assert!(local.result.is_some());
    let stop = local_rpc(&env, "shutdown").await;
    assert!(stop.result.is_some());
    let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice::<serde_json::Value>(&output.stdout)
        .expect("stdout is only the existing JSON receipt");
    let stderr = plain_stderr(&output.stderr);
    assert!(stderr.contains("foreground-identity") && stderr.contains("outcome=\"success\""));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("startup phase"));
}

#[tokio::test]
async fn family_identity_failure_is_private_and_never_creates_runtime() {
    for (status, delay, username, expected) in [
        (200, 5000, "synthetic-secret-family", "timeout"),
        (401, 0, "synthetic-secret-family", "authoritative-refusal"),
        (403, 0, "synthetic-secret-family", "authoritative-refusal"),
        (503, 0, "synthetic-secret-family", "provider-unavailable"),
        (200, 0, "synthetic-secret-wrong", "identity-mismatch"),
    ] {
        let mut env = TestEnv::new("synthetic-family-refusal").await;
        env.set_extra_env("SYNTHETIC_FAMILY_TOKEN", "synthetic-secret-token");
        env.write_named_profile(
            &env.profile_name,
            "agent-synthetic",
            "org-synthetic",
            &env.token_env_name,
            Some("synthetic-family"),
        );
        env.write_named_profile(
            "synthetic-family",
            "synthetic-secret-family",
            "org-synthetic",
            "SYNTHETIC_FAMILY_TOKEN",
            None,
        );
        Mock::given(method("GET"))
            .and(path("/api/v4/users/me"))
            .and(header("authorization", "Bearer synthetic-secret-token"))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_delay(Duration::from_millis(delay))
                    .set_body_json(
                        serde_json::json!({"id":"synthetic-family-id","username":username,
                    "message":"synthetic-secret-provider-body"}),
                    ),
            )
            .mount(&env.mock)
            .await;
        let output =
            tokio::time::timeout(Duration::from_secs(6), serve_command(&env, true).output())
                .await
                .expect("family phase bound")
                .unwrap();
        let stderr = plain_stderr(&output.stderr);
        assert!(!output.status.success(), "{stderr}");
        assert!(
            stderr.contains("reduce-family-identity") && stderr.contains(expected),
            "{stderr}"
        );
        assert!(!stderr.contains("synthetic-secret"));
        assert!(output.stdout.is_empty());
        assert!(!env.socket_path().exists());
        assert!(!env
            .chanvoy_runtime_dir()
            .join(format!("{}.pid", env.profile_name))
            .exists());
        assert_eq!(env.mock.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn successful_family_identity_is_not_logged() {
    let mut env = TestEnv::new("synthetic-family-success").await;
    env.set_extra_env("SYNTHETIC_FAMILY_TOKEN", "synthetic-secret-token");
    env.write_named_profile(
        &env.profile_name,
        "agent-synthetic",
        "org-synthetic",
        &env.token_env_name,
        Some("synthetic-family"),
    );
    env.write_named_profile(
        "synthetic-family",
        "synthetic-secret-family",
        "org-synthetic",
        "SYNTHETIC_FAMILY_TOKEN",
        None,
    );
    Mock::given(method("GET")).and(path("/api/v4/users/me"))
        .and(header("authorization", "Bearer synthetic-secret-token"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(1))
            .set_body_json(serde_json::json!({"id":"synthetic-family-id","username":"synthetic-secret-family"})))
        .mount(&env.mock).await;
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .and(header("authorization", "Bearer test-token-value"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"id":"synthetic-stream-id","username":"agent-synthetic"}),
        ))
        .mount(&env.mock)
        .await;
    let child = serve_command(&env, true).spawn().unwrap();
    let started = Instant::now();
    while !env.socket_path().exists() {
        assert!(started.elapsed() < Duration::from_secs(4));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert!(local_rpc(&env, "shutdown").await.result.is_some());
    let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(output.status.success());
    let stderr = plain_stderr(&output.stderr);
    assert!(!stderr.contains("synthetic-secret"), "{stderr}");
    serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap();
    let requests = env.mock.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r
                .headers
                .get("authorization")
                .is_some_and(|h| h == "Bearer synthetic-secret-token"))
            .count(),
        1
    );
}

#[tokio::test]
async fn parent_team_failure_keeps_typed_status_but_not_provider_body() {
    let env = TestEnv::new("synthetic-team-refusal").await;
    env.write_default_profile("agent-synthetic", "org-synthetic");
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"id":"synthetic-id","username":"agent-synthetic"}),
            ),
        )
        .mount(&env.mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v4/teams/name/org-synthetic"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(serde_json::json!({"message":"synthetic-secret-provider-body"})),
        )
        .mount(&env.mock)
        .await;
    let output = env
        .chanvoy_command()
        .env("RUST_LOG", "info")
        .args(["--profile", &env.profile_name, "--json", "daemon", "start"])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    let stderr = plain_stderr(&output.stderr);
    assert!(!stderr.contains("synthetic-secret"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("synthetic-secret"));
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_ne!(
        receipt["daemon_disposition"], "identity-refused",
        "team401 is not token-identity evidence"
    );
    assert!(!env.socket_path().exists());
}

#[tokio::test]
async fn parent_username_mismatch_is_structural_and_private() {
    let env = TestEnv::new("synthetic-parent-mismatch").await;
    env.write_default_profile("agent-synthetic", "org-synthetic");
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"id":"synthetic-id","username":"synthetic-secret-whoami"}),
        ))
        .mount(&env.mock)
        .await;
    let output = env
        .chanvoy_command()
        .env("RUST_LOG", "info")
        .args(["--profile", &env.profile_name, "--json", "daemon", "start"])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("synthetic-secret"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("synthetic-secret"));
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["daemon_disposition"], "identity-refused");
    assert!(!env.socket_path().exists());
}

#[tokio::test]
async fn foreground_cancellation_before_bind_leaves_no_listener() {
    let env = TestEnv::new("synthetic-startup-cancel").await;
    env.write_default_profile("agent-synthetic", "org-synthetic");
    Mock::given(method("GET"))
        .and(path("/api/v4/users/me"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(5))
                .set_body_json(
                    serde_json::json!({"id":"synthetic-id","username":"agent-synthetic"}),
                ),
        )
        .mount(&env.mock)
        .await;
    let mut child = serve_command(&env, true).spawn().unwrap();
    let started = Instant::now();
    while env.mock.received_requests().await.unwrap().is_empty() {
        assert!(started.elapsed() < Duration::from_secs(2));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child
        .kill()
        .await
        .expect("terminate only this harness-owned foreground child");
    assert!(child.try_wait().unwrap().is_some());
    assert!(!env.socket_path().exists());
    assert!(!env
        .chanvoy_runtime_dir()
        .join(format!("{}.pid", env.profile_name))
        .exists());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!env.socket_path().exists());
}

/// The owned synthetic provider observes connection closure; that evidence is
/// separate from client-future drop and is not a promise about real providers.
#[tokio::test]
async fn scoped_cancellation_has_measured_request_count_and_peak() {
    use std::sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    };
    use tokio::{io::AsyncReadExt, net::TcpListener, task::JoinSet};
    #[derive(Default)]
    struct Counts {
        starts: AtomicUsize,
        active: AtomicUsize,
        peak: AtomicUsize,
        canceled: AtomicUsize,
        delay_ms: AtomicU64,
    }
    struct Active(Arc<Counts>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
    struct Server(tokio::task::JoinHandle<()>);
    impl Drop for Server {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let counts = Arc::new(Counts::default());
    counts.delay_ms.store(1000, Ordering::SeqCst);
    let state = counts.clone();
    let _server = Server(tokio::spawn(async move {
        let mut requests = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((mut socket, _)) = accepted else { break; };
                    let state = state.clone();
                    requests.spawn(async move {
                        let mut header = Vec::new();
                        while !header.windows(4).any(|w| w == b"\r\n\r\n") {
                            let mut data = [0;1024];
                            let n = socket.read(&mut data).await.unwrap();
                            if n == 0 || header.len() > 8192 { return; }
                            header.extend_from_slice(&data[..n]);
                        }
                        state.starts.fetch_add(1, Ordering::SeqCst);
                        let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
                        state.peak.fetch_max(active, Ordering::SeqCst);
                        let _active = Active(state.clone());
                        let mut closed = [0;1];
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(state.delay_ms.load(Ordering::SeqCst))) => {},
                            _ = socket.read(&mut closed) => {
                                state.canceled.fetch_add(1, Ordering::SeqCst); return;
                            }
                        }
                        let body = r#"{"id":"synthetic-id","username":"agent-synthetic","is_bot":true}"#;
                        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",body.len());
                        let _ = socket.write_all(response.as_bytes()).await;
                    });
                },
                _ = requests.join_next(), if !requests.is_empty() => {},
            }
        }
    }));
    let env = TestEnv::new("synthetic-startup-request-count").await;
    env.write_default_profile("agent-synthetic", "org-synthetic");
    let mut profile: chanvoy_core::Profile = toml::from_str(
        &std::fs::read_to_string(
            env.chanvoy_config_dir()
                .join("profiles")
                .join(format!("{}.toml", env.profile_name)),
        )
        .unwrap(),
    )
    .unwrap();
    profile.server_url = url;
    let client = chanvoy_core::MattermostClient::new(&profile, "synthetic-token".into()).unwrap();
    let probe = || {
        chanvoy_core::startup::bounded(
            chanvoy_core::startup::Phase::ForegroundIdentity,
            &profile.name,
            chanvoy_core::startup::IDENTITY_BUDGET,
            client.whoami(),
            chanvoy_core::startup::classify_core_error,
        )
    };
    probe()
        .await
        .expect("1s provider succeeds within identity phase");
    counts.delay_ms.store(5000, Ordering::SeqCst);
    let error = probe().await.unwrap_err();
    assert_eq!(error.outcome, chanvoy_core::startup::Outcome::Timeout);
    let idle = async {
        while counts.active.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(1), idle)
        .await
        .expect("owned provider observed cancellation");
    assert!(tokio::time::timeout(Duration::from_millis(100), probe())
        .await
        .is_err());
    tokio::time::timeout(Duration::from_secs(1), async {
        while counts.active.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counts.starts.load(Ordering::SeqCst), 3);
    assert_eq!(counts.peak.load(Ordering::SeqCst), 1);
    assert_eq!(counts.canceled.load(Ordering::SeqCst), 2);
    eprintln!("owned provider attempts=3 peak=1 observed cancellations=2");
}
