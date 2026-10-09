//! Synthetic provider and real daemon lifecycle regression evidence.
#![allow(dead_code)]
mod common;
use common::{run_chanvoy, spawn_daemon, stop_daemon_cleanly, TestEnv};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, UnixStream},
};

#[derive(Default)]
struct ProviderState {
    delay: AtomicU64,
    status: AtomicU16,
    wrong: AtomicBool,
    ws_closed: AtomicBool,
    cached_status: AtomicU16,
    starts: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}
struct SyntheticProvider {
    url: String,
    state: Arc<ProviderState>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for SyntheticProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl SyntheticProvider {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(ProviderState::default());
        state.status.store(200, Ordering::SeqCst);
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    serve(socket, state).await;
                });
            }
        });
        Self { url, state, task }
    }
    fn mount(&self, env: &TestEnv) {
        env.write_default_profile("agent-test", "org-lanytehq");
        let path = env
            .chanvoy_config_dir()
            .join("profiles")
            .join(format!("{}.toml", env.profile_name));
        let mut profile: chanvoy_core::Profile =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        profile.server_url = self.url.clone();
        std::fs::write(path, toml::to_string(&profile).unwrap()).unwrap();
    }
}
async fn serve(mut socket: TcpStream, state: Arc<ProviderState>) {
    let mut header = vec![0; 16384];
    let Ok(size) = socket.peek(&mut header).await else {
        return;
    };
    let request = String::from_utf8_lossy(&header[..size]);
    if request.contains("/api/v4/websocket") {
        let Ok(mut ws) = tokio_tungstenite::accept_async(socket).await else {
            return;
        };
        if state.ws_closed.load(Ordering::SeqCst) {
            let _ = ws.close(None).await;
            return;
        }
        if ws.next().await.is_none() {
            return;
        }
        let _ = ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                json!({"status":"OK","seq_reply":1}).to_string().into(),
            ))
            .await;
        loop {
            tokio::select! {
                message=ws.next()=>match message {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => { let _=ws.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await; },
                    Some(Ok(_))=>{}, _=>break,
                },
                _=tokio::time::sleep(Duration::from_millis(25))=>{
                    if state.ws_closed.load(Ordering::SeqCst) { let _=ws.close(None).await;break; }
                }
            }
        }
        return;
    }
    let mut bytes = Vec::new();
    while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
        let mut chunk = [0; 2048];
        let Ok(size) = socket.read(&mut chunk).await else {
            return;
        };
        if size == 0 || bytes.len() > 16384 {
            return;
        }
        bytes.extend_from_slice(&chunk[..size]);
    }
    let request = String::from_utf8_lossy(&bytes);
    let path = request
        .split_whitespace()
        .nth(1)
        .unwrap_or("")
        .split('?')
        .next()
        .unwrap_or("");
    let team = json!({"id":"team-id","name":"org-lanytehq","display_name":"Synthetic"});
    let channel = json!({"id":"chan-id","team_id":"team-id","name":"test-channel","display_name":"Synthetic","type":"O"});
    let mut status = 200;
    let identity = path == "/api/v4/users/me";
    let body = if identity {
        state.starts.fetch_add(1, Ordering::SeqCst);
        let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
        state.peak.fetch_max(active, Ordering::SeqCst);
        let mut closed = [0; 1];
        tokio::select! {
            _=tokio::time::sleep(Duration::from_millis(state.delay.load(Ordering::SeqCst)))=>{},
            _=socket.read(&mut closed)=>{state.active.fetch_sub(1,Ordering::SeqCst);return;},
        }
        status = state.status.load(Ordering::SeqCst);
        if request.contains("Bearer test-token-value")
            && state.cached_status.load(Ordering::SeqCst) != 0
        {
            status = state.cached_status.load(Ordering::SeqCst);
        }
        json!({"id":"bot-id","username":if state.wrong.load(Ordering::SeqCst){"agent-other"}else{"agent-test"},"is_bot":true})
    } else if path == "/api/v4/users/me/teams" {
        json!([team])
    } else if path == "/api/v4/teams/name/org-lanytehq" {
        team
    } else if path.ends_with("/channels/name/test-channel") || path == "/api/v4/channels/chan-id" {
        channel
    } else if path == "/api/v4/users/me/teams/team-id/channels" {
        json!([channel])
    } else if path == "/api/v4/channels/chan-id/posts" {
        json!({"order":[],"posts":{}})
    } else {
        status = 404;
        json!({"message":"synthetic missing route"})
    };
    let body = body.to_string();
    let response=format!("HTTP/1.1 {status} Synthetic\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
    let _ = socket.write_all(response.as_bytes()).await;
    if identity {
        state.active.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn rpc(env: &TestEnv, method: &str) -> chanvoy_core::JsonRpcResponse {
    let mut socket = UnixStream::connect(env.socket_path()).await.unwrap();
    let request = chanvoy_core::rpc_request(method, json!({}));
    socket
        .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    BufReader::new(socket).read_line(&mut line).await.unwrap();
    serde_json::from_str(&line).unwrap()
}
async fn ready(env: &TestEnv) {
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    loop {
        let local = rpc(env, "daemon_observation").await;
        let status: chanvoy_core::DaemonStatus =
            serde_json::from_value(local.result.unwrap()).unwrap();
        if status.ws_connection_state == Some(chanvoy_core::WsConnectionState::Healthy)
            && status.ws_observation_admission_closed == Some(false)
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "synthetic websocket must become admissible"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn provider_free_observation_and_canceled_status_requests_have_one_peak_probe() {
    let env = TestEnv::new("probe-concurrency").await;
    let provider = SyntheticProvider::new().await;
    provider.mount(&env);
    let daemon = spawn_daemon(&env).await;
    let _guard = env.daemon_guard();
    ready(&env).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = provider.state.starts.load(Ordering::SeqCst);
    let local = rpc(&env, "daemon_observation").await.result.unwrap();
    assert_eq!(local["remote_probe"], "unknown");
    assert_eq!(local["mattermost_ok"], false);
    assert!(local["mattermost_last_error"].is_null());
    assert_eq!(provider.state.starts.load(Ordering::SeqCst), before);
    provider.state.delay.store(1000, Ordering::SeqCst);
    provider.state.peak.store(0, Ordering::SeqCst);
    let canceled = async {
        let mut socket = UnixStream::connect(env.socket_path()).await.unwrap();
        let request = chanvoy_core::rpc_request("daemon_status", json!({}));
        socket
            .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    canceled.await;
    let mut callers = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let socket = env.socket_path();
        callers.spawn(async move {
            let mut stream = UnixStream::connect(socket).await.unwrap();
            let request = chanvoy_core::rpc_request("daemon_status", json!({}));
            stream
                .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
                .await
                .unwrap();
            let mut line = String::new();
            BufReader::new(stream).read_line(&mut line).await.unwrap();
            let response: chanvoy_core::JsonRpcResponse = serde_json::from_str(&line).unwrap();
            assert_eq!(response.result.unwrap()["remote_probe"], "unknown");
        });
    }
    while let Some(result) = callers.join_next().await {
        result.unwrap();
    }
    assert_eq!(provider.state.starts.load(Ordering::SeqCst), before + 1);
    assert_eq!(provider.state.peak.load(Ordering::SeqCst), 1);
    provider.state.delay.store(0, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn slow_and_failed_health_preserve_pid_socket_and_inflight_wait() {
    let env = TestEnv::new("health-preserves-wait").await;
    let provider = SyntheticProvider::new().await;
    provider.mount(&env);
    let mut daemon = spawn_daemon(&env).await;
    let _guard = env.daemon_guard();
    ready(&env).await;
    use std::os::unix::fs::MetadataExt;
    let pid_path = env
        .chanvoy_runtime_dir()
        .join(format!("{}.pid", env.profile_name));
    let pid = std::fs::read_to_string(&pid_path).unwrap();
    let inode = std::fs::metadata(env.socket_path()).unwrap().ino();
    let started = std::time::Instant::now();
    let mut wait = env
        .chanvoy_command()
        .arg("--profile")
        .arg(&env.profile_name)
        .args(["--json", "wait", "test-channel", "--timeout", "8s"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(wait.try_wait().unwrap().is_none());
    for (delay, status) in [(1000, 200), (5000, 200), (0, 503)] {
        provider.state.delay.store(delay, Ordering::SeqCst);
        provider.state.status.store(status, Ordering::SeqCst);
        let before = std::time::Instant::now();
        let out = run_chanvoy(&env, &["--json", "daemon", "start"]).await;
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stdout)
        );
        assert!(before.elapsed() < Duration::from_millis(3500));
        let receipt: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(
            receipt["daemon_disposition"],
            if status == 200 && delay < 2000 {
                "healthy"
            } else {
                "degraded-remote"
            }
        );
        assert_eq!(std::fs::read_to_string(&pid_path).unwrap(), pid);
        assert_eq!(std::fs::metadata(env.socket_path()).unwrap().ino(), inode);
        assert!(daemon.try_wait().unwrap().is_none());
        assert!(wait.try_wait().unwrap().is_none());
    }
    let setup = env
        .chanvoy_command()
        .env("LANYTE_AGENT_ROLE", "bravo-devlead")
        .env("LANYTE_AGENT_SCOPE", "lanytehq")
        .env("LANYTE_MM_URL", &provider.url)
        .env("LANYTE_MM_TEAM", "org-lanytehq")
        .env("CHANVOY_PROFILE", &env.profile_name)
        .args(["--json", "auto-setup"])
        .output()
        .await
        .unwrap();
    assert!(
        setup.status.success(),
        "{}",
        String::from_utf8_lossy(&setup.stdout)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&setup.stdout).unwrap();
    assert_eq!(receipt["daemon_disposition"], "degraded-remote");
    let doctor = run_chanvoy(&env, &["--json", "doctor"]).await;
    assert!(
        doctor.status.success(),
        "{}",
        String::from_utf8_lossy(&doctor.stdout)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(receipt["daemon_disposition"], "degraded-remote");
    assert_eq!(std::fs::read_to_string(&pid_path).unwrap(), pid);
    assert_eq!(std::fs::metadata(env.socket_path()).unwrap().ino(), inode);
    provider.state.delay.store(0, Ordering::SeqCst);
    provider.state.status.store(200, Ordering::SeqCst);
    let out = wait.wait_with_output().await.unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(receipt["timeout"], true);
    assert!(started.elapsed() >= Duration::from_millis(7500));
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn authoritative_refusal_latches_and_expected_success_alone_clears_it() {
    let env = TestEnv::new("authoritative-refusal").await;
    let provider = SyntheticProvider::new().await;
    provider.mount(&env);
    let daemon = spawn_daemon(&env).await;
    let _guard = env.daemon_guard();
    ready(&env).await;
    for status in [401, 403] {
        provider.state.status.store(status, Ordering::SeqCst);
        let result = rpc(&env, "daemon_status").await.result.unwrap();
        assert_eq!(result["remote_probe"], "rejected-credential");
        assert_eq!(result["identity_refused"], true);
        assert_eq!(result["mattermost_identity_drift"], false);
        let before = provider.state.starts.load(Ordering::SeqCst);
        let local = rpc(&env, "daemon_observation").await.result.unwrap();
        assert_eq!(local["identity_refused"], true);
        assert_eq!(provider.state.starts.load(Ordering::SeqCst), before);
        assert!(rpc(&env, "whoami").await.error.is_some());
        provider.state.status.store(503, Ordering::SeqCst);
        assert_eq!(
            rpc(&env, "daemon_status").await.result.unwrap()["identity_refused"],
            true
        );
        provider.state.status.store(200, Ordering::SeqCst);
        assert_eq!(
            rpc(&env, "daemon_status").await.result.unwrap()["identity_refused"],
            false
        );
    }
    provider.state.wrong.store(true, Ordering::SeqCst);
    let wrong = rpc(&env, "daemon_status").await.result.unwrap();
    assert_eq!(wrong["remote_probe"], "verified");
    assert_eq!(wrong["mattermost_username"], "agent-other");
    assert_eq!(wrong["identity_refused"], true);
    assert_eq!(wrong["mattermost_identity_drift"], true);
    provider.state.wrong.store(false, Ordering::SeqCst);
    assert_eq!(
        rpc(&env, "daemon_status").await.result.unwrap()["identity_refused"],
        false
    );
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn rejected_cached_token_requires_fresh_parent_identity_before_replacement() {
    let env = TestEnv::new("validated-replacement").await;
    let provider = SyntheticProvider::new().await;
    provider.mount(&env);
    let mut daemon = spawn_daemon(&env).await;
    let _guard = env.daemon_guard();
    ready(&env).await;
    let pid_path = env
        .chanvoy_runtime_dir()
        .join(format!("{}.pid", env.profile_name));
    let old = std::fs::read_to_string(&pid_path).unwrap();
    provider.state.cached_status.store(401, Ordering::SeqCst);
    let rejected = run_chanvoy(&env, &["--json", "daemon", "start"]).await;
    assert!(!rejected.status.success());
    assert_eq!(std::fs::read_to_string(&pid_path).unwrap(), old);
    let fresh = env
        .chanvoy_command()
        .env(&env.token_env_name, "test-token-parent")
        .arg("--profile")
        .arg(&env.profile_name)
        .args(["--json", "daemon", "start"])
        .output()
        .await
        .unwrap();
    assert!(
        fresh.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&fresh.stdout),
        String::from_utf8_lossy(&fresh.stderr)
    );
    assert_ne!(std::fs::read_to_string(&pid_path).unwrap(), old);
    daemon.wait().await.unwrap();
    let stopped = run_chanvoy(&env, &["daemon", "stop"]).await;
    assert!(stopped.status.success());
}

#[tokio::test]
async fn postbind_probe_shares_gate_and_status_samples_admission_after_remote_io() {
    let env = TestEnv::new("postbind-admission").await;
    let provider = SyntheticProvider::new().await;
    provider.mount(&env);
    provider.state.delay.store(1000, Ordering::SeqCst);
    let daemon = spawn_daemon(&env).await;
    let _guard = env.daemon_guard();
    ready(&env).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while provider.state.active.load(Ordering::SeqCst) == 0 {
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for _ in 0..10 {
        assert_eq!(
            rpc(&env, "daemon_status").await.result.unwrap()["remote_probe"],
            "unknown"
        );
        let before = provider.state.starts.load(Ordering::SeqCst);
        let _ = rpc(&env, "daemon_observation").await;
        assert_eq!(provider.state.starts.load(Ordering::SeqCst), before);
    }
    // Foreground startup authenticates once before bind; its post-bind probe
    // is the second request. No status burst may add a third.
    assert_eq!(provider.state.starts.load(Ordering::SeqCst), 2);
    assert_eq!(provider.state.peak.load(Ordering::SeqCst), 1);
    while provider.state.active.load(Ordering::SeqCst) != 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pending = rpc(&env, "daemon_status");
    tokio::pin!(pending);
    tokio::select! { _=&mut pending=>panic!("delayed status unexpectedly completed"), _=tokio::time::sleep(Duration::from_millis(100))=>{} }
    provider.state.ws_closed.store(true, Ordering::SeqCst);
    let result = pending.await.result.unwrap();
    assert_eq!(result["remote_probe"], "verified");
    assert_eq!(result["identity_refused"], false);
    assert_eq!(
        result["ws_observation_admission_closed"], true,
        "status must reflect admission transition during provider I/O"
    );
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn concurrent_observation_preserves_refusal_until_expected_probe_completes() {
    let env = TestEnv::new("concurrent-refusal").await;
    let provider = SyntheticProvider::new().await;
    provider.mount(&env);
    let daemon = spawn_daemon(&env).await;
    let _guard = env.daemon_guard();
    ready(&env).await;
    provider.state.status.store(401, Ordering::SeqCst);
    assert_eq!(
        rpc(&env, "daemon_status").await.result.unwrap()["identity_refused"],
        true
    );
    provider.state.status.store(200, Ordering::SeqCst);
    provider.state.delay.store(500, Ordering::SeqCst);
    let pending = rpc(&env, "daemon_status");
    tokio::pin!(pending);
    tokio::select! { _=&mut pending=>panic!("delayed probe unexpectedly completed"), _=tokio::time::sleep(Duration::from_millis(100))=>{} }
    for method in ["daemon_observation", "daemon_status"] {
        let result = rpc(&env, method).await.result.unwrap();
        assert_eq!(result["remote_probe"], "unknown");
        assert_eq!(result["identity_refused"], true);
        assert_eq!(result["mattermost_ok"], false);
    }
    let result = pending.await.result.unwrap();
    assert_eq!(result["remote_probe"], "verified");
    assert_eq!(result["identity_refused"], false);
    let result = rpc(&env, "daemon_observation").await.result.unwrap();
    assert_eq!(result["identity_refused"], false);
    assert_eq!(result["mattermost_ok"], false);
    provider.state.delay.store(1850, Ordering::SeqCst);
    let before = std::time::Instant::now();
    let result = rpc(&env, "daemon_status").await.result.unwrap();
    assert_eq!(result["remote_probe"], "verified");
    assert!(before.elapsed() < Duration::from_millis(2750));
    assert!(stop_daemon_cleanly(&env, daemon).await);
}

#[tokio::test]
async fn old_daemon_status_timeout_preserves_responsive_local_owner() {
    use std::os::unix::fs::MetadataExt;
    let env = TestEnv::new("old-status-timeout").await;
    env.write_default_profile("agent-test", "org-lanytehq");
    std::fs::create_dir_all(env.chanvoy_runtime_dir()).unwrap();
    let mut owned = tokio::process::Command::new("sleep")
        .arg("30")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = owned.id().unwrap();
    let pid_path = env
        .chanvoy_runtime_dir()
        .join(format!("{}.pid", env.profile_name));
    std::fs::write(&pid_path, pid.to_string()).unwrap();
    let listener = tokio::net::UnixListener::bind(env.socket_path()).unwrap();
    let inode = std::fs::metadata(env.socket_path()).unwrap().ino();
    let profile = env.profile_name.clone();
    let socket = env.socket_path();
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let called = calls.clone();
    let remote_ms = Arc::new(AtomicU64::new(0));
    let measured = remote_ms.clone();
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let request: chanvoy_core::JsonRpcRequest = serde_json::from_str(&line).unwrap();
            called.lock().unwrap().push(request.method.clone());
            let response = match request.method.as_str() {
                "daemon_observation" => {
                    chanvoy_core::rpc_error(request.id, -32601, "method not found")
                }
                "profile_status" => chanvoy_core::rpc_result(
                    request.id,
                    json!({"profile_name":profile,"socket_path":socket,"role":"test","scope":"test","provider":"mattermost","bot_username":"agent-test","server_url":"http://synthetic.invalid"}),
                ),
                "daemon_status" => {
                    let started = std::time::Instant::now();
                    let mut closed = String::new();
                    let _ = reader.read_line(&mut closed).await;
                    measured.store(started.elapsed().as_millis() as u64, Ordering::SeqCst);
                    return;
                }
                other => panic!("unexpected lifecycle RPC {other}"),
            };
            reader
                .get_mut()
                .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
                .await
                .unwrap();
        }
    });
    let before = std::time::Instant::now();
    // Process startup and OS scheduling are outside the assessment clock.
    // Exact RPC deadlines are covered by the assessment's paused-clock tests.
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        env.chanvoy_command()
            .arg("--profile")
            .arg(&env.profile_name)
            .args(["--json", "daemon", "start"])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("owned CLI completion deadline")
    .expect("spawn owned CLI");
    let elapsed = before.elapsed();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["daemon_disposition"], "degraded-remote");
    assert!(receipt["observation_ready"].is_null());
    assert_eq!(std::fs::read_to_string(pid_path).unwrap(), pid.to_string());
    assert_eq!(std::fs::metadata(env.socket_path()).unwrap().ino(), inode);
    assert!(owned.try_wait().unwrap().is_none());
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("status connection closes after CLI completion")
        .unwrap();
    eprintln!(
        "owned CLI elapsed={elapsed:?}, remote status connection elapsed={}ms",
        remote_ms.load(Ordering::SeqCst)
    );
    assert_eq!(
        *calls.lock().unwrap(),
        ["daemon_observation", "profile_status", "daemon_status"]
    );
    owned.kill().await.unwrap();
    owned.wait().await.unwrap();
}
