use super::*;
use crate::{handle_client, rpc_result, StagedAttentionAdvance};
use chanvoy_core::{
    rpc_request, AttentionState, CapabilityClass, CredentialMode, EventBus, MattermostClient,
    Profile, Provider,
};
use serde_json::json;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixStream};
use tokio::sync::{oneshot, Mutex, Notify};
use wiremock::MockServer;

fn state(server_url: &str) -> Arc<AppState> {
    let profile = Profile {
        name: "request-frontier-fixture".into(),
        role: "test".into(),
        scope: "test".into(),
        provider: Provider::Mattermost,
        bot_username: "agent-test".into(),
        team_name: "org".into(),
        server_url: server_url.into(),
        env_name: "TEST_TOKEN".into(),
        env_file: None,
        credential_mode: CredentialMode::EnvName,
        capability_class: CapabilityClass::Standard,
        monitored_channels: vec!["example".into()],
        ipc: None,
        reduce: None,
    };
    Arc::new(AppState {
        client: MattermostClient::new(&profile, "synthetic-token".into()).unwrap(),
        profile,
        socket_path: "/unused/request-frontier.sock".into(),
        my_user_id: "bot-id".into(),
        event_bus: Arc::new(EventBus::new(16)),
        subscriptions: Arc::new(Mutex::new(HashMap::new())),
        ws_state_holder: Arc::new(Mutex::new(None)),
        ipc_state: None,
        attention_state: Arc::new(Mutex::new(AttentionState::default())),
        identity_drift: Arc::new(AtomicBool::new(false)),
        observed_identity_drift: Arc::new(AtomicBool::new(false)),
        probe_gate: Arc::new(chanvoy_core::recovery::IdentityProbeGate::default()),
        reduce_writer: None,
        wait_owners: Arc::new(wait_owner::WaitOwnerRegistry::new()),
        poll_cursors: crate::waitprims_poll::PollCursorStore::for_test("request-frontier-fixture"),
        fanin_replay: crate::waitprims_fanin::FanInReplayStore::new(),
        inbox_armed: Arc::new(AtomicU64::new(0)),
    })
}

fn busy(state: &AppState) {
    let snapshot = state.wait_owners.admission_snapshot();
    assert_eq!(snapshot.requests, 1);
    assert!(matches!(
        state.wait_owners.retire_if_idle(snapshot.instance),
        Ok(wait_owner::RetirementDecision::Busy(_))
    ));
}

fn outcome() -> DispatchOutcome {
    DispatchOutcome {
        response: rpc_result(uuid::Uuid::new_v4(), json!({"done": true})),
        staged_poll_ack: None,
        staged_attention: None,
        staged_fanin_consume: None,
        fanin_hold: None,
    }
}

async fn collect<T>(mut task: tokio::task::JoinHandle<T>) -> T {
    match tokio::time::timeout(Duration::from_secs(2), &mut task).await {
        Ok(result) => result.unwrap(),
        Err(_) => {
            task.abort();
            let _ = task.await;
            panic!("owned fixture task did not finish");
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Payload,
    Newline,
    Flush,
}

#[derive(Default)]
struct Gate {
    reached: Notify,
    flushed: Notify,
    released: AtomicBool,
    waker: StdMutex<Option<Waker>>,
    bytes: StdMutex<Vec<u8>>,
}

impl Gate {
    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn pending(&self, cx: &Context<'_>) -> bool {
        let mut waker = self.waker.lock().unwrap();
        if self.released.load(Ordering::SeqCst) {
            return false;
        }
        *waker = Some(cx.waker().clone());
        self.reached.notify_one();
        true
    }
}

struct Writer {
    gate: Arc<Gate>,
    stage: Option<Stage>,
    fail: bool,
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let stage = if bytes == b"\n" {
            Stage::Newline
        } else {
            Stage::Payload
        };
        if self.stage == Some(stage) {
            if self.fail {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            if self.gate.pending(cx) {
                return Poll::Pending;
            }
        }
        self.gate.bytes.lock().unwrap().extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.stage == Some(Stage::Flush) {
            if self.fail {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            if self.gate.pending(cx) {
                return Poll::Pending;
            }
        }
        self.gate.flushed.notify_one();
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn ordinary_payload_newline_and_flush_are_owned_until_confirmed_completion() {
    for stage in [Stage::Payload, Stage::Newline, Stage::Flush] {
        let state = state("http://127.0.0.1:9");
        let gate = Arc::new(Gate::default());
        let permit = state.wait_owners.request_permit().unwrap();
        let task_state = Arc::clone(&state);
        let mut writer = Writer {
            gate: Arc::clone(&gate),
            stage: Some(stage),
            fail: false,
        };
        let task =
            tokio::spawn(async move { finish(&mut writer, &task_state, outcome(), permit).await });
        tokio::time::timeout(Duration::from_secs(2), gate.reached.notified())
            .await
            .unwrap();
        busy(&state);
        gate.release();
        collect(task).await.unwrap();
        assert_eq!(state.wait_owners.admission_snapshot().requests, 0);
        let bytes = gate.bytes.lock().unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        let response: chanvoy_core::JsonRpcResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(response.error.is_none());
    }
}

#[tokio::test]
async fn failed_payload_newline_or_flush_does_not_confirm_completion() {
    for stage in [Stage::Payload, Stage::Newline, Stage::Flush] {
        let state = state("http://127.0.0.1:9");
        let permit = state.wait_owners.request_permit().unwrap();
        let mut writer = Writer {
            gate: Arc::new(Gate::default()),
            stage: Some(stage),
            fail: true,
        };
        assert!(matches!(
            finish(&mut writer, &state, outcome(), permit).await,
            Err(DaemonError::Io(_))
        ));
        busy(&state);
    }
}

#[tokio::test]
async fn cancellation_after_full_response_before_staged_commit_retains_ownership() {
    let state = state("http://127.0.0.1:9");
    // Block the actual commit function before any persistence. This test
    // never changes process env or writes to an operator's config directory.
    let lock = state.attention_state.lock().await;
    let gate = Arc::new(Gate::default());
    let permit = state.wait_owners.request_permit().unwrap();
    let mut dispatched = outcome();
    dispatched.staged_attention = Some(StagedAttentionAdvance {
        key: "org/channel".into(),
        cursor: Default::default(),
    });
    let task_state = Arc::clone(&state);
    let mut writer = Writer {
        gate: Arc::clone(&gate),
        stage: None,
        fail: false,
    };
    let task =
        tokio::spawn(async move { finish(&mut writer, &task_state, dispatched, permit).await });
    tokio::time::timeout(Duration::from_secs(2), gate.flushed.notified())
        .await
        .unwrap();
    let before_cancel = state.wait_owners.admission_snapshot();
    let decision = state.wait_owners.retire_if_idle(before_cancel.instance);
    let framed = gate.bytes.lock().unwrap().last() == Some(&b'\n');
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(lock);
    // Collect the task before assertions, including for deliberate negative
    // controls, so a failed assertion cannot unblock real persistence.
    assert_eq!(before_cancel.requests, 1);
    assert!(matches!(
        decision,
        Ok(wait_owner::RetirementDecision::Busy(_))
    ));
    assert!(framed);
    busy(&state);
}

#[tokio::test]
async fn closed_arena_refuses_all_twelve_wait_aliases_before_provider_preflight() {
    let mock = MockServer::start().await;
    let state = state(&mock.uri());
    let instance = state.wait_owners.admission_snapshot().instance;
    assert!(matches!(
        state.wait_owners.retire_if_idle(instance),
        Ok(wait_owner::RetirementDecision::Closed(_))
    ));
    for method in [
        "wait_channel",
        "wait_channel_v2",
        "wait_channel_v3",
        "wait_channels_v1",
        "wait_dm_v1",
        "wait_inbox_v1",
        "wait_follow_v1",
        "wait_follow_v2",
        "wait_dm_follow_v1",
        "wait_dm_follow_v2",
        "wait_inbox_follow_v1",
        "wait_inbox_follow_v2",
    ] {
        let (server, mut client) = UnixStream::pair().unwrap();
        let (shutdown, _receiver) = oneshot::channel();
        let task = tokio::spawn(handle_client(
            server,
            Arc::clone(&state),
            Arc::new(Mutex::new(Some(shutdown))),
        ));
        let request = rpc_request(
            method,
            json!({"channel":"example", "username":"example", "timeout_secs":1}),
        );
        client
            .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
            .await
            .unwrap();
        assert!(matches!(
            collect(task).await,
            Err(DaemonError::Core(CoreError::RuntimeAdmissionClosed))
        ));
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).await.unwrap();
        assert!(
            bytes.is_empty(),
            "local refusal EOF is not a successful response"
        );
        assert!(mock.received_requests().await.unwrap().is_empty());
    }
    assert_eq!(state.wait_owners.armed_count(), 0);
    assert_eq!(state.wait_owners.provider_io_count(), 0);
}

struct ProviderGate {
    url: String,
    started: Arc<Notify>,
    release: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl ProviderGate {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let task_started = Arc::clone(&started);
        let task_release = Arc::clone(&release);
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut header = String::new();
            stream.read_line(&mut header).await.unwrap();
            assert!(header.starts_with("GET /api/v4/"));
            loop {
                header.clear();
                assert!(stream.read_line(&mut header).await.unwrap() > 0);
                if header == "\r\n" {
                    break;
                }
            }
            task_started.notify_one();
            task_release.notified().await;
            let body =
                serde_json::to_string(&json!({"id":"bot-id", "username":"agent-test"})).unwrap();
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
            // Disconnect proof may have already closed the provider future.
            let _ = stream.get_mut().write_all(response.as_bytes()).await;
        });
        Self {
            url,
            started,
            release,
            task,
        }
    }

    async fn observed(&self) {
        tokio::time::timeout(Duration::from_secs(2), self.started.notified())
            .await
            .unwrap();
    }

    async fn finish(&mut self) {
        self.release.notify_one();
        match tokio::time::timeout(Duration::from_secs(2), &mut self.task).await {
            Ok(result) => result.unwrap(),
            Err(_) => {
                self.task.abort();
                let _ = (&mut self.task).await;
                panic!("owned HTTP fixture did not finish");
            }
        }
    }
}

impl Drop for ProviderGate {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn actual_handler_owns_provider_work_and_releases_before_idle_connection_eof() {
    let mut provider = ProviderGate::new().await;
    let state = state(&provider.url);
    let (server, mut client) = UnixStream::pair().unwrap();
    let (shutdown, _receiver) = oneshot::channel();
    let task = tokio::spawn(handle_client(
        server,
        Arc::clone(&state),
        Arc::new(Mutex::new(Some(shutdown))),
    ));
    let request = rpc_request("whoami", json!({}));
    client
        .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
        .await
        .unwrap();
    provider.observed().await;
    busy(&state);
    provider.finish().await;
    let mut client = BufReader::new(client);
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(2), client.read_line(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response: chanvoy_core::JsonRpcResponse = serde_json::from_str(&response).unwrap();
    assert!(response.error.is_none());
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.wait_owners.admission_snapshot().requests != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let instance = state.wait_owners.admission_snapshot().instance;
    assert!(matches!(
        state.wait_owners.retire_if_idle(instance),
        Ok(wait_owner::RetirementDecision::Closed(_))
    ));
    drop(client);
    collect(task).await.unwrap();
}

#[tokio::test]
async fn actual_wait_disconnect_during_provider_preflight_retains_unconfirmed_request() {
    let mut provider = ProviderGate::new().await;
    let state = state(&provider.url);
    let (server, mut client) = UnixStream::pair().unwrap();
    let (shutdown, _receiver) = oneshot::channel();
    let task = tokio::spawn(handle_client(
        server,
        Arc::clone(&state),
        Arc::new(Mutex::new(Some(shutdown))),
    ));
    let request = rpc_request(
        "wait_channel_v3",
        json!({"channel":"example", "timeout_secs":30}),
    );
    client
        .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
        .await
        .unwrap();
    provider.observed().await;
    busy(&state);
    assert_eq!(state.wait_owners.admission_snapshot().slots, 0);
    drop(client);
    collect(task).await.unwrap();
    provider.finish().await;
    busy(&state);
    assert_eq!(state.wait_owners.admission_snapshot().slots, 0);
    assert_eq!(state.wait_owners.armed_count(), 0);
}

mod held;
