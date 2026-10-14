mod subscriptions;

use super::*;
use chanvoy_core::request_ownership::{RegistrationCompletion, RequestCompletion};
use chanvoy_core::{CapabilityClass, CredentialMode, GapPayload, Provider};
use serde_json::{json, Value};
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Notify;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

// A trace oracle for IPC routing only. The daemon adapter's separate tests
// exercise the real shared arena; this probe is not a production authority.
#[derive(Default)]
struct Probe {
    reservations: AtomicUsize,
    active: AtomicUsize,
    streams: AtomicUsize,
    stream_reservations: AtomicUsize,
    refuse: AtomicUsize,
}
struct Completion(Arc<Probe>);
struct StreamCompletion(Arc<Probe>);
impl RegistrationCompletion for StreamCompletion {
    fn confirm_done(self: Box<Self>) -> Result<(), CoreError> {
        assert!(self.0.streams.fetch_sub(1, Ordering::SeqCst) > 0);
        Ok(())
    }
}
struct Admission(Arc<Probe>);
impl RequestAdmission for Admission {
    fn reserve_request(&self) -> Result<Box<dyn RequestCompletion>, CoreError> {
        self.0.reservations.fetch_add(1, Ordering::SeqCst);
        match self.0.refuse.load(Ordering::SeqCst) {
            1 => return Err(CoreError::RuntimeAdmissionClosed),
            2 => return Err(CoreError::RuntimeOwnershipExhausted),
            _ => {}
        }
        self.0.active.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Completion(Arc::clone(&self.0))))
    }
}
impl RequestCompletion for Completion {
    fn reserve_registration(&self) -> Result<Box<dyn RegistrationCompletion>, CoreError> {
        assert!(self.0.active.load(Ordering::SeqCst) > 0);
        self.0.stream_reservations.fetch_add(1, Ordering::SeqCst);
        self.0.streams.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(StreamCompletion(Arc::clone(&self.0))))
    }

    fn confirm_done(self: Box<Self>) -> Result<(), CoreError> {
        assert!(self.0.active.fetch_sub(1, Ordering::SeqCst) > 0);
        Ok(())
    }
}

fn peer(url: &str, probe: &Arc<Probe>) -> Arc<IpcPeer> {
    let profile = Profile {
        name: "request-proof".into(),
        role: "devlead".into(),
        scope: "test".into(),
        provider: Provider::Mattermost,
        bot_username: "fixture-bot".into(),
        team_name: "team".into(),
        server_url: url.into(),
        env_name: "FIXTURE_TOKEN".into(),
        env_file: None,
        credential_mode: CredentialMode::EnvName,
        capability_class: CapabilityClass::Standard,
        monitored_channels: vec![],
        ipc: None,
        reduce: None,
    };
    let client = MattermostClient::new(&profile, "test-token-ipc".into()).unwrap();
    Arc::new(IpcPeer::new(
        &profile,
        client,
        Arc::new(EventBus::new(16)),
        "unused-fixture-socket".into(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(Admission(Arc::clone(probe))),
    ))
}

fn request(kind: &str, threaded: bool) -> Frame {
    let mut value = json!({"type":kind,"request_id":"request","delegation_id":"fixture",
        "channel_id":"channel","message":"synthetic","gate_token":"fixture-gate",
        "subscription_id":"retained","resume_after_seq":0});
    if threaded {
        value["thread_root_id"] = json!("root");
    }
    Frame::new(CHANNEL_260, serde_json::to_vec(&value).unwrap())
}
fn variants() -> Vec<(&'static str, bool)> {
    vec![
        ("chat_channel_list_request", false),
        ("chat_channel_get_request", false),
        ("chat_read_request", false),
        ("chat_read_request", true),
        ("chat_post_request", false),
        ("chat_post_request", true),
        ("chat_subscribe_request", false),
        ("chat_unsubscribe_request", false),
    ]
}

struct Transport {
    probe: Arc<Probe>,
    calls: AtomicUsize,
    stop_at: usize,
    fail: bool,
    entered: Notify,
    release: Notify,
    frames: Mutex<Vec<(u16, Value)>>,
}
impl Transport {
    fn new(probe: &Arc<Probe>, stop_at: usize, fail: bool) -> Arc<Self> {
        Arc::new(Self {
            probe: Arc::clone(probe),
            calls: AtomicUsize::new(0),
            stop_at,
            fail,
            entered: Notify::new(),
            release: Notify::new(),
            frames: Mutex::new(vec![]),
        })
    }
}
impl RequestTransport for Transport {
    async fn send(&self, channel: u16, payload: &[u8]) -> SendResult {
        assert_eq!(
            self.probe.active.load(Ordering::SeqCst),
            1,
            "send before admission or after confirmation"
        );
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        if index == self.stop_at {
            self.entered.notify_one();
            self.release.notified().await;
            assert_eq!(
                self.probe.active.load(Ordering::SeqCst),
                1,
                "pending output lost ownership"
            );
            if self.fail {
                return Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into());
            }
        }
        self.frames
            .lock()
            .unwrap()
            .push((channel, serde_json::from_slice(payload)?));
        Ok(())
    }
}
fn start(
    peer: &Arc<IpcPeer>,
    tx: &Arc<Transport>,
    frame: Frame,
) -> tokio::task::JoinHandle<SendResult> {
    let peer = Arc::clone(peer);
    let tx = Arc::clone(tx);
    tokio::spawn(async move { peer.handle_frame(frame, tx.as_ref(), "fixture-peer").await })
}
async fn entered(tx: &Transport) {
    tokio::time::timeout(Duration::from_secs(2), tx.entered.notified())
        .await
        .unwrap();
}
async fn collect(task: tokio::task::JoinHandle<SendResult>) -> SendResult {
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
}
fn advance(peer: &IpcPeer) {
    peer.event_bus.emit(DaemonEvent {
        seq: 0,
        kind: DaemonEventKind::Gap,
        payload: DaemonEventPayloadInner::Gap(GapPayload {
            subscription_id: "old".into(),
            missed_from_seq: 0,
            missed_to_seq: 1,
        }),
    });
}
async fn retained(peer: &IpcPeer, probe: &Arc<Probe>) {
    probe.streams.fetch_add(1, Ordering::SeqCst);
    peer.subscriptions.lock().await.insert(
        "retained".into(),
        SubscriptionEntry {
            owner: Box::new(StreamCompletion(Arc::clone(probe))),
            filter: IpcSubscriptionFilter {
                channel_ids: None,
                event_kinds: None,
                mentions_only: None,
            },
        },
    );
}

struct ProviderReply {
    probe: Arc<Probe>,
    error: bool,
}
impl Respond for ProviderReply {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        assert_eq!(
            self.probe.active.load(Ordering::SeqCst),
            1,
            "provider effect before admission"
        );
        if self.error {
            return ResponseTemplate::new(403).set_body_json(json!({"message":"fixture refused"}));
        }
        let post = json!({"id":"root","channel_id":"channel","user_id":"user","message":"synthetic",
            "create_at":1000,"root_id":""});
        let body = match request.url.path() {
            "/api/v4/teams/name/team" => json!({"id":"team-id"}),
            "/api/v4/users/me/teams/team-id/channels" => {
                json!([{"id":"channel","name":"channel","display_name":"Channel","type":"O"}])
            }
            "/api/v4/posts" => json!({"id":"written-post"}),
            "/api/v4/posts/root" => post.clone(),
            "/api/v4/posts/root/thread" => json!({"order":["root"],"posts":{"root":post}}),
            "/api/v4/channels/channel/posts" => json!({"order":[],"posts":{}}),
            "/api/v4/users/user" => json!({"id":"user","username":"fixture-user"}),
            other => panic!("unexpected provider path {other}"),
        };
        ResponseTemplate::new(200).set_body_json(body)
    }
}
async fn provider(probe: &Arc<Probe>, error: bool) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ProviderReply {
            probe: Arc::clone(probe),
            error,
        })
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn every_handled_variant_owns_provider_mutation_and_all_selected_sends() {
    for (kind, threaded) in variants() {
        let probe = Arc::new(Probe::default());
        let server = provider(&probe, false).await;
        let peer = peer(&server.uri(), &probe);
        retained(&peer, &probe).await;
        advance(&peer);
        let tx = Transport::new(&probe, usize::MAX, false);
        collect(start(&peer, &tx, request(kind, threaded)))
            .await
            .unwrap();
        assert_eq!(probe.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        let frames = tx.frames.lock().unwrap();
        assert_eq!(frames.last().unwrap().0, CHANNEL_260);
        assert_ne!(frames.last().unwrap().1["type"], "chat_error");
        let expected = kind.replace("_request", "_response");
        assert_eq!(frames.last().unwrap().1["type"], expected);
        let expected_sends = match kind {
            "chat_subscribe_request" => 3,
            "chat_post_request" | "chat_unsubscribe_request" => 2,
            _ => 1,
        };
        assert_eq!(frames.len(), expected_sends);
    }
}

#[tokio::test]
async fn closed_and_exhausted_admission_refuse_every_variant_before_effects() {
    for refusal in [1, 2] {
        for (kind, threaded) in variants() {
            let probe = Arc::new(Probe::default());
            probe.refuse.store(refusal, Ordering::SeqCst);
            let server = MockServer::start().await;
            let peer = peer(&server.uri(), &probe);
            retained(&peer, &probe).await;
            let tx = Transport::new(&probe, usize::MAX, false);
            let error = collect(start(&peer, &tx, request(kind, threaded)))
                .await
                .unwrap_err();
            assert!(error.downcast_ref::<CoreError>().is_some());
            assert_eq!(probe.reservations.load(Ordering::SeqCst), 1);
            assert_eq!(probe.active.load(Ordering::SeqCst), 0);
            assert_eq!(tx.calls.load(Ordering::SeqCst), 0);
            assert_eq!(peer.subscriptions.lock().await.len(), 1);
            assert!(server.received_requests().await.unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn malformed_and_unhandled_frames_reserve_nothing() {
    let probe = Arc::new(Probe::default());
    let peer = peer("http://127.0.0.1:9", &probe);
    let tx = Transport::new(&probe, usize::MAX, false);
    for payload in [
        b"not-json".to_vec(),
        serde_json::to_vec(&ChatFrame::UnsubscribeResponse {
            request_id: "x".into(),
            success: false,
        })
        .unwrap(),
    ] {
        peer.handle_frame(Frame::new(CHANNEL_260, payload), tx.as_ref(), "fixture")
            .await
            .unwrap();
    }
    assert_eq!(probe.reservations.load(Ordering::SeqCst), 0);
    assert_eq!(tx.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn drift_and_empty_gate_refusals_are_owned_without_provider_work() {
    for (kind, threaded) in variants()
        .into_iter()
        .filter(|(kind, _)| *kind != "chat_unsubscribe_request")
    {
        let probe = Arc::new(Probe::default());
        let server = MockServer::start().await;
        let peer = peer(&server.uri(), &probe);
        peer.identity_drift.store(true, Ordering::SeqCst);
        let tx = Transport::new(&probe, usize::MAX, false);
        collect(start(&peer, &tx, request(kind, threaded)))
            .await
            .unwrap();
        assert_eq!(probe.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            tx.frames.lock().unwrap()[0].1["error_code"],
            "identity_drift"
        );
        assert!(server.received_requests().await.unwrap().is_empty());
    }
    let probe = Arc::new(Probe::default());
    let server = MockServer::start().await;
    let peer = peer(&server.uri(), &probe);
    let mut value: Value =
        serde_json::from_slice(&request("chat_post_request", false).payload).unwrap();
    value["gate_token"] = json!("");
    let tx = Transport::new(&probe, usize::MAX, false);
    collect(start(
        &peer,
        &tx,
        Frame::new(CHANNEL_260, serde_json::to_vec(&value).unwrap()),
    ))
    .await
    .unwrap();
    assert_eq!(probe.reservations.load(Ordering::SeqCst), 1);
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        tx.frames.lock().unwrap()[0].1["error_code"],
        "permission_denied"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn subscribe_gap_audit_and_ack_stay_owned_through_failure_or_drop() {
    for stage in 0..3 {
        for outcome in 0..3 {
            let probe = Arc::new(Probe::default());
            let peer = peer("http://127.0.0.1:9", &probe);
            advance(&peer);
            let tx = Transport::new(&probe, stage, outcome == 1);
            let task = start(&peer, &tx, request("chat_subscribe_request", false));
            entered(&tx).await;
            assert_eq!(probe.active.load(Ordering::SeqCst), 1);
            assert_eq!(
                peer.subscriptions.lock().await.len(),
                usize::from(stage > 0)
            );
            assert_eq!(probe.streams.load(Ordering::SeqCst), usize::from(stage > 0));
            assert_eq!(
                probe.stream_reservations.load(Ordering::SeqCst),
                usize::from(stage > 0)
            );
            if outcome == 2 {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                tx.release.notify_one();
                assert_eq!(collect(task).await.is_err(), outcome == 1);
            }
            assert_eq!(
                probe.active.load(Ordering::SeqCst),
                usize::from(outcome != 0)
            );
            assert_eq!(
                peer.subscriptions.lock().await.len(),
                usize::from(outcome == 0 || stage > 0)
            );
            assert_eq!(
                probe.streams.load(Ordering::SeqCst),
                usize::from(outcome == 0 || stage > 0)
            );
            assert_eq!(
                probe.stream_reservations.load(Ordering::SeqCst),
                usize::from(outcome == 0 || stage > 0)
            );
        }
    }
}

#[tokio::test]
async fn post_audit_and_reply_are_owned_for_both_provider_write_paths() {
    for threaded in [false, true] {
        for stage in 0..2 {
            for outcome in 0..3 {
                let probe = Arc::new(Probe::default());
                let server = provider(&probe, false).await;
                let peer = peer(&server.uri(), &probe);
                let tx = Transport::new(&probe, stage, outcome == 1);
                let task = start(&peer, &tx, request("chat_post_request", threaded));
                entered(&tx).await;
                assert_eq!(probe.active.load(Ordering::SeqCst), 1);
                assert_eq!(server.received_requests().await.unwrap().len(), 1);
                if outcome == 2 {
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                } else {
                    tx.release.notify_one();
                    assert_eq!(collect(task).await.is_err(), outcome == 1);
                }
                assert_eq!(
                    probe.active.load(Ordering::SeqCst),
                    usize::from(outcome != 0)
                );
            }
        }
    }
}

#[tokio::test]
async fn unsubscribe_removal_does_not_confirm_its_pending_audit_or_reply() {
    for stage in 0..2 {
        for fail in [false, true] {
            let probe = Arc::new(Probe::default());
            let peer = peer("http://127.0.0.1:9", &probe);
            retained(&peer, &probe).await;
            let tx = Transport::new(&probe, stage, fail);
            let task = start(&peer, &tx, request("chat_unsubscribe_request", false));
            entered(&tx).await;
            assert!(peer.subscriptions.lock().await.is_empty());
            assert_eq!(probe.active.load(Ordering::SeqCst), 1);
            tx.release.notify_one();
            assert_eq!(collect(task).await.is_err(), fail);
            assert_eq!(probe.active.load(Ordering::SeqCst), usize::from(fail));
        }
    }
}

#[tokio::test]
async fn provider_error_completes_only_after_its_existing_error_response() {
    for fail in [false, true] {
        let probe = Arc::new(Probe::default());
        let server = provider(&probe, true).await;
        let peer = peer(&server.uri(), &probe);
        let tx = Transport::new(&probe, 0, fail);
        let task = start(&peer, &tx, request("chat_post_request", false));
        entered(&tx).await;
        assert_eq!(probe.active.load(Ordering::SeqCst), 1);
        tx.release.notify_one();
        assert_eq!(collect(task).await.is_err(), fail);
        assert_eq!(probe.active.load(Ordering::SeqCst), usize::from(fail));
        if !fail {
            assert_eq!(tx.frames.lock().unwrap()[0].1["type"], "chat_error");
        }
    }
}

#[tokio::test]
async fn pending_provider_and_cancelled_handler_retain_request_ownership() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for abort in [false, true] {
        let probe = Arc::new(Probe::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let server_probe = Arc::clone(&probe);
        let server_reached = Arc::clone(&reached);
        let server_release = Arc::clone(&release);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut bytes = [0_u8; 1024];
            while !received.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut bytes).await.unwrap();
                assert!(n > 0);
                received.extend_from_slice(&bytes[..n]);
            }
            assert_eq!(server_probe.active.load(Ordering::SeqCst), 1);
            server_reached.notify_one();
            server_release.notified().await;
            let body = r#"{"id":"written-post"}"#;
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            let _ = socket.write_all(response.as_bytes()).await;
        });
        let peer = peer(&url, &probe);
        let tx = Transport::new(&probe, usize::MAX, false);
        let task = start(&peer, &tx, request("chat_post_request", false));
        tokio::time::timeout(Duration::from_secs(2), reached.notified())
            .await
            .unwrap();
        assert_eq!(probe.active.load(Ordering::SeqCst), 1);
        assert_eq!(tx.calls.load(Ordering::SeqCst), 0);
        if abort {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(probe.active.load(Ordering::SeqCst), 1);
            release.notify_one();
        } else {
            release.notify_one();
            collect(task).await.unwrap();
            assert_eq!(probe.active.load(Ordering::SeqCst), 0);
            assert_eq!(tx.calls.load(Ordering::SeqCst), 2);
        }
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
}

struct UnsupportedTransport(Arc<Probe>);
impl RequestTransport for UnsupportedTransport {
    async fn send(&self, channel: u16, _: &[u8]) -> SendResult {
        assert_eq!(self.0.active.load(Ordering::SeqCst), 1);
        Err(ipcprims::peer::PeerError::UnsupportedChannel(channel).into())
    }
}

#[tokio::test]
async fn unsupported_transport_refusal_is_not_request_completion() {
    let probe = Arc::new(Probe::default());
    let peer = peer("http://127.0.0.1:9", &probe);
    let tx = UnsupportedTransport(Arc::clone(&probe));
    let result = peer
        .handle_frame(request("chat_unsubscribe_request", false), &tx, "fixture")
        .await;
    assert!(matches!(
        result
            .unwrap_err()
            .downcast_ref::<ipcprims::peer::PeerError>(),
        Some(ipcprims::peer::PeerError::UnsupportedChannel(CHANNEL_260))
    ));
    assert_eq!(probe.active.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn missing_unsubscribe_emits_only_its_owned_response() {
    let probe = Arc::new(Probe::default());
    let peer = peer("http://127.0.0.1:9", &probe);
    let tx = Transport::new(&probe, usize::MAX, false);
    collect(start(
        &peer,
        &tx,
        request("chat_unsubscribe_request", false),
    ))
    .await
    .unwrap();
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    let frames = tx.frames.lock().unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].1["success"], false);
}
