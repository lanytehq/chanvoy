use super::*;
use crate::request_ownership::RegistrationCompletion;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
struct Arena {
    active: Arc<AtomicUsize>,
    closed: AtomicBool,
    fail_confirm: Arc<AtomicBool>,
}
impl RequestAdmission for Arena {
    fn reserve_request(&self) -> Result<Box<dyn RequestCompletion>, CoreError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(CoreError::RuntimeAdmissionClosed);
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Permit(
            self.active.clone(),
            self.fail_confirm.clone(),
        )))
    }
}
struct Permit(Arc<AtomicUsize>, Arc<AtomicBool>);
impl RequestCompletion for Permit {
    fn reserve_registration(&self) -> Result<Box<dyn RegistrationCompletion>, CoreError> {
        panic!("no WS registration permit")
    }
    fn confirm_done(self: Box<Self>) -> Result<(), CoreError> {
        if self.1.load(Ordering::SeqCst) {
            return Err(failure("confirmation failed"));
        }
        self.0.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}
async fn finish(task: ManagedWs) -> (Result<(), CoreError>, Result<(), WsCollectionError>) {
    tokio::time::timeout(Duration::from_secs(2), task.finish())
        .await
        .unwrap()
        .into_parts()
}

#[tokio::test]
async fn empty_scope_is_sealed_after_actual_outer_join() {
    let arena = Arc::new(Arena::default());
    let task = launch(arena.clone(), |_, _| async {});
    let (execution, collection) = finish(task).await;
    assert!(execution.is_ok() && collection.is_ok());
    assert_eq!(arena.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn registered_child_outlives_outer_and_blocks_receipt() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let child_started = started.clone();
    let child_release = release.clone();
    let task = launch(Arc::new(Arena::default()), move |_, execution| async move {
        execution.register(async move {
            child_started.notify_one();
            child_release.notified().await;
        });
    });
    started.notified().await;
    let mut receipt = Box::pin(task.finish());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut receipt)
            .await
            .is_err()
    );
    release.notify_one();
    let (execution, collection) = receipt.await.into_parts();
    assert!(execution.is_ok() && collection.is_ok());
}

#[tokio::test]
async fn dropped_finish_retains_actual_child_until_completion() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let ended = Arc::new(Notify::new());
    let a = started.clone();
    let b = release.clone();
    let c = ended.clone();
    let task = launch(Arc::new(Arena::default()), move |_, execution| async move {
        execution.register(async move {
            a.notify_one();
            b.notified().await;
            c.notify_one();
        });
    });
    started.notified().await;
    let mut receipt = Box::pin(task.finish());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut receipt)
            .await
            .is_err()
    );
    drop(receipt);
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), ended.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn reaped_child_panic_stays_failed_after_later_success() {
    let ready = Arc::new(Notify::new());
    let go = Arc::new(Notify::new());
    let a = ready.clone();
    let b = go.clone();
    let task = launch(Arc::new(Arena::default()), move |_, execution| async move {
        execution.register(async {
            panic!("synthetic recovery panic");
        });
        execution.register(async move {
            a.notify_one();
        });
        b.notified().await;
    });
    ready.notified().await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    go.notify_one();
    let (execution, collection) = finish(task).await;
    assert!(execution.is_err());
    assert_eq!(collection, Err(WsCollectionError::RecoveryJoinUnconfirmed));
}

#[tokio::test]
async fn child_confirmation_failure_is_sticky_execution_not_join_failure() {
    let arena = Arc::new(Arena::default());
    arena.fail_confirm.store(true, Ordering::SeqCst);
    let done = Arc::new(Notify::new());
    let signal = done.clone();
    let task = launch(arena.clone(), move |_, execution| async move {
        let op = execution.operations.reserve().unwrap();
        execution.register(async move {
            op.complete();
            signal.notify_one();
        });
    });
    done.notified().await;
    let (execution, collection) = finish(task).await;
    assert!(execution.is_err() && collection.is_ok());
    assert_eq!(arena.active.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn closed_admission_is_known_unstarted() {
    let arena = Arc::new(Arena::default());
    arena.closed.store(true, Ordering::SeqCst);
    let done = Arc::new(Notify::new());
    let signal = done.clone();
    let task = launch(arena.clone(), move |_, execution| async move {
        assert!(matches!(
            execution.operations.reserve(),
            Err(CoreError::RuntimeAdmissionClosed)
        ));
        signal.notify_one();
    });
    done.notified().await;
    let (execution, collection) = finish(task).await;
    assert!(execution.is_ok() && collection.is_ok());
    assert_eq!(arena.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropped_operation_retains_owner_and_sticky_execution_error() {
    let arena = Arc::new(Arena::default());
    let done = Arc::new(Notify::new());
    let signal = done.clone();
    let task = launch(arena.clone(), move |_, execution| async move {
        drop(execution.operations.reserve().unwrap());
        execution.operations.reserve().unwrap().complete();
        signal.notify_one();
    });
    done.notified().await;
    let (execution, collection) = finish(task).await;
    assert!(execution.is_err() && collection.is_ok());
    assert_eq!(arena.active.load(Ordering::SeqCst), 1);
}

fn websocket(url: String) -> Arc<MattermostWs> {
    let profile = crate::Profile {
        name: "test".into(),
        role: "test".into(),
        scope: "test".into(),
        provider: crate::Provider::Mattermost,
        bot_username: "test-bot".into(),
        team_name: "test-team".into(),
        server_url: url,
        env_name: "TEST_TOKEN".into(),
        env_file: None,
        credential_mode: crate::CredentialMode::EnvName,
        capability_class: crate::CapabilityClass::Standard,
        monitored_channels: Vec::new(),
        ipc: None,
        reduce: None,
    };
    let client = crate::MattermostClient::new(&profile, "test-token".into()).unwrap();
    Arc::new(MattermostWs::new(
        &profile,
        client,
        Arc::new(crate::EventBus::new(64)),
        "self".into(),
    ))
}

#[tokio::test]
async fn production_idle_authenticated_connection_stops_without_operational_owner() {
    use futures_util::{SinkExt, StreamExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = websocket(format!("http://{}", listener.local_addr().unwrap()));
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(crate::WsMessage::Text(r#"{"event":"hello"}"#.into()))
            .await
            .unwrap();
        while let Some(Ok(_)) = socket.next().await {}
    });
    let arena = Arc::new(Arena::default());
    let task = ws.clone().spawn_managed(arena.clone());
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if *ws.ws_state.connection_state.lock().await == crate::WsConnectionState::Healthy
                && arena.active.load(Ordering::SeqCst) == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (execution, collection) = finish(task).await;
    assert!(execution.is_ok() && collection.is_ok());
    assert_eq!(arena.active.load(Ordering::SeqCst), 0);
    assert!(ws
        .ws_state
        .observation_admission_closed
        .load(Ordering::Acquire));
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn production_returned_connect_failures_do_not_accumulate_owners() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = websocket(format!("http://{}", listener.local_addr().unwrap()));
    drop(listener);
    let arena = Arc::new(Arena::default());
    let task = ws.clone().spawn_managed(arena.clone());
    tokio::time::timeout(Duration::from_secs(5), async {
        while ws.ws_state.reconnect_count.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let (execution, collection) = finish(task).await;
    assert!(execution.is_ok() && collection.is_ok());
    assert_eq!(arena.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn production_closed_admission_never_connects_or_opens_observation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = websocket(format!("http://{}", listener.local_addr().unwrap()));
    let arena = Arc::new(Arena::default());
    arena.closed.store(true, Ordering::SeqCst);
    let task = ws.clone().spawn_managed(arena.clone());
    // Await the actual producer ending on admission refusal, without requesting
    // stop first (which would conceal a missing admission check).
    let mut task = task;
    let result = tokio::time::timeout(Duration::from_secs(2), &mut task.receipt)
        .await
        .unwrap()
        .unwrap();
    let (execution, collection) = result.into_parts();
    assert!(execution.is_ok() && collection.is_ok());
    assert_eq!(arena.active.load(Ordering::SeqCst), 0);
    assert!(ws
        .ws_state
        .observation_admission_closed
        .load(Ordering::Acquire));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn production_stop_does_not_drop_pending_connect_handshake() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = websocket(format!("http://{}", listener.local_addr().unwrap()));
    let arena = Arc::new(Arena::default());
    let task = ws.spawn_managed(arena.clone());
    // TCP accepted but no WebSocket handshake response: connect_async is active.
    let (stream, _) = listener.accept().await.unwrap();
    let mut receipt = Box::pin(task.finish());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut receipt)
            .await
            .is_err()
    );
    assert_eq!(arena.active.load(Ordering::SeqCst), 1);
    drop(stream);
    let (execution, collection) = tokio::time::timeout(Duration::from_secs(2), receipt)
        .await
        .unwrap()
        .into_parts();
    assert!(execution.is_ok() && collection.is_ok());
    assert_eq!(arena.active.load(Ordering::SeqCst), 0);
}

async fn production_author_result(catchup: bool, elapsed: bool) {
    use futures_util::{SinkExt, StreamExt};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let http = MockServer::start().await;
    for (route, value) in [
        (
            "/api/v4/teams/name/test-team",
            serde_json::json!({"id":"team"}),
        ),
        (
            "/api/v4/users/me/teams",
            serde_json::json!([{"id":"team","name":"test-team","display_name":"Test"}]),
        ),
        (
            "/api/v4/teams/team/channels/name/general",
            serde_json::json!({"id":"ch","name":"general","display_name":"General","type":"O","team_id":"team"}),
        ),
        (
            "/api/v4/users/me/teams/team/channels",
            serde_json::json!([{"id":"ch","name":"general","display_name":"General","type":"O","last_post_at":0}]),
        ),
        (
            "/api/v4/channels/ch/posts",
            serde_json::json!({"posts":{"post":{"id":"post","user_id":"author","message":"test","create_at":1}}}),
        ),
    ] {
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(value))
            .mount(&http)
            .await;
    }
    let response = if elapsed {
        ResponseTemplate::new(200).set_delay(Duration::from_secs(30))
    } else {
        ResponseTemplate::new(500)
    };
    Mock::given(method("GET"))
        .and(path("/api/v4/users/author"))
        .respond_with(response)
        .mount(&http)
        .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut ws = websocket(http.uri());
    let inner = Arc::get_mut(&mut ws).unwrap();
    inner.ws_url = format!("ws://{}", listener.local_addr().unwrap());
    if catchup {
        inner.monitored_channels = vec!["general".into()];
    }
    inner.inbox_armed.store(1, Ordering::SeqCst);
    let mut events = ws.event_bus.subscribe();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(crate::WsMessage::Text(r#"{"event":"hello"}"#.into()))
            .await
            .unwrap();
        if !catchup {
            let post = serde_json::json!({"id":"post","channel_id":"ch","user_id":"author","message":"test","create_at":1});
            let frame = serde_json::json!({"event":"posted","data":{"post":post.to_string(),"channel_type":"D"}});
            socket
                .send(crate::WsMessage::Text(frame.to_string().into()))
                .await
                .unwrap();
        }
        while let Some(Ok(_)) = socket.next().await {}
    });
    let arena = Arc::new(Arena::default());
    let task = ws.clone().spawn_managed(arena.clone());
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if matches!(
                events.recv().await.unwrap().kind,
                crate::DaemonEventKind::InboundMessage
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    let (execution, collection) = finish(task).await;
    assert!(collection.is_ok());
    assert_eq!(execution.is_err(), elapsed);
    assert_eq!(arena.active.load(Ordering::SeqCst), usize::from(elapsed));
    assert!(!ws.ws_state.catchup_in_flight.load(Ordering::Relaxed));
    assert!(ws
        .ws_state
        .observation_admission_closed
        .load(Ordering::Acquire));
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn production_inbound_timeout_retains_owner_after_fallback_publication() {
    production_author_result(false, true).await;
}
#[tokio::test]
async fn production_catchup_timeout_retains_owner_after_latch_clear() {
    production_author_result(true, true).await;
}
#[tokio::test]
async fn production_natural_author_errors_complete_inbound_and_catchup() {
    production_author_result(false, false).await;
    production_author_result(true, false).await;
}

struct BlockedSink {
    started: Arc<Notify>,
    release: oneshot::Receiver<()>,
    fail: bool,
}
impl futures_util::Sink<crate::WsMessage> for BlockedSink {
    type Error = std::io::Error;
    fn poll_ready(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, _: crate::WsMessage) -> Result<(), Self::Error> {
        self.started.notify_one();
        Ok(())
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match Pin::new(&mut self.release).poll(cx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(_) => std::task::Poll::Ready(if self.fail {
                Err(std::io::Error::other("synthetic send failure"))
            } else {
                Ok(())
            }),
        }
    }
    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

#[tokio::test]
async fn production_send_helper_retains_auth_pong_and_heartbeat_through_stop_and_error() {
    for payload in [
        crate::WsMessage::Text("authentication_challenge".into()),
        crate::WsMessage::Pong(Vec::new().into()),
        crate::WsMessage::Text("user_activity".into()),
    ] {
        let arena = Arc::new(Arena::default());
        let started = Arc::new(Notify::new());
        let ready = started.clone();
        let (release, gate) = oneshot::channel();
        let task = launch(arena.clone(), move |_, execution| async move {
            let mut sink = BlockedSink {
                started: ready,
                release: gate,
                fail: true,
            };
            let mut residuals = Vec::new();
            assert!(
                send_owned(Some(&execution), &mut sink, &mut residuals, || payload)
                    .await
                    .unwrap()
                    .is_err()
            );
            drop(sink);
            for owner in residuals {
                owner.complete();
            }
        });
        started.notified().await;
        let mut receipt = Box::pin(task.finish());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut receipt)
                .await
                .is_err()
        );
        assert_eq!(arena.active.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        let (execution, collection) = receipt.await.into_parts();
        assert!(execution.is_ok() && collection.is_ok());
        assert_eq!(arena.active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn production_recovery_sleeper_is_stopped_collected_and_cannot_reopen() {
    use futures_util::{SinkExt, StreamExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = websocket(format!("http://{}", listener.local_addr().unwrap()));
    ws.ws_state.last_disconnect_at.store(1, Ordering::Relaxed);
    ws.ws_state.reconnect_count.store(1, Ordering::Release);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(crate::WsMessage::Text(r#"{"event":"hello"}"#.into()))
            .await
            .unwrap();
        while let Some(Ok(_)) = socket.next().await {}
    });
    let arena = Arc::new(Arena::default());
    let task = ws.clone().spawn_managed(arena.clone());
    tokio::time::timeout(Duration::from_secs(2), async {
        while ws.ws_state.recovering_until.load(Ordering::Relaxed) == 0
            || ws.ws_state.catchup_in_flight.load(Ordering::Relaxed)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (execution, collection) = finish(task).await;
    assert!(execution.is_ok() && collection.is_ok());
    assert_eq!(arena.active.load(Ordering::SeqCst), 0);
    assert_eq!(ws.ws_state.last_recovered_at.load(Ordering::Relaxed), 0);
    assert!(ws
        .ws_state
        .observation_admission_closed
        .load(Ordering::Acquire));
    server.await.unwrap();
}

#[tokio::test]
async fn production_denied_auth_success_does_not_prepare_or_publish() {
    use futures_util::{SinkExt, StreamExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = websocket(format!("http://{}", listener.local_addr().unwrap()));
    let arena = Arc::new(Arena::default());
    let server_arena = arena.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket.next().await.unwrap().unwrap();
        server_arena.closed.store(true, Ordering::SeqCst);
        socket
            .send(crate::WsMessage::Text(r#"{"event":"hello"}"#.into()))
            .await
            .unwrap();
        while let Some(Ok(_)) = socket.next().await {}
    });
    let mut task = ws.clone().spawn_managed(arena.clone());
    let (execution, collection) = tokio::time::timeout(Duration::from_secs(2), &mut task.receipt)
        .await
        .unwrap()
        .unwrap()
        .into_parts();
    assert!(execution.is_ok() && collection.is_ok());
    assert_eq!(arena.active.load(Ordering::SeqCst), 0);
    assert!(!ws.ws_state.catchup_in_flight.load(Ordering::Relaxed));
    assert_eq!(ws.ws_state.recovering_until.load(Ordering::Relaxed), 0);
    assert!(ws
        .ws_state
        .observation_admission_closed
        .load(Ordering::Acquire));
    server.await.unwrap();
}

#[tokio::test]
async fn missing_receipt_is_unknown_execution_and_unconfirmed_collection() {
    let (stop, _receiver) = watch::channel(false);
    let (sender, receipt) = oneshot::channel();
    drop(sender);
    let (execution, collection) = finish(ManagedWs { stop, receipt }).await;
    assert!(execution.is_err());
    assert_eq!(collection, Err(WsCollectionError::CollectorUnavailable));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receipt_waits_for_actual_child_destructor() {
    struct Destructor {
        entered: Arc<Notify>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl Drop for Destructor {
        fn drop(&mut self) {
            self.entered.notify_one();
            self.release.recv_timeout(Duration::from_secs(3)).unwrap();
        }
    }
    let entered = Arc::new(Notify::new());
    let signal = entered.clone();
    let (release, gate) = std::sync::mpsc::channel();
    let task = launch(Arc::new(Arena::default()), move |_, execution| async move {
        execution.register(async move {
            let _destructor = Destructor {
                entered: signal,
                release: gate,
            };
        });
    });
    entered.notified().await;
    let mut receipt = Box::pin(task.finish());
    let pending = tokio::time::timeout(Duration::from_millis(30), &mut receipt)
        .await
        .is_err();
    // Always release the real destructor before asserting, including faults.
    release.send(()).unwrap();
    assert!(pending, "receipt preceded actual child destruction");
    let (execution, collection) = receipt.await.into_parts();
    assert!(execution.is_ok() && collection.is_ok());
}

#[derive(Default)]
struct NativeState {
    written: Mutex<Vec<u8>>,
    writes: AtomicUsize,
    flushes: AtomicUsize,
    reads: AtomicUsize,
    dropped_with_owners: AtomicUsize,
    dropped: AtomicBool,
}

// The real locked tungstenite codec owns its buffers. This fixture controls
// only the underlying I/O; it does not implement or simulate a WebSocket sink.
struct NativeIo {
    state: Arc<NativeState>,
    arena: Arc<Arena>,
    fail_write: Option<usize>,
    first_frame: Option<Vec<u8>>,
    busy: bool,
    read_delay: Duration,
}
impl Drop for NativeIo {
    fn drop(&mut self) {
        self.state
            .dropped_with_owners
            .store(self.arena.active.load(Ordering::SeqCst), Ordering::SeqCst);
        self.state.dropped.store(true, Ordering::SeqCst);
    }
}
impl tokio::io::AsyncRead for NativeIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let count = self.state.reads.fetch_add(1, Ordering::SeqCst);
        // Bound fault-control failures instead of spinning forever.
        if count >= 200 {
            return std::task::Poll::Ready(Err(std::io::Error::other("fixture read limit")));
        }
        let frame = self
            .first_frame
            .take()
            .or_else(|| self.busy.then(|| text_frame("{}")));
        let Some(frame) = frame else {
            return std::task::Poll::Pending;
        };
        std::thread::sleep(self.read_delay);
        buf.put_slice(&frame);
        std::task::Poll::Ready(Ok(()))
    }
}
impl tokio::io::AsyncWrite for NativeIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let call = self.state.writes.fetch_add(1, Ordering::SeqCst);
        if self.fail_write == Some(call) {
            return std::task::Poll::Ready(Err(std::io::Error::other(
                "injected partial-write failure",
            )));
        }
        let size = if self.fail_write == Some(call + 1) {
            3.min(buf.len())
        } else {
            buf.len()
        };
        self.state
            .written
            .lock()
            .unwrap()
            .extend_from_slice(&buf[..size]);
        std::task::Poll::Ready(Ok(size))
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.state.flushes.fetch_add(1, Ordering::SeqCst);
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
fn text_frame(text: &str) -> Vec<u8> {
    assert!(text.len() < 126);
    let mut frame = vec![0x81, text.len() as u8];
    frame.extend_from_slice(text.as_bytes());
    frame
}
fn native_io(arena: Arc<Arena>, state: Arc<NativeState>) -> NativeIo {
    NativeIo {
        state,
        arena,
        fail_write: None,
        first_frame: None,
        busy: false,
        read_delay: Duration::ZERO,
    }
}
async fn native_socket(io: NativeIo) -> tokio_tungstenite::WebSocketStream<NativeIo> {
    tokio_tungstenite::WebSocketStream::from_raw_socket(
        io,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await
}

#[tokio::test]
async fn native_partial_send_retains_owner_through_later_flush_and_transport_drop() {
    use futures_util::SinkExt;
    for payload in [
        crate::WsMessage::Text("test-token-synthetic".into()),
        crate::WsMessage::Pong(vec![1, 2, 3].into()),
        crate::WsMessage::Text("user_activity".into()),
    ] {
        let arena = Arc::new(Arena::default());
        let state = Arc::new(NativeState::default());
        let a = arena.clone();
        let s = state.clone();
        let task = launch(arena.clone(), move |_, execution| async move {
            let mut io = native_io(a.clone(), s.clone());
            io.fail_write = Some(1);
            let mut socket = native_socket(io).await;
            let mut residuals = Vec::new();
            assert!(
                send_owned(Some(&execution), &mut socket, &mut residuals, || payload)
                    .await
                    .unwrap()
                    .is_err()
            );
            assert_eq!(s.written.lock().unwrap().len(), 3);
            assert_eq!(
                a.active.load(Ordering::SeqCst),
                1,
                "native buffer still has output"
            );
            socket.flush().await.unwrap();
            assert!(
                s.written.lock().unwrap().len() > 3,
                "locked native sink must flush the retained suffix"
            );
            assert_eq!(a.active.load(Ordering::SeqCst), 1);
            drop(socket);
            assert_eq!(s.dropped_with_owners.load(Ordering::SeqCst), 1);
            for owner in residuals {
                owner.complete();
            }
        });
        // Wait for the actual body, without stopping its admission first.
        let mut task = task;
        let (execution, collection) = (&mut task.receipt).await.unwrap().into_parts();
        assert!(execution.is_ok() && collection.is_ok());
        assert_eq!(arena.active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn production_session_destroys_failed_auth_pong_and_heartbeat_buffers_before_confirmation() {
    for output in ["auth", "pong", "heartbeat"] {
        let arena = Arc::new(Arena::default());
        let state = Arc::new(NativeState::default());
        let a = arena.clone();
        let s = state.clone();
        let mut task = launch(arena.clone(), move |mut stop, execution| async move {
            let mut io = native_io(a, s);
            io.fail_write = Some(if output == "auth" { 1 } else { 2 });
            if output == "pong" {
                io.first_frame = Some(vec![0x89, 3, 1, 2, 3]);
            }
            if output == "heartbeat" {
                io.first_frame = Some(text_frame(r#"{"event":"hello"}"#));
            }
            let ws = websocket("http://localhost:1".into());
            let result = ws
                .listen_owned(
                    native_socket(io).await,
                    &mut stop,
                    Some(&execution),
                    Duration::from_secs(10),
                    Duration::from_millis(5),
                )
                .await;
            assert_eq!(result.is_err(), output == "auth");
        });
        let (execution, collection) =
            tokio::time::timeout(Duration::from_secs(2), &mut task.receipt)
                .await
                .unwrap()
                .unwrap()
                .into_parts();
        assert!(execution.is_ok() && collection.is_ok());
        assert!(state.dropped.load(Ordering::SeqCst));
        assert_eq!(state.dropped_with_owners.load(Ordering::SeqCst), 1);
        assert_eq!(arena.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            state.writes.load(Ordering::SeqCst),
            if output == "auth" { 2 } else { 3 },
            "failed output must never be flushed by a later read"
        );
    }
}

#[tokio::test]
async fn native_failed_send_unwinding_does_not_confirm_residual_owner() {
    let arena = Arc::new(Arena::default());
    let state = Arc::new(NativeState::default());
    let a = arena.clone();
    let s = state.clone();
    let mut task = launch(arena.clone(), move |_, execution| async move {
        let mut io = native_io(a, s);
        io.fail_write = Some(1);
        let mut socket = native_socket(io).await;
        let mut residuals = Vec::new();
        assert!(
            send_owned(Some(&execution), &mut socket, &mut residuals, || {
                crate::WsMessage::Text("test-token-synthetic".into())
            })
            .await
            .unwrap()
            .is_err()
        );
        panic!("injected unwind before explicit transport retirement");
    });
    let (execution, collection) = (&mut task.receipt).await.unwrap().into_parts();
    assert!(execution.is_err());
    assert_eq!(collection, Err(WsCollectionError::OuterJoinUnconfirmed));
    assert!(state.dropped.load(Ordering::SeqCst));
    assert_eq!(arena.active.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_busy_input_cannot_starve_auth_expiry_or_heartbeat() {
    for managed in [false, true] {
        for authenticated in [false, true] {
            let arena = Arc::new(Arena::default());
            let state = Arc::new(NativeState::default());
            let a = arena.clone();
            let s = state.clone();
            let mut task = launch(arena.clone(), move |mut stop, execution| async move {
                let mut io = native_io(a, s.clone());
                io.busy = true;
                io.read_delay = Duration::from_millis(1);
                if authenticated {
                    io.first_frame = Some(text_frame(r#"{"event":"hello"}"#));
                }
                let ws = websocket("http://localhost:1".into());
                let result = ws
                    .listen_owned(
                        native_socket(io).await,
                        &mut stop,
                        managed.then_some(&execution),
                        Duration::from_millis(20),
                        Duration::from_millis(5),
                    )
                    .await;
                if authenticated {
                    assert!(
                        s.flushes.load(Ordering::SeqCst) > 2,
                        "busy reads starved repeated heartbeats"
                    );
                } else {
                    assert!(
                        matches!(result, Err(CoreError::Io(ref e)) if e.kind() == std::io::ErrorKind::TimedOut)
                    );
                    assert!(s.reads.load(Ordering::SeqCst) < 200);
                }
            });
            let (execution, collection) = (&mut task.receipt).await.unwrap().into_parts();
            assert!(execution.is_ok() && collection.is_ok());
            assert_eq!(arena.active.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn production_rejects_auth_success_returned_after_deadline() {
    let arena = Arc::new(Arena::default());
    let mut io = native_io(arena, Arc::new(NativeState::default()));
    io.first_frame = Some(text_frame(r#"{"event":"hello"}"#));
    io.read_delay = Duration::from_millis(30);
    io.busy = true;
    let ws = websocket("http://localhost:1".into());
    let (_sender, mut stop) = watch::channel(false);
    let result = ws
        .listen_owned(
            native_socket(io).await,
            &mut stop,
            None,
            Duration::from_millis(10),
            Duration::from_secs(30),
        )
        .await;
    assert!(
        matches!(result, Err(CoreError::Io(ref e)) if e.kind() == std::io::ErrorKind::TimedOut)
    );
    assert!(!ws.ws_state.catchup_in_flight.load(Ordering::Relaxed));
    assert_eq!(ws.ws_state.recovering_until.load(Ordering::Relaxed), 0);
}
