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
            assert!(send_owned(Some(&execution), &mut sink, || payload)
                .await
                .unwrap()
                .is_err());
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
