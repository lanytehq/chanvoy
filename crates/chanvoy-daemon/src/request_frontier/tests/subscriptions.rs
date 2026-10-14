use super::*;
use crate::subscription_owner;
use chanvoy_core::{
    DaemonEvent, DaemonEventKind, DaemonEventPayloadInner, InboundEventPayload, SubscriptionFilter,
};

struct FrameWriter {
    gate: Arc<Gate>,
    stage: Stage,
    target: usize,
    frame: usize,
    fail: bool,
    ack_hold: Option<Arc<Gate>>,
}

impl AsyncWrite for FrameWriter {
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
        if self.frame >= self.target && self.stage == stage {
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
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.frame == 0 {
            if let Some(ack) = &self.ack_hold {
                if ack.pending(cx) {
                    return Poll::Pending;
                }
            }
        }
        if self.frame >= self.target && self.stage == Stage::Flush {
            if self.fail {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            if self.gate.pending(cx) {
                return Poll::Pending;
            }
        }
        self.frame += 1;
        self.gate.flushed.notify_one();
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn make_writer(stage: Stage, target: usize, fail: bool) -> (FrameWriter, Arc<Gate>) {
    let gate = Arc::new(Gate::default());
    (
        FrameWriter {
            gate: Arc::clone(&gate),
            stage,
            target,
            frame: 0,
            fail,
            ack_hold: None,
        },
        gate,
    )
}

fn start(
    state: &Arc<AppState>,
    writer: FrameWriter,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<Result<(), DaemonError>>,
) {
    let (server, client) = tokio::io::duplex(4096);
    let (shutdown, _rx) = oneshot::channel();
    let task = tokio::spawn(crate::handle_client_io(
        BufReader::new(server),
        writer,
        Arc::clone(state),
        Arc::new(Mutex::new(Some(shutdown))),
    ));
    (client, task)
}

async fn send(client: &mut tokio::io::DuplexStream, method: &str, params: serde_json::Value) {
    let request = rpc_request(method, params);
    client
        .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
        .await
        .unwrap();
}

async fn notified(signal: &Notify) {
    tokio::time::timeout(Duration::from_secs(2), signal.notified())
        .await
        .unwrap();
}

async fn counts(state: &AppState, requests: usize, streams: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = state.wait_owners.admission_snapshot();
            if snapshot.requests == requests && snapshot.streams == streams {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

// Gates and collected joins establish these observations without polling.
fn assert_counts(state: &AppState, requests: usize, streams: usize) {
    let snapshot = state.wait_owners.admission_snapshot();
    assert_eq!((snapshot.requests, snapshot.streams), (requests, streams));
}

fn assert_busy(state: &AppState) {
    let snapshot = state.wait_owners.admission_snapshot();
    assert!(matches!(
        state.wait_owners.retire_if_idle(snapshot.instance),
        Ok(wait_owner::RetirementDecision::Busy(_))
    ));
}

fn documents(gate: &Gate) -> Vec<serde_json::Value> {
    let bytes = gate.bytes.lock().unwrap().clone();
    String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn subscribe(client: &mut tokio::io::DuplexStream, gate: &Gate, state: &AppState) -> String {
    send(client, "subscribe", json!({"filter":"all_monitored"})).await;
    notified(&gate.flushed).await;
    counts(state, 0, 1).await;
    documents(gate)[0]["result"]["subscription_id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn event() -> DaemonEvent {
    DaemonEvent {
        seq: 0,
        kind: DaemonEventKind::InboundMessage,
        payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
            profile: "test".into(),
            provider: Provider::Mattermost,
            channel_id: "channel-id".into(),
            channel_name: "example".into(),
            channel_type: "O".into(),
            post_id: "post-id".into(),
            root_id: "".into(),
            sender_id: "sender-id".into(),
            sender_username: "sender".into(),
            message: "synthetic message".into(),
            create_at: 1000,
            received_at: 1001,
            mentioned: false,
            mention_user_ids: None,
        }),
    }
}

#[tokio::test]
async fn registration_exists_before_subscribe_ack_and_failed_ack_retains_both_owners() {
    for stage in [Stage::Payload, Stage::Newline, Stage::Flush] {
        for fail in [false, true] {
            let state = state("http://127.0.0.1:9");
            let (writer, gate) = make_writer(stage, 0, fail);
            let (mut client, task) = start(&state, writer);
            send(&mut client, "subscribe", json!({"filter":"all_monitored"})).await;
            if fail {
                assert!(matches!(collect(task).await, Err(DaemonError::Io(_))));
                assert_counts(&state, 1, 1);
                assert_eq!(state.subscriptions.lock().await.len(), 1);
            } else {
                notified(&gate.reached).await;
                assert_counts(&state, 1, 1);
                assert_eq!(state.subscriptions.lock().await.len(), 1);
                assert_busy(&state);
                gate.release();
                notified(&gate.flushed).await;
                counts(&state, 0, 1).await;
                drop(client);
                collect(task).await.unwrap();
                counts(&state, 0, 0).await;
            }
        }
    }
}

#[tokio::test]
async fn push_and_gap_payload_newline_flush_keep_independent_send_ownership() {
    for gap in [false, true] {
        for stage in [Stage::Payload, Stage::Newline, Stage::Flush] {
            let state = state("http://127.0.0.1:9");
            let (mut writer, gate) = make_writer(stage, 1, false);
            let ack = Arc::new(Gate::default());
            if gap {
                writer.ack_hold = Some(Arc::clone(&ack));
            }
            let (mut client, task) = start(&state, writer);
            if gap {
                send(&mut client, "subscribe", json!({"filter":"all_monitored"})).await;
                notified(&ack.reached).await;
                for _ in 0..64 {
                    state.event_bus.emit(event());
                }
                ack.release();
            } else {
                subscribe(&mut client, &gate, &state).await;
                state.event_bus.emit(event());
            }
            notified(&gate.reached).await;
            assert_counts(&state, 1, 1);
            assert_busy(&state);
            gate.release();
            drop(client);
            collect(task).await.unwrap();
            counts(&state, 0, 0).await;
            let frames = documents(&gate);
            assert!(frames.len() >= 2);
            assert_eq!(frames[1]["method"] == "push.gap", gap);
        }
    }
}

#[tokio::test]
async fn failed_push_or_gap_keeps_send_owner_after_exact_registration_removal() {
    for gap in [false, true] {
        for stage in [Stage::Payload, Stage::Newline, Stage::Flush] {
            let state = state("http://127.0.0.1:9");
            let (mut writer, gate) = make_writer(stage, 1, true);
            let ack = Arc::new(Gate::default());
            if gap {
                writer.ack_hold = Some(Arc::clone(&ack));
            }
            let (mut client, task) = start(&state, writer);
            let id = if gap {
                send(&mut client, "subscribe", json!({"filter":"all_monitored"})).await;
                notified(&ack.reached).await;
                let id = state
                    .subscriptions
                    .lock()
                    .await
                    .keys()
                    .next()
                    .unwrap()
                    .clone();
                for _ in 0..64 {
                    state.event_bus.emit(event());
                }
                ack.release();
                id
            } else {
                let id = subscribe(&mut client, &gate, &state).await;
                state.event_bus.emit(event());
                id
            };
            assert!(matches!(collect(task).await, Err(DaemonError::Io(_))));
            assert_counts(&state, 1, 1);
            assert!(
                subscription_owner::remove(&mut *state.subscriptions.lock().await, &id).unwrap()
            );
            counts(&state, 1, 0).await;
            assert_busy(&state);
        }
    }
}

#[tokio::test]
async fn concurrent_unsubscribe_cannot_clear_a_selected_send() {
    let state = state("http://127.0.0.1:9");
    let (writer, gate) = make_writer(Stage::Flush, 1, false);
    let (mut client, task) = start(&state, writer);
    let id = subscribe(&mut client, &gate, &state).await;
    state.event_bus.emit(event());
    notified(&gate.reached).await;
    assert_counts(&state, 1, 1);
    let (other_writer, other_gate) = make_writer(Stage::Flush, usize::MAX, false);
    let (mut other, other_task) = start(&state, other_writer);
    send(&mut other, "unsubscribe", json!({"subscription_id":id})).await;
    counts(&state, 2, 1).await;
    assert_busy(&state);
    gate.release();
    notified(&other_gate.flushed).await;
    counts(&state, 0, 0).await;
    assert_eq!(documents(&other_gate)[0]["result"], true);
    drop(client);
    drop(other);
    collect(task).await.unwrap();
    collect(other_task).await.unwrap();
}

#[tokio::test]
async fn clean_eof_removes_only_its_connection_registrations() {
    let state = state("http://127.0.0.1:9");
    let (first_writer, first_gate) = make_writer(Stage::Flush, usize::MAX, false);
    let (mut first, first_task) = start(&state, first_writer);
    let first_id = subscribe(&mut first, &first_gate, &state).await;
    let (second_writer, second_gate) = make_writer(Stage::Flush, usize::MAX, false);
    let (mut second, second_task) = start(&state, second_writer);
    send(&mut second, "subscribe", json!({"filter":"all_monitored"})).await;
    notified(&second_gate.flushed).await;
    counts(&state, 0, 2).await;
    let second_id = documents(&second_gate)[0]["result"]["subscription_id"]
        .as_str()
        .unwrap()
        .to_string();
    drop(first);
    collect(first_task).await.unwrap();
    counts(&state, 0, 1).await;
    let map = state.subscriptions.lock().await;
    assert!(!map.contains_key(&first_id));
    assert!(map.contains_key(&second_id));
    drop(map);
    send(
        &mut second,
        "unsubscribe",
        json!({"subscription_id":first_id}),
    )
    .await;
    notified(&second_gate.flushed).await;
    counts(&state, 0, 1).await;
    assert_eq!(documents(&second_gate)[1]["result"], false);
    send(
        &mut second,
        "unsubscribe",
        json!({"subscription_id":second_id}),
    )
    .await;
    notified(&second_gate.flushed).await;
    counts(&state, 0, 0).await;
    drop(second);
    collect(second_task).await.unwrap();
}

#[tokio::test]
async fn parse_failure_or_dropped_connection_does_not_confirm_registration_cleanup() {
    for parse_error in [false, true] {
        let state = state("http://127.0.0.1:9");
        let (writer, gate) = make_writer(Stage::Flush, usize::MAX, false);
        let (mut client, task) = start(&state, writer);
        subscribe(&mut client, &gate, &state).await;
        if parse_error {
            client.write_all(b"not-json\n").await.unwrap();
            assert!(collect(task).await.is_err());
        } else {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        counts(&state, 0, 1).await;
        assert_eq!(state.subscriptions.lock().await.len(), 1);
        assert_busy(&state);
    }
}

#[test]
fn duplicate_missing_and_closed_registration_operations_do_not_clear_other_owners() {
    let registry = Arc::new(wait_owner::WaitOwnerRegistry::new());
    let mut map = HashMap::new();
    subscription_owner::insert(
        &mut map,
        &registry,
        "first".into(),
        SubscriptionFilter::AllMonitored,
    )
    .unwrap();
    assert!(subscription_owner::insert(
        &mut map,
        &registry,
        "first".into(),
        SubscriptionFilter::MentionsOnly
    )
    .is_err());
    assert_eq!(map["first"].filter, SubscriptionFilter::AllMonitored);
    assert_eq!(registry.admission_snapshot().streams, 1);
    assert!(!subscription_owner::remove(&mut map, "missing").unwrap());
    subscription_owner::insert(
        &mut map,
        &registry,
        "second".into(),
        SubscriptionFilter::MentionsOnly,
    )
    .unwrap();
    assert!(subscription_owner::remove(&mut map, "first").unwrap());
    assert_eq!(registry.admission_snapshot().streams, 1);
    assert_eq!(map["second"].filter, SubscriptionFilter::MentionsOnly);
    assert!(subscription_owner::remove(&mut map, "second").unwrap());
    let snapshot = registry.admission_snapshot();
    assert!(matches!(
        registry.retire_if_idle(snapshot.instance),
        Ok(wait_owner::RetirementDecision::Closed(_))
    ));
    assert!(matches!(
        subscription_owner::insert(
            &mut map,
            &registry,
            "late".into(),
            SubscriptionFilter::AllMonitored
        ),
        Err(CoreError::RuntimeAdmissionClosed)
    ));
    assert!(map.is_empty());
    assert_eq!(registry.admission_snapshot().streams, 0);
}

struct ErrorOnEof<R>(R);
impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for ErrorOnEof<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let available = buf.remaining();
        let result = Pin::new(&mut self.0).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && available > 0 && before == buf.filled().len() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        result
    }
}

#[tokio::test]
async fn read_error_is_not_clean_eof_registration_cleanup() {
    let state = state("http://127.0.0.1:9");
    let (writer, gate) = make_writer(Stage::Flush, usize::MAX, false);
    let (reader, mut client) = tokio::io::duplex(4096);
    let (shutdown, _rx) = oneshot::channel();
    let task = tokio::spawn(crate::handle_client_io(
        BufReader::new(ErrorOnEof(reader)),
        writer,
        Arc::clone(&state),
        Arc::new(Mutex::new(Some(shutdown))),
    ));
    subscribe(&mut client, &gate, &state).await;
    drop(client);
    assert!(matches!(collect(task).await, Err(DaemonError::Io(_))));
    counts(&state, 0, 1).await;
    assert_eq!(state.subscriptions.lock().await.len(), 1);
    assert_busy(&state);
}

#[tokio::test]
async fn actual_subscribe_after_closed_cannot_insert_or_emit_an_ack() {
    let state = state("http://127.0.0.1:9");
    let snapshot = state.wait_owners.admission_snapshot();
    assert!(matches!(
        state.wait_owners.retire_if_idle(snapshot.instance),
        Ok(wait_owner::RetirementDecision::Closed(_))
    ));
    let (writer, gate) = make_writer(Stage::Flush, usize::MAX, false);
    let (mut client, task) = start(&state, writer);
    send(&mut client, "subscribe", json!({"filter":"all_monitored"})).await;
    assert!(matches!(
        collect(task).await,
        Err(DaemonError::Core(CoreError::RuntimeAdmissionClosed))
    ));
    assert!(gate.bytes.lock().unwrap().is_empty());
    assert!(state.subscriptions.lock().await.is_empty());
    counts(&state, 0, 0).await;
}
