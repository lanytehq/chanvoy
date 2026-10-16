use super::*;
use crate::subscription_owner;
use chanvoy_core::{ConnectionStateChangedPayload, WsConnectionState};
use std::sync::Condvar;

struct Wire {
    probe: Arc<Probe>,
    pause: bool,
    fail: bool,
    entered: Notify,
    release: Notify,
    delivered: Notify,
    frames: Mutex<Vec<Value>>,
    destruction: Option<Arc<Destruction>>,
}
impl Wire {
    fn new(probe: &Arc<Probe>, pause: bool, fail: bool) -> Arc<Self> {
        Arc::new(Self {
            probe: Arc::clone(probe),
            pause,
            fail,
            entered: Notify::new(),
            release: Notify::new(),
            delivered: Notify::new(),
            frames: Mutex::new(vec![]),
            destruction: None,
        })
    }
}
impl RequestTransport for Arc<Wire> {
    async fn send(&self, _: u16, payload: &[u8]) -> SendResult {
        assert!(self.probe.active.load(Ordering::SeqCst) > 0);
        let _destruction = self
            .destruction
            .as_ref()
            .map(|gate| DestructionGuard(Arc::clone(gate)));
        if self.pause {
            self.entered.notify_one();
            self.release.notified().await;
        }
        assert!(self.probe.active.load(Ordering::SeqCst) > 0);
        if self.fail {
            return Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into());
        }
        self.frames
            .lock()
            .unwrap()
            .push(serde_json::from_slice(payload)?);
        self.delivered.notify_one();
        Ok(())
    }
}
async fn notify(signal: &Notify) {
    tokio::time::timeout(Duration::from_secs(2), signal.notified())
        .await
        .unwrap();
}
async fn requests(probe: &Probe, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while probe.active.load(Ordering::SeqCst) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn subscribed(peer: &IpcPeer, probe: &Arc<Probe>) -> String {
    let tx = Wire::new(probe, false, false);
    peer.handle_frame(request("chat_subscribe_request", false), &tx, "fixture")
        .await
        .unwrap();
    let frames = tx.frames.lock().unwrap();
    frames.last().unwrap()["subscription_id"]
        .as_str()
        .unwrap()
        .to_owned()
}
async fn unsubscribe(peer: &IpcPeer, probe: &Arc<Probe>, id: &str) -> bool {
    let frame = Frame::new(
        CHANNEL_260,
        serde_json::to_vec(&ChatFrame::UnsubscribeRequest {
            request_id: "unsubscribe".into(),
            delegation_id: "fixture".into(),
            subscription_id: id.into(),
        })
        .unwrap(),
    );
    let tx = Wire::new(probe, false, false);
    peer.handle_frame(frame, &tx, "fixture").await.unwrap();
    let frames = tx.frames.lock().unwrap();
    frames.last().unwrap()["success"].as_bool().unwrap()
}
fn event(gap: bool) -> DaemonEvent {
    if gap {
        DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::Gap,
            payload: DaemonEventPayloadInner::Gap(GapPayload {
                subscription_id: "prior".into(),
                missed_from_seq: 1,
                missed_to_seq: 2,
            }),
        }
    } else {
        DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::ConnectionStateChanged,
            payload: DaemonEventPayloadInner::ConnectionStateChanged(
                ConnectionStateChangedPayload {
                    profile: "fixture".into(),
                    provider: Provider::Mattermost,
                    state: WsConnectionState::Healthy,
                    message: "synthetic".into(),
                },
            ),
        }
    }
}
fn run(peer: &IpcPeer, wire: Arc<Wire>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(peer.forwarding_state().run(
        peer.event_bus.subscribe(),
        wire,
        CancellationToken::new(),
    ))
}
async fn joined(task: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(
        Duration::from_secs(3),
        subscription_owner::abort_and_collect(task),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn duplicate_and_missing_ids_do_not_replace_or_clear_another_registration() {
    let probe = Arc::new(Probe::default());
    let peer = peer("http://127.0.0.1:9", &probe);
    let first = subscribed(&peer, &probe).await;
    let request = peer.request_admission.reserve_request().unwrap();
    let result = subscription_owner::insert(
        &mut *peer.subscriptions.lock().await,
        first.clone(),
        IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: None,
            mentions_only: Some(true),
        },
        request.as_ref(),
    );
    assert!(result.is_err());
    assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
    assert_eq!(probe.stream_reservations.load(Ordering::SeqCst), 1);
    assert_eq!(
        peer.subscriptions.lock().await[&first].filter.mentions_only,
        None
    );
    request.confirm_done().unwrap();
    let second = subscribed(&peer, &probe).await;
    assert_ne!(first, second);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 2);
    assert!(!unsubscribe(&peer, &probe, "missing-old-id").await);
    assert!(unsubscribe(&peer, &probe, &first).await);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
    assert!(peer.subscriptions.lock().await.contains_key(&second));
    assert!(!unsubscribe(&peer, &probe, &first).await);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
    assert!(unsubscribe(&peer, &probe, &second).await);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn clean_old_forwarder_exit_and_replacement_preserve_original_registration() {
    let probe = Arc::new(Probe::default());
    let peer = peer("http://127.0.0.1:9", &probe);
    assert!(Arc::ptr_eq(
        &peer.subscriptions,
        &peer.forwarding_state().subscriptions
    ));
    let id = subscribed(&peer, &probe).await;
    let first = Wire::new(&probe, true, false);
    let old = run(&peer, Arc::clone(&first));
    peer.event_bus.emit(event(false));
    notify(&first.entered).await;
    assert_eq!(probe.active.load(Ordering::SeqCst), 1);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
    first.release.notify_one();
    notify(&first.delivered).await;
    requests(&probe, 0).await;
    joined(old).await;
    let successor = peer.forwarding_state();
    assert!(successor.subscriptions.lock().await.contains_key(&id));
    assert_eq!(probe.stream_reservations.load(Ordering::SeqCst), 1);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
    let second = Wire::new(&probe, false, false);
    let next = tokio::spawn(successor.run(
        peer.event_bus.subscribe(),
        Arc::clone(&second),
        CancellationToken::new(),
    ));
    peer.event_bus.emit(event(true));
    notify(&second.delivered).await;
    requests(&probe, 0).await;
    joined(next).await;
    assert_eq!(second.frames.lock().unwrap()[0]["subscription_id"], id);
    assert_eq!(probe.stream_reservations.load(Ordering::SeqCst), 1);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
    assert!(unsubscribe(&peer, &probe, &id).await);
    assert_eq!(
        (
            probe.active.load(Ordering::SeqCst),
            probe.streams.load(Ordering::SeqCst)
        ),
        (0, 0)
    );
}

#[tokio::test]
async fn failed_or_aborted_forward_output_stays_owned_after_exact_unsubscribe() {
    for gap in [false, true] {
        for abort in [false, true] {
            let probe = Arc::new(Probe::default());
            let peer = peer("http://127.0.0.1:9", &probe);
            let id = subscribed(&peer, &probe).await;
            let wire = Wire::new(&probe, true, !abort);
            let task = run(&peer, Arc::clone(&wire));
            peer.event_bus.emit(event(gap));
            notify(&wire.entered).await;
            if abort {
                joined(task).await;
            } else {
                wire.release.notify_one();
                tokio::time::timeout(Duration::from_secs(2), task)
                    .await
                    .unwrap()
                    .unwrap();
            }
            assert_eq!(
                (
                    probe.active.load(Ordering::SeqCst),
                    probe.streams.load(Ordering::SeqCst)
                ),
                (1, 1)
            );
            assert!(peer
                .forwarding_state()
                .subscriptions
                .lock()
                .await
                .contains_key(&id));
            assert_eq!(probe.stream_reservations.load(Ordering::SeqCst), 1);
            assert!(unsubscribe(&peer, &probe, &id).await);
            assert_eq!(
                (
                    probe.active.load(Ordering::SeqCst),
                    probe.streams.load(Ordering::SeqCst)
                ),
                (1, 0)
            );
        }
    }
}

#[tokio::test]
async fn concurrent_unsubscribe_cannot_erase_selected_forward_ownership() {
    let probe = Arc::new(Probe::default());
    let peer = peer("http://127.0.0.1:9", &probe);
    let id = subscribed(&peer, &probe).await;
    let wire = Wire::new(&probe, true, false);
    let old = run(&peer, Arc::clone(&wire));
    peer.event_bus.emit(event(false));
    notify(&wire.entered).await;
    let other_peer = Arc::clone(&peer);
    let other_probe = Arc::clone(&probe);
    let removal = tokio::spawn(async move { unsubscribe(&other_peer, &other_probe, &id).await });
    requests(&probe, 2).await;
    assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
    assert!(!removal.is_finished());
    wire.release.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(2), removal)
        .await
        .unwrap()
        .unwrap());
    joined(old).await;
    assert_eq!(
        (
            probe.active.load(Ordering::SeqCst),
            probe.streams.load(Ordering::SeqCst)
        ),
        (0, 0)
    );
}

#[tokio::test]
async fn drift_suppression_and_admission_refusal_preserve_streams_without_output() {
    let probe = Arc::new(Probe::default());
    let peer = peer("http://127.0.0.1:9", &probe);
    let id = subscribed(&peer, &probe).await;
    let baseline = probe.reservations.load(Ordering::SeqCst);
    let forwarding = peer.forwarding_state();
    let wire = Wire::new(&probe, false, false);
    peer.identity_drift.store(true, Ordering::SeqCst);
    forwarding.event(&event(true), &wire).await.unwrap();
    assert_eq!(probe.reservations.load(Ordering::SeqCst), baseline);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
    peer.identity_drift.store(false, Ordering::SeqCst);
    probe.refuse.store(2, Ordering::SeqCst);
    assert!(forwarding.event(&event(true), &wire).await.is_err());
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
    assert!(wire.frames.lock().unwrap().is_empty());
    assert!(forwarding.subscriptions.lock().await.contains_key(&id));
}

#[tokio::test]
async fn idle_empty_forwarder_has_no_permanent_operational_owner() {
    let probe = Arc::new(Probe::default());
    let peer = peer("http://127.0.0.1:9", &probe);
    let wire = Wire::new(&probe, false, false);
    let task = run(&peer, Arc::clone(&wire));
    peer.event_bus.emit(event(false));
    tokio::time::timeout(Duration::from_secs(2), async {
        while !peer.event_bus.sender().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(probe.reservations.load(Ordering::SeqCst), 0);
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    assert!(wire.frames.lock().unwrap().is_empty());
    joined(task).await;
    assert_eq!(probe.streams.load(Ordering::SeqCst), 0);
}

struct Destruction {
    entered: Notify,
    gate: (Mutex<bool>, Condvar),
}
impl Destruction {
    fn release(&self) {
        let (lock, changed) = &self.gate;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
}
struct DestructionGuard(Arc<Destruction>);
impl Drop for DestructionGuard {
    fn drop(&mut self) {
        self.0.entered.notify_one();
        let (lock, changed) = &self.0.gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            let (next, timeout) = changed
                .wait_timeout(released, Duration::from_secs(5))
                .unwrap();
            released = next;
            if timeout.timed_out() {
                break;
            } // Failed-test teardown bound only.
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_waits_for_actual_old_send_destruction_and_join() {
    let probe = Arc::new(Probe::default());
    let peer = peer("http://127.0.0.1:9", &probe);
    let id = subscribed(&peer, &probe).await;
    let destruction = Arc::new(Destruction {
        entered: Notify::new(),
        gate: (Mutex::new(false), Condvar::new()),
    });
    let wire = Arc::new(Wire {
        probe: Arc::clone(&probe),
        pause: true,
        fail: false,
        entered: Notify::new(),
        release: Notify::new(),
        delivered: Notify::new(),
        frames: Mutex::new(vec![]),
        destruction: Some(Arc::clone(&destruction)),
    });
    let old = run(&peer, Arc::clone(&wire));
    peer.event_bus.emit(event(false));
    notify(&wire.entered).await;
    // Poll the actual collection future while the child's destructor is
    // held. A marker after a competing map lock could hide a missing join.
    use std::future::Future;
    use std::task::Poll;
    let mut collection = Box::pin(subscription_owner::abort_and_collect(old));
    let mut observed = std::future::poll_fn(|cx| Poll::Ready(collection.as_mut().poll(cx))).await;
    notify(&destruction.entered).await;
    if observed.is_pending() {
        observed = std::future::poll_fn(|cx| Poll::Ready(collection.as_mut().poll(cx))).await;
    }
    let join_pending = observed.is_pending();
    let snapshot = (
        probe.active.load(Ordering::SeqCst),
        probe.streams.load(Ordering::SeqCst),
    );
    destruction.release();
    match observed {
        Poll::Pending => collection.await.unwrap(),
        Poll::Ready(result) => result.unwrap(),
    }
    let state = peer.forwarding_state();
    assert!(state.subscriptions.lock().await.contains_key(&id));
    let next = tokio::spawn(state.run(
        peer.event_bus.subscribe(),
        Wire::new(&probe, false, false),
        CancellationToken::new(),
    ));
    joined(next).await;
    assert!(
        join_pending,
        "replacement became eligible before the old join completed"
    );
    assert_eq!(snapshot, (1, 1));
    assert_eq!(probe.stream_reservations.load(Ordering::SeqCst), 1);
    assert_eq!(
        (
            probe.active.load(Ordering::SeqCst),
            probe.streams.load(Ordering::SeqCst)
        ),
        (1, 1)
    );
}

struct PanicWire {
    probe: Arc<Probe>,
    entered: Arc<Notify>,
}
impl RequestTransport for PanicWire {
    async fn send(&self, _: u16, _: &[u8]) -> SendResult {
        assert_eq!(self.probe.active.load(Ordering::SeqCst), 1);
        self.entered.notify_one();
        panic!("synthetic send panic")
    }
}
#[tokio::test]
async fn collected_panic_is_not_success_and_does_not_release_stream_or_output() {
    let probe = Arc::new(Probe::default());
    let peer = peer("http://127.0.0.1:9", &probe);
    let id = subscribed(&peer, &probe).await;
    let entered = Arc::new(Notify::new());
    let task = tokio::spawn(peer.forwarding_state().run(
        peer.event_bus.subscribe(),
        PanicWire {
            probe: Arc::clone(&probe),
            entered: Arc::clone(&entered),
        },
        CancellationToken::new(),
    ));
    peer.event_bus.emit(event(false));
    notify(&entered).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(subscription_owner::abort_and_collect(task).await.is_err());
    assert_eq!(
        (
            probe.active.load(Ordering::SeqCst),
            probe.streams.load(Ordering::SeqCst)
        ),
        (1, 1)
    );
    assert!(peer.subscriptions.lock().await.contains_key(&id));
}

#[tokio::test]
async fn lifecycle_audit_and_session_loss_preserve_existing_registration() {
    for failed in [false, true] {
        let probe = Arc::new(Probe::default());
        let peer = peer("http://127.0.0.1:9", &probe);
        let id = subscribed(&peer, &probe).await;
        let wire = Wire::new(&probe, true, failed);
        let audit_peer = Arc::clone(&peer);
        let tx = Arc::clone(&wire);
        let audit = tokio::spawn(async move {
            crate::lifecycle_audit::emit(
                audit_peer.request_admission.as_ref(),
                &tx,
                "fixture",
                "chat.peer_disconnected",
            )
            .await
        });
        notify(&wire.entered).await;
        assert_eq!(probe.active.load(Ordering::SeqCst), 1);
        assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
        let old = run(&peer, Wire::new(&probe, false, false));
        joined(old).await;
        assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
        assert_eq!(probe.active.load(Ordering::SeqCst), 1);
        wire.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(3), audit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.is_err(), failed);
        assert_eq!(probe.active.load(Ordering::SeqCst), usize::from(failed));
        assert_eq!(probe.streams.load(Ordering::SeqCst), 1);
        assert!(peer
            .forwarding_state()
            .subscriptions
            .lock()
            .await
            .contains_key(&id));
        assert_eq!(probe.stream_reservations.load(Ordering::SeqCst), 1);
        assert!(unsubscribe(&peer, &probe, &id).await);
        assert_eq!(probe.streams.load(Ordering::SeqCst), 0);
        assert_eq!(probe.active.load(Ordering::SeqCst), usize::from(failed));
    }
}
