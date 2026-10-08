//! Actual pinned runner/production callback + production framed UDS writer.
use super::*;
use crate::follow_delivery::{tests::HeldWriter, ArmPause, ChannelDelivery};
use crate::wait_owner::WaitOwnerRegistry;
use std::sync::atomic::AtomicUsize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;

const P0: &str = "postid00000000000000000000";
const P1: &str = "postid00000000000000000001";
const P2: &str = "postid00000000000000000002";

#[derive(Default)]
struct Counts {
    binds: AtomicUsize,
    active: AtomicUsize,
    drops: AtomicUsize,
    next: AtomicUsize,
}
struct FixtureBind {
    reg: IdToken,
    start: Anchor,
    counts: Arc<Counts>,
}
impl BindHandle for FixtureBind {
    fn registration_id(&self) -> &IdToken {
        &self.reg
    }
    fn resolved_start(&self) -> &Anchor {
        &self.start
    }
}
impl Drop for FixtureBind {
    fn drop(&mut self) {
        self.counts.active.fetch_sub(1, Ordering::SeqCst);
        self.counts.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Clone)]
struct SamePollObserver {
    start: Anchor,
    ready: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    events: Arc<Mutex<VecDeque<Observation>>>,
    counts: Arc<Counts>,
}
impl Observer for SamePollObserver {
    type Bind = FixtureBind;
    async fn bind(&self, reg: &Registration) -> waitprims_core::Result<FixtureBind> {
        self.counts.binds.fetch_add(1, Ordering::SeqCst);
        self.counts.active.fetch_add(1, Ordering::SeqCst);
        self.ready.lock().unwrap().take().unwrap().send(()).unwrap();
        Ok(FixtureBind {
            reg: reg.registration_id.clone(),
            start: self.start.clone(),
            counts: Arc::clone(&self.counts),
        })
    }
    async fn next(&self, _: &FixtureBind) -> waitprims_core::Result<Observation> {
        self.counts.next.fetch_add(1, Ordering::SeqCst);
        let event = self.events.lock().unwrap().pop_front();
        match event {
            Some(event) => Ok(event),
            None => std::future::pending().await,
        }
    }
    async fn cancel(&self, _: &FixtureBind) -> waitprims_core::Result<()> {
        Ok(())
    }
    fn poll_ready(&self, _: &FixtureBind) -> Option<Observation> {
        self.events.lock().unwrap().pop_front()
    }
    fn restore_ready(&self, _: &FixtureBind, obs: Observation) -> waitprims_core::Result<()> {
        self.events.lock().unwrap().push_front(obs);
        Ok(())
    }
}

#[derive(Default)]
struct FixtureControl {
    before: Option<Arc<ArmPause>>,
    after_reservation: Option<Arc<ArmPause>>,
    reservation_started: Option<Arc<tokio::sync::Notify>>,
    full_queue: bool,
}

struct Fixture {
    registry: Arc<WaitOwnerRegistry>,
    session: WaitSession,
    release: Arc<LeaseRelease>,
    sidecar: MessageSidecar,
    tip: Arc<Mutex<Option<String>>>,
    inner_cancel: CancellationToken,
    delivery: ChannelDelivery,
    counts: Arc<Counts>,
    receive_no: usize,
    records: tokio::sync::mpsc::Receiver<crate::wait::FollowStreamRecord>,
    task: tokio::task::JoinHandle<Result<WaitFollowResult, CoreError>>,
}
impl Fixture {
    async fn start(coalesce: Option<u64>, direct: bool, legacy: bool, budget: Duration) -> Self {
        Self::start_entries(
            coalesce,
            direct,
            legacy,
            budget,
            &[
                (P1, FollowObservationPhase::Backlog),
                (P2, FollowObservationPhase::Backlog),
            ],
        )
        .await
    }

    async fn start_entries(
        coalesce: Option<u64>,
        direct: bool,
        legacy: bool,
        budget: Duration,
        entries: &[(&str, FollowObservationPhase)],
    ) -> Self {
        Self::start_controlled(
            coalesce,
            direct,
            legacy,
            budget,
            entries,
            FixtureControl::default(),
        )
        .await
    }

    async fn start_controlled(
        coalesce: Option<u64>,
        direct: bool,
        legacy: bool,
        budget: Duration,
        entries: &[(&str, FollowObservationPhase)],
        controls: FixtureControl,
    ) -> Self {
        let registry = Arc::new(WaitOwnerRegistry::new());
        let lease = if direct {
            registry
                .acquire_direct("ch-1", "org", "usera__userb", None, budget)
                .await
                .unwrap()
        } else {
            registry
                .acquire("ch-1", "org", "ops", None, budget)
                .await
                .unwrap()
        };
        let (session, guard) = lease.into_guard();
        let release = Arc::new(LeaseRelease::new(guard));
        let sidecar = MessageSidecar::new();
        let start = Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new(P0),
        };
        let reg = IdToken::new(format!("reg:{}", session.wait_id));
        let events = entries
            .iter()
            .copied()
            .map(|(id, phase)| {
                let message = Message {
                    id: id.into(),
                    user_id: "alice".into(),
                    username: "alice".into(),
                    message: "owned fixture".into(),
                    create_at: 1,
                    root_id: id.into(),
                    mention_user_ids: None,
                };
                Observation::Event(Box::new(
                    event_from_message_with_phase(
                        &message,
                        &reg,
                        &channel_subject("ch-1"),
                        &start,
                        &sidecar,
                        phase,
                    )
                    .unwrap(),
                ))
            })
            .collect();
        let (ready, bind_ready_rx) = tokio::sync::oneshot::channel();
        let counts = Arc::new(Counts::default());
        let observer = SamePollObserver {
            start,
            ready: Arc::new(Mutex::new(Some(ready))),
            events: Arc::new(Mutex::new(events)),
            counts: Arc::clone(&counts),
        };
        let (stream, records) = tokio::sync::mpsc::channel(1);
        if controls.full_queue {
            let (written, receipt) = tokio::sync::oneshot::channel();
            drop(receipt);
            stream
                .try_send(crate::wait::FollowStreamRecord {
                    method: "owned.capacity.fixture",
                    event: serde_json::json!({"fixture":"occupied"}),
                    written,
                })
                .unwrap();
        }
        let deadline = Instant::now() + budget;
        let client_gone = CancellationToken::new();
        let delivery = ChannelDelivery::new(client_gone.clone());
        delivery.publish_deadline(deadline);
        delivery.publish_session(&session);
        delivery.set_armed_pauses(
            controls.before,
            controls.after_reservation,
            controls.reservation_started,
        );
        let tip = Arc::new(Mutex::new(None));
        let inner_cancel = CancellationToken::new();
        let run = FollowRun {
            release: Arc::clone(&release),
            sidecar: sidecar.clone(),
            last_error: Arc::new(Mutex::new(None)),
            inner_cancel: inner_cancel.clone(),
            tip_state: Arc::clone(&tip),
            bind_ready_rx,
            stream,
            coalesce_ms: coalesce,
            client_gone,
            delivery: (!legacy).then_some(delivery.clone()),
        };
        let owned_session = session.clone();
        let task = tokio::spawn(async move {
            drive_follow(
                &observer,
                FollowWait {
                    channel: if direct { "usera__userb" } else { "ops" },
                    channel_id: "ch-1",
                    after: Some(P0),
                    deadline,
                    session: &owned_session,
                    my_user_id: "bot",
                },
                run,
            )
            .await
        });
        Self {
            registry,
            session,
            release,
            sidecar,
            tip,
            inner_cancel,
            delivery,
            counts,
            receive_no: 0,
            records,
            task,
        }
    }
    async fn recv(&mut self) -> crate::wait::FollowStreamRecord {
        self.recv_guard(Instant::now() + Duration::from_secs(2), "next")
            .await
    }
    async fn recv_deadline_flush(&mut self) -> crate::wait::FollowStreamRecord {
        // Test observation horizon only: the deliberate flush is triggered
        // by the published original deadline. Never passed to production.
        self.recv_guard(
            self.delivery.published_deadline().unwrap() + Duration::from_secs(1),
            "deadline_flush",
        )
        .await
    }
    async fn recv_guard(
        &mut self,
        horizon: Instant,
        phase: &str,
    ) -> crate::wait::FollowStreamRecord {
        self.receive_no += 1;
        let started = Instant::now();
        let record=tokio::time::timeout_at(horizon,self.records.recv()).await.inspect_err(|_| {
            let now=Instant::now();let deadline=self.delivery.published_deadline().unwrap();
            eprintln!("receive expired: case={} phase={} ordinal={} elapsed={:?} deadline_remaining={:?} deadline_elapsed={:?} armed={} stop={:?} producer_finished={} sidecar={} tip={:?} binds={} active={} drops={}",
                std::thread::current().name().unwrap_or("unnamed"),phase,self.receive_no,started.elapsed(),deadline.saturating_duration_since(now),
                now.saturating_duration_since(deadline),self.delivery.armed_committed(),self.delivery.captured_stop_state(),self.task.is_finished(),
                self.sidecar.inner.lock().unwrap().len(),*self.tip.lock().unwrap(),self.counts.binds.load(Ordering::SeqCst),
                self.counts.active.load(Ordering::SeqCst),self.counts.drops.load(Ordering::SeqCst));
        }).unwrap();
        match record {
            Some(record) => record,
            None => panic!(
                "runner closed stream before expected {phase} record: {:?}",
                self.finish().await
            ),
        }
    }
    fn held(&self) {
        assert!(self.registry.snapshot("ch-1").is_some());
        assert!(!self.release.is_released());
    }
    fn clean(&self) {
        assert!(self.registry.snapshot("ch-1").is_none());
        assert!(self.release.is_released());
        assert!(self.inner_cancel.is_cancelled());
        assert_eq!(self.counts.binds.load(Ordering::SeqCst), 1);
        assert_eq!(self.counts.active.load(Ordering::SeqCst), 0);
        assert_eq!(self.counts.drops.load(Ordering::SeqCst), 1);
    }
    async fn finish(&mut self) -> Result<WaitFollowResult, CoreError> {
        tokio::time::timeout(Duration::from_secs(2), &mut self.task)
            .await
            .unwrap()
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_line<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> serde_json::Value {
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    serde_json::from_str(line.trim_end()).unwrap()
}

#[tokio::test]
async fn same_poll_backlog_stays_untouched_until_real_armed_ack_then_data_ack_commits_before_cancel(
) {
    for direct in [false, true] {
        let mut fixture = Fixture::start(None, direct, false, Duration::from_secs(5)).await;
        let armed = fixture.recv().await;
        assert_eq!(
            armed.event["mode"], "armed",
            "same-poll callback overtook Armed"
        );
        assert_eq!(
            fixture.sidecar.inner.lock().unwrap().len(),
            2,
            "sidecar taken before Armed ACK"
        );
        assert_eq!(*fixture.tip.lock().unwrap(), None);
        assert!(fixture.records.try_recv().is_err());
        let (server, client) = UnixStream::pair().unwrap();
        let (read, write) = server.into_split();
        let mut client = BufReader::new(client);
        let armed_ack = Arc::new(tokio::sync::Notify::new());
        fixture.delivery.hold_ack(Arc::clone(&armed_ack));
        let delivery = fixture.delivery.clone();
        let writer = tokio::spawn(async move {
            let mut reader = BufReader::new(read);
            let mut writer = write;
            delivery
                .write_record(&mut writer, &mut reader, &mut String::new(), armed)
                .await
                .unwrap();
            (reader, writer)
        });
        assert_eq!(read_line(&mut client).await["params"]["mode"], "armed");
        assert!(!fixture.delivery.armed_committed());
        assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 2);
        assert_eq!(*fixture.tip.lock().unwrap(), None);
        fixture.held();
        armed_ack.notify_one();
        let (mut reader, mut writer) = writer.await.unwrap();
        let first = fixture.recv().await;
        assert_eq!(first.event["tip"], P1);
        assert_eq!(
            *fixture.tip.lock().unwrap(),
            None,
            "tip advanced on enqueue"
        );
        let data_ack = Arc::new(tokio::sync::Notify::new());
        fixture.delivery.hold_ack(Arc::clone(&data_ack));
        let delivery = fixture.delivery.clone();
        let data_writer = tokio::spawn(async move {
            delivery
                .write_record(&mut writer, &mut reader, &mut String::new(), first)
                .await
                .unwrap();
            (reader, writer)
        });
        assert_eq!(read_line(&mut client).await["params"]["tip"], P1);
        assert_eq!(*fixture.tip.lock().unwrap(), None);
        fixture.held();
        data_ack.notify_one();
        fixture.session.cancel.cancel();
        let (mut reader, mut writer) = data_writer.await.unwrap();
        let terminal = fixture.recv().await;
        assert_eq!(terminal.event["mode"], "canceled");
        assert_eq!(
            *fixture.tip.lock().unwrap(),
            Some(P1.into()),
            "ACK was lost to simultaneous cancellation"
        );
        assert_eq!(
            fixture.sidecar.inner.lock().unwrap().len(),
            1,
            "later entry admitted after cancellation"
        );
        fixture.held();
        fixture
            .delivery
            .write_record(&mut writer, &mut reader, &mut String::new(), terminal)
            .await
            .unwrap();
        assert_eq!(read_line(&mut client).await["params"]["mode"], "canceled");
        assert!(fixture.finish().await.is_err());
        fixture.clean();
        assert!(fixture.records.try_recv().is_err());
    }
}

#[tokio::test]
async fn v2_visible_unacked_armed_never_populates_coalescer_or_flushing_output() {
    let mut fixture = Fixture::start(Some(10_000), false, false, Duration::from_secs(5)).await;
    let armed = fixture.recv().await;
    assert_eq!(armed.event["mode"], "armed");
    let (server, client) = UnixStream::pair().unwrap();
    let (read, mut writer) = server.into_split();
    let mut reader = BufReader::new(read);
    let mut client = BufReader::new(client);
    let ack = Arc::new(tokio::sync::Notify::new());
    fixture.delivery.hold_ack(Arc::clone(&ack));
    let delivery = fixture.delivery.clone();
    let writer_task = tokio::spawn(async move {
        delivery
            .write_record(&mut writer, &mut reader, &mut String::new(), armed)
            .await
    });
    assert_eq!(read_line(&mut client).await["params"]["mode"], "armed");
    assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 2);
    assert!(!fixture.delivery.armed_committed());
    fixture.held();
    fixture.session.cancel.cancel();
    assert!(writer_task.await.unwrap().is_err());
    assert!(fixture.finish().await.is_err());
    fixture.clean();
    assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 2);
    assert_eq!(*fixture.tip.lock().unwrap(), None);
    assert!(fixture.records.try_recv().is_err());
    ack.notify_one();
    assert_eq!(
        fixture.delivery.admit().await,
        Err(crate::follow_delivery::StopCause::Transport)
    );
}

#[tokio::test]
async fn v2_admitted_buffer_flushes_at_deadline_with_real_ready_writer_and_terminal_ack() {
    let mut fixture = Fixture::start(Some(10_000), false, false, Duration::from_secs(2)).await;
    let (server, client) = UnixStream::pair().unwrap();
    let (read, mut writer) = server.into_split();
    let mut reader = BufReader::new(read);
    let mut client = BufReader::new(client);
    let armed = fixture.recv().await;
    assert_eq!(armed.event["mode"], "armed");
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), armed)
        .await
        .unwrap();
    assert_eq!(read_line(&mut client).await["params"]["mode"], "armed");
    let flush = fixture.recv_deadline_flush().await;
    assert_eq!(flush.event["mode"], "backlog");
    assert_eq!(flush.event["messages"].as_array().unwrap().len(), 2);
    assert_eq!(flush.event["tip"], P2);
    assert_eq!(*fixture.tip.lock().unwrap(), None);
    fixture.held();
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), flush)
        .await
        .unwrap();
    assert_eq!(read_line(&mut client).await["params"]["tip"], P2);
    let terminal = fixture.recv().await;
    assert_eq!(terminal.event["mode"], "deadman");
    assert_eq!(*fixture.tip.lock().unwrap(), Some(P2.into()));
    fixture.held();
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), terminal)
        .await
        .unwrap();
    assert_eq!(read_line(&mut client).await["params"]["mode"], "deadman");
    let result = fixture.finish().await.unwrap();
    assert!(
        matches!(result.kind, WaitFollowResultKind::Deadman { tip: Some(ref tip) } if tip == P2)
    );
    fixture.clean();
    fixture
        .delivery
        .write_response(
            &mut writer,
            &mut reader,
            &mut String::new(),
            &chanvoy_core::rpc_result(uuid::Uuid::nil(), serde_json::to_value(result).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(read_line(&mut client).await["result"]["mode"], "deadman");
}

#[tokio::test]
async fn v2_pending_terminal_poison_releases_only_old_generation() {
    let mut fixture = Fixture::start(Some(10_000), false, false, Duration::from_secs(2)).await;
    let (server, client) = UnixStream::pair().unwrap();
    let (read, mut writer) = server.into_split();
    let mut reader = BufReader::new(read);
    let mut client = BufReader::new(client);
    let armed = fixture.recv().await;
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), armed)
        .await
        .unwrap();
    read_line(&mut client).await;
    let flush = fixture.recv_deadline_flush().await;
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), flush)
        .await
        .unwrap();
    read_line(&mut client).await;
    let terminal = fixture.recv().await;
    fixture.held();
    let mut writer = HeldWriter::new(writer, Some(7), false);
    assert!(fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), terminal)
        .await
        .is_err());
    assert!(fixture.finish().await.is_err());
    fixture.clean();
    assert_eq!(*fixture.tip.lock().unwrap(), Some(P2.into()));
    let successor = fixture
        .registry
        .acquire("ch-1", "org", "ops", None, Duration::from_secs(5))
        .await
        .unwrap();
    let id = successor.wait_id.clone();
    fixture.release.release();
    assert_eq!(fixture.registry.snapshot("ch-1").unwrap().wait_id, id);
    drop(successor);
}

#[tokio::test]
async fn actual_replacement_interrupts_pending_writer_and_old_cleanup_cannot_release_successor() {
    let mut fixture = Fixture::start(None, false, false, Duration::from_secs(5)).await;
    let (server, client) = UnixStream::pair().unwrap();
    let (read, mut writer) = server.into_split();
    let mut reader = BufReader::new(read);
    let mut client = BufReader::new(client);
    let armed = fixture.recv().await;
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), armed)
        .await
        .unwrap();
    read_line(&mut client).await;
    let data = fixture.recv().await;
    let mut writer = HeldWriter::new(writer, Some(7), false);
    let hold = Arc::clone(&writer.hold);
    let delivery = fixture.delivery.clone();
    let writing = tokio::spawn(async move {
        delivery
            .write_record(&mut writer, &mut reader, &mut String::new(), data)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), hold.reached.notified())
        .await
        .unwrap();
    fixture.held();
    assert_eq!(*fixture.tip.lock().unwrap(), None);
    let registry = Arc::clone(&fixture.registry);
    let old = fixture.session.wait_id.clone();
    let replacing = tokio::spawn(async move {
        registry
            .acquire("ch-1", "org", "ops", Some(&old), Duration::from_secs(5))
            .await
    });
    assert!(tokio::time::timeout(Duration::from_secs(1), writing)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert!(fixture.finish().await.is_err());
    let successor = replacing.await.unwrap().unwrap();
    assert!(fixture.release.is_released());
    assert_eq!(fixture.counts.active.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.counts.drops.load(Ordering::SeqCst), 1);
    assert_eq!(*fixture.tip.lock().unwrap(), None);
    fixture.release.release();
    assert_eq!(
        fixture.registry.snapshot("ch-1").unwrap().wait_id,
        successor.wait_id
    );
    assert!(fixture.records.try_recv().is_err());
    hold.release();
    drop(successor);
}

#[tokio::test]
async fn dropping_prearmed_producer_closes_gate_and_releases_bind_and_lease() {
    let mut fixture = Fixture::start(None, false, false, Duration::from_secs(5)).await;
    let armed = fixture.recv().await;
    assert_eq!(armed.event["mode"], "armed");
    fixture.held();
    fixture.task.abort();
    assert!((&mut fixture.task)
        .await
        .as_ref()
        .unwrap_err()
        .is_cancelled());
    drop(armed);
    fixture.clean();
    assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 2);
    assert_eq!(*fixture.tip.lock().unwrap(), None);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), fixture.delivery.admit())
            .await
            .unwrap(),
        Err(crate::follow_delivery::StopCause::Transport)
    );
}

#[tokio::test]
async fn dedicated_dm_legacy_context_retains_its_existing_same_poll_path() {
    let mut fixture = Fixture::start(None, true, true, Duration::from_secs(5)).await;
    let record = fixture.recv().await;
    assert_eq!(
        record.event["mode"], "backlog",
        "legacy DM unexpectedly opted into channel gate"
    );
    assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 1);
    assert!(!fixture.delivery.armed_committed());
    fixture.task.abort();
    assert!((&mut fixture.task)
        .await
        .as_ref()
        .unwrap_err()
        .is_cancelled());
    drop(record);
    fixture.clean();
}

#[tokio::test]
async fn production_multientry_callback_rechecks_admission_after_first_actual_ack() {
    let registry = Arc::new(WaitOwnerRegistry::new());
    let lease = registry
        .acquire("ch-1", "org", "ops", None, Duration::from_secs(5))
        .await
        .unwrap();
    let (session, guard) = lease.into_guard();
    let delivery = ChannelDelivery::new(CancellationToken::new());
    delivery.publish_deadline(Instant::now() + Duration::from_secs(5));
    delivery.publish_session(&session);
    let sidecar = MessageSidecar::new();
    let reg = IdToken::new(format!("reg:{}", session.wait_id));
    let start = Anchor {
        kind: AnchorKind::ProviderOpaque,
        value: IdToken::new(P0),
    };
    let events = [P1, P2]
        .into_iter()
        .map(|id| {
            let message = Message {
                id: id.into(),
                user_id: "alice".into(),
                username: "alice".into(),
                message: "owned callback".into(),
                create_at: 1,
                root_id: id.into(),
                mention_user_ids: None,
            };
            event_from_message_with_phase(
                &message,
                &reg,
                &channel_subject("ch-1"),
                &start,
                &sidecar,
                FollowObservationPhase::Backlog,
            )
            .unwrap()
        })
        .collect();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let tip = Arc::new(Mutex::new(None));
    let sink = FollowSink {
        stream: tx.clone(),
        sidecar: sidecar.clone(),
        last_error: Arc::new(Mutex::new(None)),
        tip_state: Arc::clone(&tip),
        channel: "ops".into(),
        wait_id: session.wait_id.clone(),
        coalesce: None,
        deadline_tx: tokio::sync::watch::channel(None).0,
        sink_failed: Arc::new(AtomicBool::new(false)),
        delivery: Some(delivery.clone()),
    };
    let control = delivery.clone();
    let wait_id = session.wait_id.clone();
    let consume = tokio::spawn(async move {
        emit_armed(&tx, false, wait_id, None).await.unwrap();
        control.open_gate();
        sink.consume(waitprims_async::FollowBurst { events }).await
    });
    let (server, client) = UnixStream::pair().unwrap();
    let (read, mut writer) = server.into_split();
    let mut reader = BufReader::new(read);
    let mut client = BufReader::new(client);
    let armed = rx.recv().await.unwrap();
    delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), armed)
        .await
        .unwrap();
    read_line(&mut client).await;
    let first = rx.recv().await.unwrap();
    assert_eq!(first.event["tip"], P1);
    assert_eq!(*tip.lock().unwrap(), None);
    let ack = Arc::new(tokio::sync::Notify::new());
    delivery.hold_ack(Arc::clone(&ack));
    let control = delivery.clone();
    let writing = tokio::spawn(async move {
        control
            .write_record(&mut writer, &mut reader, &mut String::new(), first)
            .await
    });
    read_line(&mut client).await;
    ack.notify_one();
    session.cancel.cancel();
    assert!(writing.await.unwrap().is_ok());
    assert!(tokio::time::timeout(Duration::from_secs(1), consume)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert_eq!(*tip.lock().unwrap(), Some(P1.into()));
    assert_eq!(sidecar.inner.lock().unwrap().len(), 1);
    assert!(rx.try_recv().is_err());
    drop(guard);
    assert!(registry.snapshot("ch-1").is_none());
}

#[tokio::test]
async fn v2_phase_change_freezes_only_buffered_entries_after_actual_ack_cancel_race() {
    let p3 = "postid00000000000000000003";
    let mut fixture = Fixture::start_entries(
        Some(10_000),
        false,
        false,
        Duration::from_secs(5),
        &[
            (P1, FollowObservationPhase::Backlog),
            (P2, FollowObservationPhase::Live),
            (p3, FollowObservationPhase::Live),
        ],
    )
    .await;
    let (server, client) = UnixStream::pair().unwrap();
    let (read, mut writer) = server.into_split();
    let mut reader = BufReader::new(read);
    let mut client = BufReader::new(client);
    let armed = fixture.recv().await;
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), armed)
        .await
        .unwrap();
    read_line(&mut client).await;
    let old_phase = fixture.recv().await;
    assert_eq!(old_phase.event["mode"], "backlog");
    assert_eq!(old_phase.event["tip"], P1);
    assert_eq!(
        fixture.sidecar.inner.lock().unwrap().len(),
        1,
        "phase-change buffer admitted later entry before old flush ACK"
    );
    assert_eq!(*fixture.tip.lock().unwrap(), None);
    fixture.held();
    let ack = Arc::new(tokio::sync::Notify::new());
    fixture.delivery.hold_ack(Arc::clone(&ack));
    let delivery = fixture.delivery.clone();
    let writing = tokio::spawn(async move {
        delivery
            .write_record(&mut writer, &mut reader, &mut String::new(), old_phase)
            .await
            .unwrap();
        (reader, writer)
    });
    read_line(&mut client).await;
    ack.notify_one();
    fixture.session.cancel.cancel();
    let (mut reader, mut writer) = writing.await.unwrap();
    let admitted = fixture.recv().await;
    assert_eq!(admitted.event["mode"], "live");
    assert_eq!(admitted.event["messages"].as_array().unwrap().len(), 1);
    assert_eq!(admitted.event["tip"], P2);
    assert_eq!(*fixture.tip.lock().unwrap(), Some(P1.into()));
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), admitted)
        .await
        .unwrap();
    read_line(&mut client).await;
    let terminal = fixture.recv().await;
    assert_eq!(terminal.event["mode"], "canceled");
    assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 1);
    assert_eq!(*fixture.tip.lock().unwrap(), Some(P2.into()));
    fixture.held();
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), terminal)
        .await
        .unwrap();
    read_line(&mut client).await;
    assert!(fixture.finish().await.is_err());
    fixture.clean();
}

#[tokio::test]
async fn expired_before_armed_never_fabricates_armed_or_deadman() {
    let mut fixture = Fixture::start(Some(10_000), false, false, Duration::from_nanos(1)).await;
    assert!(fixture.finish().await.is_err());
    fixture.clean();
    assert!(!fixture.delivery.armed_committed());
    assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 2);
    assert_eq!(*fixture.tip.lock().unwrap(), None);
    assert!(fixture.records.try_recv().is_err());
}

#[tokio::test]
async fn actual_ready_eof_and_data_ack_commits_tip_before_freezing_next_entry() {
    use tokio::io::AsyncWriteExt;
    let mut fixture = Fixture::start(None, false, false, Duration::from_secs(5)).await;
    let (server, client) = UnixStream::pair().unwrap();
    let (read, mut writer) = server.into_split();
    let mut reader = BufReader::new(read);
    let mut client = BufReader::new(client);
    let armed = fixture.recv().await;
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), armed)
        .await
        .unwrap();
    read_line(&mut client).await;
    let data = fixture.recv().await;
    assert_eq!(*fixture.tip.lock().unwrap(), None);
    client.get_mut().shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), reader.get_ref().as_ref().readable())
        .await
        .unwrap()
        .unwrap();
    assert!(
        fixture.delivery.cause().is_none(),
        "kernel EOF is not yet an observed operation cause"
    );
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), data)
        .await
        .unwrap();
    assert_eq!(read_line(&mut client).await["params"]["tip"], P1);
    let terminal = fixture.recv().await;
    assert_eq!(terminal.event["mode"], "canceled");
    assert_eq!(*fixture.tip.lock().unwrap(), Some(P1.into()));
    assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 1);
    fixture.held();
    fixture
        .delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), terminal)
        .await
        .unwrap();
    read_line(&mut client).await;
    assert!(fixture.finish().await.is_err());
    fixture.clean();
}

async fn serve_prearmed(fixture: &mut Fixture) -> (serde_json::Value, Option<(String, String)>) {
    use tokio::io::AsyncReadExt;
    let (server, mut client) = UnixStream::pair().unwrap();
    let (read, mut writer) = server.into_split();
    writer.as_ref().writable().await.unwrap();
    let mut reader = BufReader::new(read);
    let (_, empty) = tokio::sync::mpsc::channel(1);
    let records = std::mem::replace(&mut fixture.records, empty);
    let delivery = fixture.delivery.clone();
    let evidence = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&evidence);
    let follow = async {
        let outcome = (&mut fixture.task).await.unwrap();
        *captured.lock().unwrap() = Some(match &outcome {
            Err(CoreError::WaitReplaced {
                wait_id,
                replaced_by_wait_id,
            }) => (wait_id.clone(), replaced_by_wait_id.clone()),
            Err(CoreError::WaitProviderDegraded { .. }) => ("refused".into(), String::new()),
            other => panic!("pre-Armed path must be non-success: {other:?}"),
        });
        outcome
    };
    tokio::time::timeout(
        Duration::from_secs(2),
        delivery.serve(&mut writer, &mut reader, uuid::Uuid::nil(), records, follow),
    )
    .await
    .unwrap()
    .unwrap();
    drop(writer);
    let mut bytes = String::new();
    client.read_to_string(&mut bytes).await.unwrap();
    let lines: Vec<_> = bytes.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "stopped producer emitted a new Armed/terminal: {bytes}"
    );
    let response: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert!(response["error"].is_object());
    assert!(response.get("method").is_none());
    assert!(response.get("result").is_none());
    assert!(!fixture.delivery.armed_committed());
    assert_eq!(
        fixture.delivery.framed_calls(),
        1,
        "only final non-success response may be written"
    );
    let captured = evidence.lock().unwrap().clone();
    (response, captured)
}

#[tokio::test]
async fn actual_postbind_stop_never_enqueues_or_acks_armed_and_preserves_replacement() {
    use crate::follow_delivery::StopCause;
    for v2 in [false, true] {
        for cause in [
            StopCause::Deadline,
            StopCause::Canceled,
            StopCause::Replaced,
        ] {
            let pause = Arc::new(ArmPause::default());
            let mut fixture = Fixture::start_controlled(
                v2.then_some(10_000),
                false,
                false,
                if cause == StopCause::Deadline {
                    Duration::from_secs(2)
                } else {
                    Duration::from_secs(5)
                },
                &[
                    (P1, FollowObservationPhase::Backlog),
                    (P2, FollowObservationPhase::Backlog),
                ],
                FixtureControl {
                    before: Some(Arc::clone(&pause)),
                    ..Default::default()
                },
            )
            .await;
            tokio::time::timeout(Duration::from_secs(2), pause.entered.notified())
                .await
                .unwrap();
            fixture.held();
            assert_eq!(fixture.counts.binds.load(Ordering::SeqCst), 1);
            assert!(fixture.delivery.cause().is_none());
            assert!(!fixture.delivery.armed_committed());
            assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 2);
            assert_eq!(*fixture.tip.lock().unwrap(), None);
            assert!(fixture.records.try_recv().is_err());
            let mut replacing = None;
            match cause {
                StopCause::Deadline => {
                    tokio::time::sleep_until(fixture.delivery.published_deadline().unwrap()).await
                }
                StopCause::Canceled => fixture.session.cancel.cancel(),
                StopCause::Replaced => {
                    let registry = Arc::clone(&fixture.registry);
                    let old = fixture.session.wait_id.clone();
                    replacing = Some(tokio::spawn(async move {
                        registry
                            .acquire("ch-1", "org", "ops", Some(&old), Duration::from_secs(5))
                            .await
                    }));
                    tokio::time::timeout(
                        Duration::from_secs(1),
                        fixture.session.cancel.cancelled(),
                    )
                    .await
                    .unwrap();
                }
                StopCause::Transport => unreachable!(),
            }
            assert_eq!(fixture.delivery.cause(), Some(cause));
            pause.release.notify_one();
            let (_, evidence) = serve_prearmed(&mut fixture).await;
            assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 2);
            assert_eq!(*fixture.tip.lock().unwrap(), None);
            if let Some(replacing) = replacing {
                let successor = replacing.await.unwrap().unwrap();
                assert_eq!(
                    evidence.unwrap(),
                    (fixture.session.wait_id.clone(), successor.wait_id.clone())
                );
                fixture.release.release();
                assert_eq!(
                    fixture.registry.snapshot("ch-1").unwrap().wait_id,
                    successor.wait_id
                );
                let (_, successor_guard) = successor.into_guard();
                drop(successor_guard);
            }
            fixture.clean();
        }
    }
}

#[tokio::test]
async fn actual_capacity_and_stop_ready_rechecks_after_reservation_before_armed_send() {
    for after_reservation in [false, true] {
        let started = Arc::new(tokio::sync::Notify::new());
        let pause = Arc::new(ArmPause::default());
        let mut fixture = Fixture::start_controlled(
            Some(10_000),
            false,
            false,
            Duration::from_secs(5),
            &[
                (P1, FollowObservationPhase::Backlog),
                (P2, FollowObservationPhase::Backlog),
            ],
            FixtureControl {
                full_queue: !after_reservation,
                reservation_started: Some(Arc::clone(&started)),
                after_reservation: after_reservation.then_some(Arc::clone(&pause)),
                ..Default::default()
            },
        )
        .await;
        if after_reservation {
            tokio::time::timeout(Duration::from_secs(2), pause.entered.notified())
                .await
                .unwrap();
        } else {
            tokio::time::timeout(Duration::from_secs(2), started.notified())
                .await
                .unwrap();
        }
        assert!(fixture.delivery.cause().is_none());
        fixture.held();
        assert!(!fixture.delivery.armed_committed());
        // No yield: in the full-queue case, reserve and stop are both ready
        // at the next producer poll. The permit-biased branch alone fails.
        fixture.session.cancel.cancel();
        if after_reservation {
            pause.release.notify_one();
        } else {
            let occupied = fixture.records.try_recv().unwrap();
            assert_eq!(occupied.method, "owned.capacity.fixture");
            drop(occupied);
        }
        serve_prearmed(&mut fixture).await;
        assert_eq!(fixture.sidecar.inner.lock().unwrap().len(), 2);
        assert_eq!(*fixture.tip.lock().unwrap(), None);
        fixture.clean();
    }
}
