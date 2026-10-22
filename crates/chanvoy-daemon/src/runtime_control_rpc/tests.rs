use super::*;
use crate::retirement_control::tests::{request, setup};
use crate::retirement_control::Outcome;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{oneshot, Mutex};

const ID: &str = "87654321-4321-4321-8321-cba987654321";

fn result(document: &ControlDocument) -> &Value {
    &document.value()["result"]
}

/// A daemon's shutdown signal for projection tests: whether the receiver is
/// alive decides whether a send can succeed, and the latch is shared.
struct Signal {
    sender: Mutex<Option<oneshot::Sender<()>>>,
    started: std::sync::atomic::AtomicBool,
    receiver: std::sync::Mutex<Option<oneshot::Receiver<()>>>,
}
impl Signal {
    fn new(deliverable: bool) -> Self {
        let (tx, rx) = oneshot::channel();
        Self {
            sender: Mutex::new(Some(tx)),
            started: std::sync::atomic::AtomicBool::new(false),
            receiver: std::sync::Mutex::new(deliverable.then_some(rx)),
        }
    }
    fn starter(&self) -> ShutdownStarter<'_> {
        ShutdownStarter {
            sender: &self.sender,
            started: &self.started,
        }
    }
    fn sent(&self) -> bool {
        self.receiver
            .lock()
            .unwrap()
            .as_mut()
            .is_some_and(|rx| rx.try_recv().is_ok())
    }
}

/// Project with a fresh signal; returns the document and whether a shutdown
/// signal was actually sent.
async fn project(
    facts: &ResultFacts,
    request: &ControlDocument,
    deliverable: bool,
) -> (Result<ControlDocument, ControlError>, usize) {
    let signal = Signal::new(deliverable);
    let projected = retire(request, facts, &signal.starter()).await;
    (projected, usize::from(signal.sent()))
}

#[tokio::test]
async fn every_coordinator_outcome_projects_to_its_contract_branch() {
    // Busy, then Committed, AlreadyRetiring and Continued from the real
    // coordinator against the shared arena.
    let (c, p) = setup();
    let permit = p.registry.request_permit().unwrap();
    let begin = request(&p, "begin", |_| {});
    let busy = c.attempt(&begin, |_| Ok(())).unwrap();
    assert_eq!(busy.outcome, Outcome::Busy);
    let (doc, calls) = project(&busy, &begin, true).await;
    let doc = doc.unwrap();
    assert_eq!(calls, 0, "Busy never starts shutdown");
    assert_eq!(result(&doc)["outcome"], "busy");
    assert_eq!(result(&doc)["counts"]["requests"], 1);
    assert_eq!(result(&doc)["facts"]["shutdown_started"], false);
    permit.confirm_done().unwrap();

    let begin = request(&p, "begin", |_| {});
    let committed = c.attempt(&begin, |_| Ok(())).unwrap();
    assert_eq!(committed.outcome, Outcome::Committed);
    let (doc, calls) = project(&committed, &begin, true).await;
    let doc = doc.unwrap();
    assert_eq!(calls, 1);
    assert_eq!(result(&doc)["outcome"], "committed");
    assert_eq!(result(&doc)["request_committed"], true);
    assert_eq!(result(&doc)["facts"]["durable_revision"], 3);
    assert_eq!(result(&doc)["facts"]["shutdown_started"], true);
    // A shutdown that could not start is reported, not assumed.
    let (doc, _) = project(&committed, &begin, false).await;
    assert_eq!(result(&doc.unwrap())["facts"]["shutdown_started"], false);

    // shutdown_started describes the target: after one request starts it,
    // later refusals and continuations on the same daemon report it.
    let shared = Signal::new(true);
    let doc = retire(&begin, &committed, &shared.starter()).await.unwrap();
    assert_eq!(result(&doc)["facts"]["shutdown_started"], true);
    assert!(shared.sent());

    let again = request(&p, "begin", |_| {});
    let already = c.attempt(&again, |_| Ok(())).unwrap();
    assert_eq!(already.outcome, Outcome::AlreadyRetiring);
    let doc = retire(&again, &already, &shared.starter()).await.unwrap();
    assert_eq!(result(&doc)["facts"]["shutdown_started"], true);
    let (doc, calls) = project(&already, &again, true).await;
    let doc = doc.unwrap();
    assert_eq!(calls, 0);
    assert_eq!(result(&doc)["facts"]["shutdown_started"], false);
    assert_eq!(
        (
            result(&doc)["outcome"].as_str(),
            result(&doc)["reason"].as_str()
        ),
        (Some("refused"), Some("already_retiring"))
    );
    assert_eq!(result(&doc)["facts"]["memory_retiring"], true);

    let cont = request(&p, "continue_same_epoch", |_| {});
    let continued = c.attempt(&cont, |_| Ok(())).unwrap();
    assert_eq!(continued.outcome, Outcome::Continued);
    let (doc, calls) = project(&continued, &cont, true).await;
    let doc = doc.unwrap();
    assert_eq!(calls, 1);
    assert_eq!(result(&doc)["outcome"], "continued");
    assert_eq!(result(&doc)["request_committed"], false);
    // A repeated continuation after the signal was already sent still
    // reports the target's started shutdown, without sending again.
    let doc = retire(&cont, &continued, &shared.starter()).await.unwrap();
    assert_eq!(result(&doc)["facts"]["shutdown_started"], true);
}

#[tokio::test]
async fn refusals_keep_known_facts_and_unknown_targets_stay_null() {
    let (c, p) = setup();
    let stale = request(&p, "begin", |v| {
        v["params"]["target"]["record_revision"] = json!(1)
    });
    let facts = c.attempt(&stale, |_| Ok(())).unwrap();
    assert_eq!(facts.outcome, Outcome::RevisionChanged);
    let doc = project(&facts, &stale, true).await.0.unwrap();
    assert_eq!(result(&doc)["reason"], "revision_changed");
    assert_eq!(result(&doc)["facts"]["memory_retiring"], false);
    assert_eq!(result(&doc)["facts"]["durable_revision"], 2);

    let generation = request(&p, "begin", |v| {
        v["params"]["caller_generation"]["commit"] = json!("b".repeat(40))
    });
    let facts = c.attempt(&generation, |_| Ok(())).unwrap();
    assert_eq!(facts.outcome, Outcome::GenerationRefused);
    let doc = project(&facts, &generation, true).await.0.unwrap();
    assert_eq!(result(&doc)["reason"], "generation_refused");

    let not_retiring = request(&p, "continue_same_epoch", |_| {});
    let facts = c.attempt(&not_retiring, |_| Ok(())).unwrap();
    assert_eq!(facts.outcome, Outcome::NotRetiring);
    let doc = project(&facts, &not_retiring, true).await.0.unwrap();
    assert_eq!(result(&doc)["reason"], "revision_changed");
    assert_eq!(result(&doc)["facts"]["memory_retiring"], false);

    let native = request(&p, "begin", |_| {});
    let facts = c
        .attempt(&native, |_| Err(RuntimeError::Unavailable("synthetic")))
        .unwrap();
    assert_eq!(facts.outcome, Outcome::NativeUnavailable);
    let (doc, calls) = project(&facts, &native, true).await;
    let doc = doc.unwrap();
    assert_eq!(calls, 0);
    assert_eq!(result(&doc)["reason"], "native_evidence_unavailable");
    for fact in ["memory_retiring", "durable_retiring", "shutdown_started"] {
        assert!(result(&doc)["facts"][fact].is_null(), "{fact}");
    }
    assert!(result(&doc)["counts"]["requests"].is_null());

    let mismatch = request(&p, "begin", |v| {
        v["params"]["target"]["instance_uuid"] = json!(ID)
    });
    let facts = c.attempt(&mismatch, |_| Ok(())).unwrap();
    assert_eq!(facts.outcome, Outcome::TargetMismatch);
    let doc = project(&facts, &mismatch, true).await.0.unwrap();
    assert_eq!(result(&doc)["reason"], "target_mismatch");
}

use chanvoy_core::runtime::RuntimeError;

#[tokio::test]
async fn publication_failure_is_unconfirmed_without_shutdown() {
    for mode in 1..=3 {
        let (c, p) = setup();
        p.failure.store(mode, Ordering::SeqCst);
        let begin = request(&p, "begin", |_| {});
        let facts = c.attempt(&begin, |_| Ok(())).unwrap();
        assert_eq!(facts.outcome, Outcome::PublicationUnconfirmed, "{mode}");
        let (doc, calls) = project(&facts, &begin, true).await;
        let doc = doc.unwrap();
        assert_eq!(calls, 0, "{mode}");
        assert_eq!(result(&doc)["outcome"], "unconfirmed");
        assert_eq!(result(&doc)["request_committed"], true);
        assert_eq!(result(&doc)["facts"]["memory_retiring"], true);
        assert_eq!(result(&doc)["facts"]["shutdown_started"], false);
        assert_ne!(result(&doc)["facts"]["durable_retiring"], true);

        // Forward retry on the same epoch commits nothing new.
        let cont = request(&p, "continue_same_epoch", |_| {});
        let retried = c.attempt(&cont, |_| Ok(())).unwrap();
        assert_eq!(retried.outcome, Outcome::PublicationUnconfirmed);
        let doc = project(&retried, &cont, true).await.0.unwrap();
        assert_eq!(result(&doc)["request_committed"], false);
    }
}

#[tokio::test]
async fn unrepresentable_facts_are_refused_before_shutdown() {
    let (_, p) = setup();
    let begin = request(&p, "begin", |_| {});
    // Committed without arena facts cannot be reported truthfully.
    let facts = ResultFacts {
        outcome: Outcome::Committed,
        arena: None,
        publication: None,
    };
    let (projected, calls) = project(&facts, &begin, true).await;
    assert!(projected.is_err());
    assert_eq!(calls, 0);
}

// --- the daemon socket loop -------------------------------------------------

async fn exchange(
    frames: &[Vec<u8>],
) -> (
    Vec<Value>,
    Arc<crate::AppState>,
    oneshot::Receiver<()>,
    Result<(), crate::DaemonError>,
) {
    let state = crate::request_frontier::tests::state("http://127.0.0.1:9");
    let (reader, client) = tokio::io::duplex(64 * 1024);
    let (server_read, server_write) = tokio::io::split(reader);
    let (shutdown, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(crate::handle_client_io(
        BufReader::new(server_read),
        server_write,
        Arc::clone(&state),
        Arc::new(Mutex::new(Some(shutdown))),
    ));
    let (read, mut write) = tokio::io::split(client);
    // Write concurrently with reading so a server that stops reading cannot
    // block the client; a stalled write simply ends with the exchange.
    let input: Vec<u8> = frames
        .iter()
        .flat_map(|frame| frame.iter().copied().chain(std::iter::once(b'\n')))
        .collect();
    let writer = tokio::spawn(async move {
        let _ = write.write_all(&input).await;
        write
    });
    let mut lines = BufReader::new(read).lines();
    let mut replies = Vec::new();
    while replies.len() < frames.len() {
        match tokio::time::timeout(Duration::from_secs(5), lines.next_line()).await {
            Ok(Ok(Some(line))) => replies.push(serde_json::from_str(&line).unwrap()),
            _ => break,
        }
    }
    drop(lines);
    writer.abort();
    let _ = writer.await;
    let outcome = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("client loop exits")
        .unwrap();
    (replies, state, shutdown_rx, outcome)
}

fn fixture(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "../../schemas/runtime/control/v1/fixtures/chanvoy-runtime-control-v1/{name}.json"
    ));
    serde_json::to_vec(&serde_json::from_slice::<Value>(&std::fs::read(path).unwrap()).unwrap())
        .unwrap()
}

#[tokio::test]
async fn routed_methods_answer_strict_roots_without_an_operational_permit() {
    let state_probe = crate::request_frontier::tests::state("http://127.0.0.1:9");
    // An operational request in flight must not make control Busy-refused.
    let permit = state_probe.wait_owners.request_permit().unwrap();
    drop(permit);

    let (replies, state, mut shutdown_rx, outcome) = exchange(&[
        fixture("retire_rpc_request-positive"),
        fixture("observation_rpc_request-positive"),
    ])
    .await;
    outcome.unwrap();
    assert_eq!(replies.len(), 2);
    let retire = ControlDocument::decode(
        ControlRoot::RetireResponse,
        &serde_json::to_vec(&replies[0]).unwrap(),
    )
    .expect("retire reply matches its root");
    assert_eq!(retire.value()["id"], ID);
    assert_eq!(result(&retire)["outcome"], "refused");
    assert_eq!(result(&retire)["reason"], "native_evidence_unavailable");
    let observation = ControlDocument::decode(
        ControlRoot::ObservationResponse,
        &serde_json::to_vec(&replies[1]).unwrap(),
    )
    .expect("observation reply matches its root");
    assert_eq!(result(&observation)["source"], "daemon_application");
    assert_eq!(result(&observation)["phase"], "running");
    assert_eq!(result(&observation)["counts"]["requests"], 0);
    assert_eq!(
        result(&observation)["unavailable_reason"],
        "native_identity_unknown"
    );
    // Control took no permit and left nothing owned; nothing shut down.
    let snapshot = state.wait_owners.admission_snapshot();
    assert_eq!((snapshot.requests, snapshot.streams), (0, 0));
    assert!(shutdown_rx.try_recv().is_err());
}

#[tokio::test]
async fn control_is_answered_while_operational_work_is_in_flight() {
    let state = crate::request_frontier::tests::state("http://127.0.0.1:9");
    let held = state.wait_owners.request_permit().unwrap();
    let (reader, mut client) = tokio::io::duplex(64 * 1024);
    let (server_read, server_write) = tokio::io::split(reader);
    let (shutdown, _rx) = oneshot::channel();
    let task = tokio::spawn(crate::handle_client_io(
        BufReader::new(server_read),
        server_write,
        Arc::clone(&state),
        Arc::new(Mutex::new(Some(shutdown))),
    ));
    client
        .write_all(&[fixture("observation_rpc_request-positive"), b"\n".to_vec()].concat())
        .await
        .unwrap();
    let (read, write) = tokio::io::split(client);
    let line = tokio::time::timeout(
        Duration::from_secs(5),
        BufReader::new(read).lines().next_line(),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let reply: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(reply["result"]["counts"]["requests"], 1);
    drop(write);
    held.confirm_done().unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn invalid_and_oversized_control_frames_never_reach_legacy_handlers() {
    // Contract-invalid but well-formed: refused, connection stays usable.
    let (replies, _, mut shutdown_rx, outcome) = exchange(&[
        fixture("retire_rpc_request-unknown-root-field"),
        fixture("observation_rpc_request-positive"),
    ])
    .await;
    outcome.unwrap();
    assert_eq!(replies.len(), 2);
    ControlDocument::decode(
        ControlRoot::RpcErrorResponse,
        &serde_json::to_vec(&replies[0]).unwrap(),
    )
    .unwrap();
    assert_eq!(replies[0]["error"]["code"], -32602);
    assert!(replies[0]["id"].is_null());
    assert_eq!(replies[1]["result"]["kind"], "lifecycle_observation");
    assert!(shutdown_rx.try_recv().is_err());

    // Duplicate keys are refused even though serde would accept them.
    let duplicate = br#"{"jsonrpc":"2.0","id":"87654321-4321-4321-8321-cba987654321","method":"runtime_observation_v1","method":"runtime_observation_v1","params":{"schema_version":1,"kind":"observation_params","namespace":"f691215e0d009bcb711257dd344ca628c882a411938ad516b9cc999981ee4acd","expected_instance_uuid":null}}"#;
    let (replies, _, _, outcome) = exchange(&[duplicate.to_vec()]).await;
    outcome.unwrap();
    assert_eq!(replies[0]["error"]["code"], -32600);
    // A legacy method shadowed by a control method is still strict-routed and
    // refused; it never reaches the legacy shutdown handler.
    let shadowed = br#"{"jsonrpc":"2.0","id":"87654321-4321-4321-8321-cba987654321","method":"shutdown","method":"runtime_retire_v1","params":{}}"#;
    let (replies, _, mut shutdown_rx, outcome) = exchange(&[shadowed.to_vec()]).await;
    outcome.unwrap();
    assert_eq!(replies[0]["error"]["code"], -32600);
    assert!(
        shutdown_rx.try_recv().is_err(),
        "legacy shutdown was not taken"
    );

    // Over the frame cap: refused and the exchange closes.
    let mut big: Value = serde_json::from_slice(&fixture("retire_rpc_request-positive")).unwrap();
    big["params"]["target"]["process"]["executable"] = json!("61".repeat(9_000));
    let big = serde_json::to_vec(&big).unwrap();
    assert!(big.len() > chanvoy_core::runtime::control::RPC_MAX_BYTES);
    let (replies, _, mut shutdown_rx, outcome) =
        exchange(&[big, fixture("observation_rpc_request-positive")]).await;
    outcome.unwrap();
    assert_eq!(
        replies.len(),
        1,
        "connection closes after a framing refusal"
    );
    assert_eq!(replies[0]["error"]["code"], -32600);
    assert!(shutdown_rx.try_recv().is_err());
}

#[tokio::test]
async fn legacy_methods_keep_their_existing_path() {
    let request = chanvoy_core::rpc_request("profile_status", json!({}));
    let (replies, _, _, outcome) = exchange(&[serde_json::to_vec(&request).unwrap()]).await;
    outcome.unwrap();
    assert_eq!(replies[0]["id"], json!(request.id));
    assert!(replies[0].get("result").is_some(), "{}", replies[0]);
}

#[tokio::test]
async fn any_frame_naming_a_control_method_is_strict_refused_never_legacy() {
    let positive = fixture("observation_rpc_request-positive");
    let mut trailing = positive.clone();
    trailing.extend_from_slice(b" {}");
    let mut bom = b"\xef\xbb\xbf".to_vec();
    bom.extend_from_slice(&positive);
    let array =
        br#"["2.0","87654321-4321-4321-8321-cba987654321","runtime_retire_v1",{}]"#.to_vec();
    let escaped = String::from_utf8(positive.clone())
        .unwrap()
        .replace("runtime_observation_v1", "runtime_\\u006fbservation_v1")
        .into_bytes();
    let shutdown_array =
        br#"["2.0","87654321-4321-4321-8321-cba987654321","shutdown",{},"runtime_retire_v1"]"#
            .to_vec();
    for (name, frame) in [
        ("trailing", trailing),
        ("bom", bom),
        ("array", array),
        ("shutdown-array", shutdown_array),
    ] {
        let (replies, state, mut shutdown_rx, outcome) = exchange(&[frame]).await;
        outcome.unwrap();
        assert_eq!(replies.len(), 1, "{name}: one strict refusal");
        ControlDocument::decode(
            ControlRoot::RpcErrorResponse,
            &serde_json::to_vec(&replies[0]).unwrap(),
        )
        .unwrap_or_else(|e| panic!("{name}: {e:?} {}", replies[0]));
        assert!(shutdown_rx.try_recv().is_err(), "{name}: no shutdown");
        let snapshot = state.wait_owners.admission_snapshot();
        assert_eq!(snapshot.requests, 0, "{name}: no operational permit");
    }
    // An escaped method name is the same JSON string: it is strict-routed and,
    // being exactly the selected root, answered by the control handler.
    let (replies, _, _, outcome) = exchange(&[escaped]).await;
    outcome.unwrap();
    assert_eq!(replies[0]["result"]["kind"], "lifecycle_observation");
}

#[tokio::test]
async fn a_control_frame_without_its_delimiter_is_truncated() {
    let state = crate::request_frontier::tests::state("http://127.0.0.1:9");
    let (reader, client) = tokio::io::duplex(64 * 1024);
    let (server_read, server_write) = tokio::io::split(reader);
    let (shutdown, _rx) = oneshot::channel();
    let task = tokio::spawn(crate::handle_client_io(
        BufReader::new(server_read),
        server_write,
        Arc::clone(&state),
        Arc::new(Mutex::new(Some(shutdown))),
    ));
    let (read, mut write) = tokio::io::split(client);
    write
        .write_all(&fixture("observation_rpc_request-positive"))
        .await
        .unwrap();
    write.shutdown().await.unwrap();
    let line = tokio::time::timeout(
        Duration::from_secs(5),
        BufReader::new(read).lines().next_line(),
    )
    .await
    .unwrap()
    .unwrap()
    .expect("a protocol error, not silence");
    let reply: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(reply["error"]["code"], -32600, "{reply}");
    drop(write);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn an_unread_control_client_does_not_hold_up_shutdown() {
    let state = crate::request_frontier::tests::state("http://127.0.0.1:9");
    // A tiny transport buffer so replies back up when the client never reads.
    let (reader, client) = tokio::io::duplex(256);
    let (server_read, server_write) = tokio::io::split(reader);
    let (shutdown, _rx) = oneshot::channel();
    let stop = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(crate::handle_client_io_with_shutdown(
        BufReader::new(server_read),
        server_write,
        Arc::clone(&state),
        Arc::new(Mutex::new(Some(shutdown))),
        stop.clone(),
    ));
    let (_read, mut write) = tokio::io::split(client);
    let mut frame = fixture("observation_rpc_request-positive");
    frame.push(b'\n');
    // Queue requests without reading any reply.
    let writer = tokio::spawn(async move {
        for _ in 0..64 {
            if write.write_all(&frame).await.is_err() {
                break;
            }
        }
        write
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("client loop exits on stop while a control reply is blocked")
        .unwrap()
        .unwrap();
    writer.abort();
}

#[test]
fn only_method_position_selects_a_control_root() {
    let legacy = |params: Value| {
        serde_json::to_vec(&json!({
            "jsonrpc": "2.0", "id": ID, "method": "post_message", "params": params,
        }))
        .unwrap()
    };
    // Payload text that mentions a control method keeps its legacy handler.
    for params in [
        json!({"channel": "x", "message": "Review runtime_retire_v1 today"}),
        json!({"query": "runtime_observation_v1"}),
        json!({"contains": "runtime_retire_v10", "method": "runtime_retire_v1"}),
        json!({"text": "runtime_\\u0072etire_v1"}),
    ] {
        assert_eq!(frame_root(&legacy(params.clone())), None, "{params}");
    }
    // Method position selects, whatever the spelling or shape.
    assert_eq!(
        frame_root(br#"{"\u006dethod":"runtime_\u0072etire_v1","id":"x"}"#),
        Some(ControlRoot::RetireRequest)
    );
    // Every duplicate counts, not just the one a lenient map keeps.
    for frame in [
        br#"{"method":"shutdown","method":"runtime_observation_v1"}"#.as_slice(),
        br#"{"method":"runtime_observation_v1","method":"shutdown"}"#.as_slice(),
    ] {
        assert_eq!(frame_root(frame), Some(ControlRoot::ObservationRequest));
    }
    assert_eq!(
        frame_root(br#"["2.0","x","runtime_retire_v1",{}]"#),
        Some(ControlRoot::RetireRequest)
    );
    // Not one JSON value: any mention routes to strict refusal.
    assert_eq!(
        frame_root(b"{\"method\":\"runtime_retire_v1\"} trailing"),
        Some(ControlRoot::RetireRequest)
    );
    assert_eq!(frame_root(br#"{"method":"profile_status"}"#), None);
}

#[tokio::test]
async fn chat_text_naming_a_control_method_reaches_its_legacy_handler() {
    let request = chanvoy_core::rpc_request(
        "profile_status",
        json!({"note": "about runtime_retire_v1 and runtime_observation_v1"}),
    );
    let (replies, _, _, outcome) = exchange(&[serde_json::to_vec(&request).unwrap()]).await;
    outcome.unwrap();
    assert_eq!(replies[0]["id"], json!(request.id));
    assert!(replies[0].get("result").is_some(), "{}", replies[0]);
}

// --- bounded ingress ----------------------------------------------------------

/// A reader over fixed bytes that records how much the frame reader consumed.
struct Counted {
    data: Vec<u8>,
    pos: usize,
    /// Never serve more than this per fill, so the cap logic sees partial reads.
    chunk: usize,
    /// Yield between chunks like a socket, so a reader that keeps reading
    /// lets the test deadline fire instead of spinning.
    yielded: bool,
}
impl Counted {
    fn new(data: Vec<u8>, chunk: usize) -> Self {
        Self {
            data,
            pos: 0,
            chunk,
            yielded: false,
        }
    }
}
impl tokio::io::AsyncRead for Counted {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if !std::mem::replace(&mut self.yielded, true) {
            cx.waker().wake_by_ref();
            return std::task::Poll::Pending;
        }
        self.yielded = false;
        let end = (self.pos + self.chunk)
            .min(self.data.len())
            .min(self.pos + buf.remaining());
        buf.put_slice(&self.data[self.pos..end]);
        self.pos = end;
        std::task::Poll::Ready(Ok(()))
    }
}

fn padded_observation(total_body: usize) -> Vec<u8> {
    let body = fixture("observation_rpc_request-positive");
    let mut frame = body[..body.len() - 1].to_vec();
    frame.resize(total_body - 1, b' ');
    frame.push(b'}');
    assert_eq!(frame.len(), total_body);
    frame
}

const CAP: usize = chanvoy_core::runtime::control::RPC_MAX_BYTES;

/// A reader that stops waiting fails the test instead of hanging it.
async fn bounded_read<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> Frame {
    tokio::time::timeout(Duration::from_secs(5), FrameReader::default().read(reader))
        .await
        .expect("frame reader returned without waiting for more input")
        .unwrap()
}

#[tokio::test]
async fn a_control_frame_of_exactly_the_cap_is_answered() {
    let mut frame = padded_observation(CAP);
    frame.push(b'\n');
    let (replies, _, _, outcome) = exchange(&[frame[..frame.len() - 1].to_vec()]).await;
    outcome.unwrap();
    assert_eq!(
        replies[0]["result"]["kind"], "lifecycle_observation",
        "{}",
        replies[0]
    );
}

#[tokio::test]
async fn an_over_cap_control_frame_stops_at_cap_plus_one_bytes() {
    // Body over the cap, no LF, more data behind it: the reader must stop.
    let mut data = padded_observation(CAP + 4096);
    data.extend_from_slice(b"\n");
    data.extend_from_slice(&fixture("observation_rpc_request-positive"));
    data.extend_from_slice(b"\n");
    for chunk in [1, 7, 4096, 1 << 20] {
        let mut reader = BufReader::with_capacity(64, Counted::new(data.clone(), chunk));
        let frame = bounded_read(&mut reader).await;
        assert!(matches!(frame, Frame::ControlTooLarge), "chunk {chunk}");
        let consumed = reader.get_ref().pos - reader.buffer().len();
        assert_eq!(consumed, CAP + 1, "chunk {chunk}: consumed {consumed}");
    }
}

#[tokio::test]
async fn a_late_control_method_within_the_cap_window_is_refused() {
    // The method comes after ~16 KiB of other members, still inside cap + 1.
    let filler = "x".repeat(CAP - 200);
    let body = format!(
        r#"{{"jsonrpc":"2.0","pad":"{filler}","method":"runtime_retire_v1","params":{{}},"more":"{}"}}"#,
        "y".repeat(8192)
    );
    assert!(body.len() > CAP);
    let mut reader = BufReader::new(Counted::new(body.into_bytes(), 1024));
    assert!(matches!(
        bounded_read(&mut reader).await,
        Frame::ControlTooLarge
    ));
}

#[tokio::test]
async fn an_over_cap_legacy_frame_is_still_read_to_its_delimiter() {
    let request = chanvoy_core::rpc_request("profile_status", json!({"note": "z".repeat(3 * CAP)}));
    let mut data = serde_json::to_vec(&request).unwrap();
    data.push(b'\n');
    let expected = data.clone();
    let mut reader = BufReader::new(Counted::new(data, 4096));
    match bounded_read(&mut reader).await {
        Frame::Line(bytes) => assert_eq!(bytes, expected),
        _ => panic!("legacy frame refused"),
    }
    // And the daemon still serves it.
    let (replies, _, _, outcome) = exchange(&[serde_json::to_vec(&request).unwrap()]).await;
    outcome.unwrap();
    assert!(replies[0].get("result").is_some(), "{}", replies[0]);
}

#[tokio::test]
async fn a_framing_refusal_never_reinterprets_the_buffered_suffix() {
    let mut data = padded_observation(CAP + 100);
    data.push(b'\n');
    data.extend_from_slice(&fixture("observation_rpc_request-positive"));
    data.push(b'\n');
    // Both frames in one write; read every reply until the server closes.
    let state = crate::request_frontier::tests::state("http://127.0.0.1:9");
    let (reader, client) = tokio::io::duplex(64 * 1024);
    let (server_read, server_write) = tokio::io::split(reader);
    let (shutdown, _rx) = oneshot::channel();
    let task = tokio::spawn(crate::handle_client_io(
        BufReader::new(server_read),
        server_write,
        Arc::clone(&state),
        Arc::new(Mutex::new(Some(shutdown))),
    ));
    let (read, mut write) = tokio::io::split(client);
    let writer = tokio::spawn(async move {
        let _ = write.write_all(&data).await;
        write
    });
    let mut replies = Vec::new();
    let mut lines = BufReader::new(read).lines();
    while let Ok(Some(line)) = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("server closes after a framing refusal")
    {
        replies.push(serde_json::from_str::<Value>(&line).unwrap());
    }
    writer.abort();
    let _ = writer.await;
    task.await.unwrap().unwrap();
    assert_eq!(replies.len(), 1, "{replies:?}");
    assert_eq!(replies[0]["error"]["code"], -32600);
}

#[tokio::test]
async fn invalid_utf8_in_a_control_frame_gets_a_protocol_error() {
    let mut frame = fixture("observation_rpc_request-positive");
    let at = frame.len() - 2;
    frame.insert(at, 0xff);
    let (replies, _, _, outcome) = exchange(&[frame]).await;
    outcome.unwrap();
    assert_eq!(replies[0]["error"]["code"], -32600, "{replies:?}");
}

// --- cancellation by other select! arms ---------------------------------------

/// Serves scripted chunks; before each chunk after the first `ready` it parks
/// until released, and signals `consumed` once it has served everything up to
/// the park point.
struct Scripted {
    chunks: std::collections::VecDeque<Vec<u8>>,
    park_before: usize,
    served: usize,
    consumed: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    acquire: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
}
impl tokio::io::AsyncRead for Scripted {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.served == self.park_before {
            if self.acquire.is_none() {
                self.consumed.notify_one();
                let release = Arc::clone(&self.release);
                self.acquire = Some(Box::pin(async move {
                    release.acquire().await.unwrap().forget();
                }));
            }
            match self.acquire.as_mut().unwrap().as_mut().poll(cx) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(()) => {
                    self.acquire = None;
                    self.park_before = usize::MAX;
                }
            }
        }
        match self.chunks.front_mut() {
            Some(chunk) => {
                let n = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk[..n]);
                chunk.drain(..n);
                if chunk.is_empty() {
                    self.chunks.pop_front();
                    self.served += 1;
                }
                std::task::Poll::Ready(Ok(()))
            }
            None => std::task::Poll::Pending,
        }
    }
}

/// A subscribed client sends a control-method prefix without LF; a
/// subscription event wakes the loop; the client then sends a legacy
/// `shutdown` and LF. The single wire frame names a control method and must be
/// strict-refused as a whole. The suffix must never reach legacy dispatch.
#[tokio::test]
async fn an_event_wake_never_drops_a_partial_frame() {
    for (name, prefix) in [
        ("below-cap", br#"{"method":"runtime_retire_v1"} "#.to_vec()),
        ("budget-split", {
            let mut p = br#"{"method":"runtime_observation_v1","pad":""#.to_vec();
            p.resize(CAP - 64, b'x');
            p
        }),
    ] {
        let state = crate::request_frontier::tests::state("http://127.0.0.1:9");
        let subscribe = chanvoy_core::rpc_request("subscribe", json!({"filter": "all_monitored"}));
        let shutdown_req = chanvoy_core::rpc_request("shutdown", json!({}));
        let mut first = serde_json::to_vec(&subscribe).unwrap();
        first.push(b'\n');
        let mut suffix = br#""}"#.to_vec();
        if name == "below-cap" {
            suffix.clear();
        }
        suffix.extend_from_slice(&serde_json::to_vec(&shutdown_req).unwrap());
        // Pad past the cap for the split-budget case so the whole frame is over cap.
        if name == "budget-split" {
            let filler = vec![b' '; 256];
            suffix.splice(0..0, filler);
        }
        suffix.push(b'\n');
        let consumed = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let reader = Scripted {
            chunks: [first, prefix, suffix].into_iter().collect(),
            park_before: 2,
            served: 0,
            consumed: Arc::clone(&consumed),
            release: Arc::clone(&release),
            acquire: None,
        };
        let (server_write, client) = tokio::io::duplex(64 * 1024);
        let (shutdown, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(crate::handle_client_io(
            BufReader::new(reader),
            server_write,
            Arc::clone(&state),
            Arc::new(Mutex::new(Some(shutdown))),
        ));
        let mut lines = BufReader::new(client).lines();
        let ack: Value = serde_json::from_str(
            &tokio::time::timeout(Duration::from_secs(5), lines.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(
            ack["result"]["subscription_id"].is_string(),
            "{name}: {ack}"
        );
        // The prefix is consumed and the reader is parked mid-frame.
        tokio::time::timeout(Duration::from_secs(5), consumed.notified())
            .await
            .expect("prefix consumed");
        // An event wakes the connection's other select! arm.
        let _ = state.event_bus.sender().send(Arc::new(
            crate::request_frontier::tests::subscriptions::event(),
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        release.add_permits(1);
        let mut replies = Vec::new();
        while let Ok(Ok(Some(line))) =
            tokio::time::timeout(Duration::from_secs(2), lines.next_line()).await
        {
            replies.push(serde_json::from_str::<Value>(&line).unwrap());
        }
        task.abort();
        let refusals: Vec<_> = replies
            .iter()
            .filter(|r| r.get("error").is_some())
            .collect();
        assert_eq!(refusals.len(), 1, "{name}: {replies:?}");
        assert!(
            replies.iter().all(|r| r
                .get("result")
                .is_none_or(|res| res.get("stopping").is_none())),
            "{name}: legacy shutdown was dispatched: {replies:?}"
        );
        assert!(
            shutdown_rx.try_recv().is_err(),
            "{name}: shutdown signalled"
        );
    }
}
