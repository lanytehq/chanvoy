use super::*;

const METHODS: [&str; 6] = [
    "wait_follow_v1",
    "wait_follow_v2",
    "wait_dm_follow_v1",
    "wait_dm_follow_v2",
    "wait_inbox_follow_v1",
    "wait_inbox_follow_v2",
];

fn params(method: &str) -> serde_json::Value {
    let mut value = json!({"timeout_secs":30});
    if method.starts_with("wait_dm_") {
        value["username"] = json!("peer");
    } else if !method.starts_with("wait_inbox_") {
        value["channel"] = json!("unmonitored");
    }
    if method.ends_with("v2") {
        value["coalesce_ms"] = json!(5000);
    }
    value
}

struct ObservedRead<R> {
    inner: R,
    eof: Arc<Notify>,
}
impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for ObservedRead<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let available = buf.remaining();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && available > 0 && before == buf.filled().len() {
            self.eof.notify_one();
        }
        result
    }
}

fn held_busy(state: &AppState) {
    busy(state);
    assert_eq!(state.wait_owners.admission_snapshot().streams, 1);
}

async fn healthy(state: &AppState) {
    let ws = Arc::new(chanvoy_core::WsState::new());
    ws.set_state(chanvoy_core::WsConnectionState::Healthy).await;
    *state.ws_state_holder.lock().await = Some(ws);
}

async fn owners_cleared(state: &AppState) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.wait_owners.admission_snapshot().requests != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(state.wait_owners.admission_snapshot().streams, 0);
}

#[tokio::test]
async fn every_held_alias_owns_request_and_delivery_before_provider_preflight() {
    for method in METHODS {
        let mut provider = ProviderGate::new().await;
        let state = state(&provider.url);
        healthy(&state).await;
        let (server, mut client) = UnixStream::pair().unwrap();
        let (shutdown, _rx) = oneshot::channel();
        let task = tokio::spawn(handle_client(
            server,
            Arc::clone(&state),
            Arc::new(Mutex::new(Some(shutdown))),
        ));
        let request = rpc_request(method, params(method));
        client
            .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
            .await
            .unwrap();
        provider.observed().await;
        held_busy(&state);
        assert_eq!(state.wait_owners.admission_snapshot().slots, 0);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        provider.finish().await;
        held_busy(&state);
    }
}

#[tokio::test]
async fn every_held_terminal_response_keeps_both_owners_until_final_flush() {
    for method in METHODS {
        let state = state("http://127.0.0.1:9");
        let gate = Arc::new(Gate::default());
        let writer = Writer {
            gate: Arc::clone(&gate),
            stage: Some(Stage::Flush),
            fail: false,
        };
        let (reader, mut client) = tokio::io::duplex(4096);
        let (shutdown, _rx) = oneshot::channel();
        let task = tokio::spawn(crate::handle_client_io(
            BufReader::new(reader),
            writer,
            Arc::clone(&state),
            Arc::new(Mutex::new(Some(shutdown))),
        ));
        let mut input = params(method);
        // Valid request shape, rejected inside the actual held producer.
        input["pattern"] = json!("[");
        let request = rpc_request(method, input);
        client
            .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), gate.reached.notified())
            .await
            .unwrap();
        held_busy(&state);
        gate.release();
        owners_cleared(&state).await;
        drop(client);
        collect(task).await.unwrap();
        let bytes = gate.bytes.lock().unwrap();
        let response: chanvoy_core::JsonRpcResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(response.error.is_some());
    }
}

#[tokio::test]
async fn failed_held_terminal_flush_keeps_both_owners() {
    for method in METHODS {
        let state = state("http://127.0.0.1:9");
        let writer = Writer {
            gate: Arc::new(Gate::default()),
            stage: Some(Stage::Flush),
            fail: true,
        };
        let (reader, mut client) = tokio::io::duplex(4096);
        let (shutdown, _rx) = oneshot::channel();
        let task = tokio::spawn(crate::handle_client_io(
            BufReader::new(reader),
            writer,
            Arc::clone(&state),
            Arc::new(Mutex::new(Some(shutdown))),
        ));
        let mut input = params(method);
        input["pattern"] = json!("[");
        let request = rpc_request(method, input);
        client
            .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
            .await
            .unwrap();
        assert!(matches!(collect(task).await, Err(DaemonError::Io(_))));
        held_busy(&state);
    }
}

#[tokio::test]
async fn short_held_input_response_confirms_only_after_full_flush() {
    for method in METHODS {
        let state = state("http://127.0.0.1:9");
        let gate = Arc::new(Gate::default());
        let writer = Writer {
            gate: Arc::clone(&gate),
            stage: Some(Stage::Flush),
            fail: false,
        };
        let (reader, mut client) = tokio::io::duplex(4096);
        let (shutdown, _rx) = oneshot::channel();
        let task = tokio::spawn(crate::handle_client_io(
            BufReader::new(reader),
            writer,
            Arc::clone(&state),
            Arc::new(Mutex::new(Some(shutdown))),
        ));
        let request = rpc_request(method, json!({"timeout_secs":"invalid"}));
        client
            .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), gate.reached.notified())
            .await
            .unwrap();
        busy(&state);
        assert_eq!(state.wait_owners.admission_snapshot().streams, 0);
        gate.release();
        owners_cleared(&state).await;
        drop(client);
        collect(task).await.unwrap();
    }
}

#[tokio::test]
async fn dm_and_inbox_v1_eof_cannot_confirm_terminal_completion() {
    for method in ["wait_dm_follow_v1", "wait_inbox_follow_v1"] {
        let mut provider = ProviderGate::new().await;
        let state = state(&provider.url);
        healthy(&state).await;
        let (server, mut client) = UnixStream::pair().unwrap();
        let (shutdown, _rx) = oneshot::channel();
        let task = tokio::spawn(handle_client(
            server,
            Arc::clone(&state),
            Arc::new(Mutex::new(Some(shutdown))),
        ));
        let request = rpc_request(method, params(method));
        client
            .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
            .await
            .unwrap();
        provider.observed().await;
        client.shutdown().await.unwrap();
        collect(task).await.unwrap();
        provider.finish().await;
        held_busy(&state);
    }
}

#[tokio::test]
async fn dm_and_inbox_v2_eof_during_preflight_does_not_confirm() {
    for method in ["wait_dm_follow_v2", "wait_inbox_follow_v2"] {
        let mut provider = ProviderGate::new().await;
        let state = state(&provider.url);
        healthy(&state).await;
        let (server, mut client) = UnixStream::pair().unwrap();
        let (shutdown, _rx) = oneshot::channel();
        let eof = Arc::new(Notify::new());
        let (read, write) = server.into_split();
        let reader = ObservedRead {
            inner: read,
            eof: Arc::clone(&eof),
        };
        let task = tokio::spawn(crate::handle_client_io(
            BufReader::new(reader),
            write,
            Arc::clone(&state),
            Arc::new(Mutex::new(Some(shutdown))),
        ));
        let request = rpc_request(method, params(method));
        client
            .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
            .await
            .unwrap();
        provider.observed().await;
        client.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), eof.notified())
            .await
            .unwrap();
        held_busy(&state);
        // The provider gate cannot complete. Collect cancellation of the actual
        // handler, then inspect the arena, rather than treating EOF as a join.
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        provider.finish().await;
        held_busy(&state);
    }
}

#[tokio::test]
async fn lost_selected_ack_cannot_reach_foreground_confirmation() {
    let state = state("http://127.0.0.1:9");
    let request = state.wait_owners.request_permit().unwrap();
    let stream = request.reserve_stream().unwrap();
    let (tx, rx) = oneshot::channel();
    drop(rx);
    let result = async {
        acknowledge(tx, Ok(()))?;
        finish_held(&mut tokio::io::sink(), request, stream).await
    }
    .await;
    assert!(matches!(result, Err(DaemonError::Io(_))));
    held_busy(&state);
}

#[tokio::test]
async fn selected_channel_payload_newline_flush_and_ack_keep_both_owners() {
    use crate::follow_delivery::{tests::HeldWriter, ChannelDelivery};
    use chanvoy_core::{WaitFollowEvent, WaitFollowResult, WaitFollowResultKind};
    for stage in ["payload", "newline", "flush", "ack"] {
        let state = state("http://127.0.0.1:9");
        let request = state.wait_owners.request_permit().unwrap();
        let owner = request.reserve_stream().unwrap();
        let session =
            wait_owner::WaitSession::for_test("wait_0123456789abcdef0123456789abcdef", None, false);
        let delivery = ChannelDelivery::new(tokio_util::sync::CancellationToken::new());
        delivery.publish_deadline(tokio::time::Instant::now() + Duration::from_secs(30));
        delivery.publish_session(&session);
        let event =
            serde_json::to_value(WaitFollowEvent::armed(session.wait_id.clone(), None)).unwrap();
        let document = chanvoy_core::JsonRpcNotification {
            jsonrpc: "2.0".into(),
            method: chanvoy_core::WAIT_FOLLOW_V1_EVENT_METHOD.into(),
            params: event.clone(),
        };
        let payload_len = serde_json::to_vec(&document).unwrap().len();
        let (server, client) = UnixStream::pair().unwrap();
        let (read, write) = server.into_split();
        let mut reader = BufReader::new(read);
        let mut writer = HeldWriter::new(
            write,
            match stage {
                "payload" => Some(0),
                "newline" => Some(payload_len),
                _ => None,
            },
            stage == "flush",
        );
        let hold = Arc::clone(&writer.hold);
        let ack_hold = Arc::new(Notify::new());
        if stage == "ack" {
            delivery.hold_ack(Arc::clone(&ack_hold));
        }
        let (written, receipt) = oneshot::channel();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let follow = async move {
            tx.send(crate::wait::FollowStreamRecord {
                method: chanvoy_core::WAIT_FOLLOW_V1_EVENT_METHOD,
                event,
                written,
            })
            .await
            .unwrap();
            receipt.await.unwrap().unwrap();
            Ok(WaitFollowResult {
                wait_id: session.wait_id,
                kind: WaitFollowResultKind::Deadman { tip: None },
            })
        };
        let task = tokio::spawn(async move {
            delivery
                .serve(&mut writer, &mut reader, uuid::Uuid::nil(), rx, follow)
                .await?;
            finish_held(&mut writer, request, owner).await
        });
        let mut client = BufReader::new(client);
        if stage == "ack" {
            let mut frame = String::new();
            tokio::time::timeout(Duration::from_secs(2), client.read_line(&mut frame))
                .await
                .unwrap()
                .unwrap();
            assert!(frame.ends_with('\n'));
        } else {
            tokio::time::timeout(Duration::from_secs(2), hold.reached.notified())
                .await
                .unwrap();
        }
        held_busy(&state);
        hold.release();
        ack_hold.notify_one();
        collect(task).await.unwrap();
        owners_cleared(&state).await;
    }
}
