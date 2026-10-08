use super::*;
use crate::wait_owner::WaitSession;
use std::pin::Pin;
use std::task::{Context, Waker};
use std::time::Duration;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::UnixStream;

#[derive(Default)]
pub(crate) struct Hold {
    pub(crate) reached: tokio::sync::Notify,
    released: AtomicBool,
    waker: Mutex<Option<Waker>>,
}
impl Hold {
    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
    fn poll(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.reached.notify_one();
        *self.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.released.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

/// Hold the actual AsyncWrite call or flush on an owned UDS. No fake ACK
/// replaces production ChannelDelivery::write_record.
pub(crate) struct HeldWriter<W> {
    pub(crate) inner: W,
    pub(crate) hold: Arc<Hold>,
    pub(crate) after_bytes: Option<usize>,
    pub(crate) flush: bool,
    written: usize,
}
impl<W> HeldWriter<W> {
    pub(crate) fn new(inner: W, after_bytes: Option<usize>, flush: bool) -> Self {
        Self {
            inner,
            hold: Arc::new(Hold::default()),
            after_bytes,
            flush,
            written: 0,
        }
    }
}
impl<W: AsyncWrite + Unpin> AsyncWrite for HeldWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.after_bytes.is_some_and(|limit| self.written >= limit) {
            match self.hold.poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            }
        }
        let count = self
            .after_bytes
            .filter(|limit| self.written < *limit)
            .map(|limit| buf.len().min(limit - self.written))
            .unwrap_or(buf.len());
        match Pin::new(&mut self.inner).poll_write(cx, &buf[..count]) {
            Poll::Ready(Ok(n)) => {
                self.written += n;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.flush {
            match self.hold.poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            }
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn context(deadline: Instant, replaced: bool) -> (ChannelDelivery, WaitSession) {
    let session = WaitSession::for_test(
        "wait_0123456789abcdef0123456789abcdef",
        replaced.then_some("wait_abcdef0123456789abcdef0123456789"),
        false,
    );
    let delivery = ChannelDelivery::new(CancellationToken::new());
    delivery.publish_deadline(deadline);
    delivery.publish_session(&session);
    (delivery, session)
}

#[tokio::test]
async fn actual_writer_pending_is_interrupted_at_every_frontier() {
    // Each stage runs against the actual production helper, including final
    // response completion. The unchanged operation deadline is the only
    // production clock; test timeouts merely fail a hung test.
    for stage in [Some(0), Some(7), None] {
        for cause in [
            StopCause::Deadline,
            StopCause::Canceled,
            StopCause::Replaced,
            StopCause::Transport,
        ] {
            for response in [false, true] {
                let (server, mut client) = UnixStream::pair().unwrap();
                let (read, write) = server.into_split();
                let mut reader = BufReader::new(read);
                let mut writer = HeldWriter::new(write, stage, stage.is_none());
                let hold = Arc::clone(&writer.hold);
                let (delivery, session) = context(
                    Instant::now() + Duration::from_millis(100),
                    cause == StopCause::Replaced,
                );
                let control = delivery.clone();
                let task = tokio::spawn(async move {
                    delivery
                        .write(
                            &mut writer,
                            &mut reader,
                            &mut String::new(),
                            b"{\"receipt\":true}\n",
                            response,
                        )
                        .await
                });
                tokio::time::timeout(Duration::from_secs(2), hold.reached.notified())
                    .await
                    .unwrap();
                match cause {
                    StopCause::Deadline => {}
                    StopCause::Canceled | StopCause::Replaced => session.cancel.cancel(),
                    StopCause::Transport => {
                        client.shutdown().await.unwrap();
                    }
                }
                let result = tokio::time::timeout(Duration::from_secs(2), task)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    result.is_err(),
                    "Pending must not become success: {stage:?}/{cause:?}/{response}"
                );
                assert_eq!(control.cause(), Some(StopCause::Transport));
                hold.release(); // No stopped future may resume writing later.
                let mut bytes = Vec::new();
                client.read_to_end(&mut bytes).await.unwrap();
                let expected = match stage {
                    Some(n) => n,
                    None => b"{\"receipt\":true}\n".len(),
                };
                assert_eq!(
                    bytes.len(),
                    expected,
                    "partial frame resumed after teardown"
                );
            }
        }
    }
}

#[tokio::test]
async fn actual_ack_barrier_is_not_bytes_visible_success() {
    let (server, mut client) = UnixStream::pair().unwrap();
    let (read, mut write) = server.into_split();
    let mut reader = BufReader::new(read);
    let (delivery, session) = context(Instant::now() + Duration::from_secs(10), false);
    let hold = Arc::new(tokio::sync::Notify::new());
    delivery.hold_ack(Arc::clone(&hold));
    let control = delivery.clone();
    let task = tokio::spawn(async move {
        delivery
            .write(
                &mut write,
                &mut reader,
                &mut String::new(),
                b"complete\n",
                false,
            )
            .await
    });
    let mut visible = [0; 9];
    client.read_exact(&mut visible).await.unwrap();
    assert_eq!(&visible, b"complete\n");
    assert!(!task.is_finished());
    session.cancel.cancel();
    assert!(tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert_eq!(control.cause(), Some(StopCause::Transport));
    hold.notify_one();
    assert!(!control.armed_committed());
}

#[tokio::test]
async fn simultaneous_actual_ack_and_cancel_commits_the_write_first() {
    let (server, mut client) = UnixStream::pair().unwrap();
    let (read, mut write) = server.into_split();
    let mut reader = BufReader::new(read);
    let (delivery, session) = context(Instant::now() + Duration::from_secs(10), false);
    let hold = Arc::new(tokio::sync::Notify::new());
    delivery.hold_ack(Arc::clone(&hold));
    let task = tokio::spawn(async move {
        delivery
            .write(
                &mut write,
                &mut reader,
                &mut String::new(),
                b"complete\n",
                false,
            )
            .await
    });
    client.read_exact(&mut [0; 9]).await.unwrap();
    // Both wakes become ready without yielding this current-thread executor.
    hold.notify_one();
    session.cancel.cancel();
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test]
async fn elapsed_deadline_allows_only_immediate_finite_completion() {
    let (server, mut client) = UnixStream::pair().unwrap();
    let (read, mut write) = server.into_split();
    let mut reader = BufReader::new(read);
    // Establish the declared actual Tokio write-readiness condition before
    // the cause. A newly registered socket can otherwise return Pending on
    // its first poll even with kernel capacity; that is correctly teardown.
    write.as_ref().writable().await.unwrap();
    let (delivery, _) = context(Instant::now(), false);
    delivery
        .write(
            &mut write,
            &mut reader,
            &mut String::new(),
            b"terminal\n",
            false,
        )
        .await
        .unwrap();
    delivery
        .write_response(
            &mut write,
            &mut reader,
            &mut String::new(),
            &serde_json::json!({"result":"deadman"}),
        )
        .await
        .unwrap();
    drop(write);
    let mut bytes = String::new();
    client.read_to_string(&mut bytes).await.unwrap();
    assert_eq!(bytes, "terminal\n{\"result\":\"deadman\"}\n");
}

#[tokio::test]
async fn missing_stream_context_refuses_and_early_response_has_no_fallback_wait() {
    let (server, _) = UnixStream::pair().unwrap();
    let (read, write) = server.into_split();
    let mut reader = BufReader::new(read);
    let mut writer = HeldWriter::new(write, Some(0), false);
    let delivery = ChannelDelivery::new(CancellationToken::new());
    assert!(delivery
        .write(
            &mut writer,
            &mut reader,
            &mut String::new(),
            b"armed\n",
            false
        )
        .await
        .is_err());
    assert!(!delivery.armed_committed());
    assert!(delivery
        .write_response(
            &mut writer,
            &mut reader,
            &mut String::new(),
            &serde_json::json!({"error":"invalid"})
        )
        .await
        .is_err());
}

#[tokio::test]
async fn completion_drain_and_final_response_use_the_same_bounded_writer_without_optimistic_retry()
{
    use chanvoy_core::{rpc_result, WaitFollowEvent, WaitFollowResult, WaitFollowResultKind};
    for frontier in ["drain", "response"] {
        let (server, mut client) = UnixStream::pair().unwrap();
        let (read, write) = server.into_split();
        write.as_ref().writable().await.unwrap();
        let mut reader = BufReader::new(read);
        let (delivery, session) = context(Instant::now(), false);
        let event =
            serde_json::to_value(WaitFollowEvent::armed(session.wait_id.clone(), None)).unwrap();
        let document = chanvoy_core::JsonRpcNotification {
            jsonrpc: "2.0".into(),
            method: chanvoy_core::WAIT_FOLLOW_V1_EVENT_METHOD.into(),
            params: event.clone(),
        };
        let frame_size = serde_json::to_vec(&document).unwrap().len() + 1;
        let mut writer = HeldWriter::new(
            write,
            Some(if frontier == "drain" { 7 } else { frame_size }),
            false,
        );
        let hold = Arc::clone(&writer.hold);
        let (written, receipt) = tokio::sync::oneshot::channel();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        // Initially recv is Pending. The completion branch's actual future
        // enqueues its sole finite record in the same poll before Ready.
        let follow = async move {
            tx.try_send(crate::wait::FollowStreamRecord {
                method: chanvoy_core::WAIT_FOLLOW_V1_EVENT_METHOD,
                event,
                written,
            })
            .unwrap();
            Ok(WaitFollowResult {
                wait_id: session.wait_id,
                kind: WaitFollowResultKind::Deadman { tip: None },
            })
        };
        assert!(delivery
            .serve(&mut writer, &mut reader, uuid::Uuid::nil(), rx, follow)
            .await
            .is_err());
        let receipt = receipt.await.unwrap();
        assert_eq!(receipt.is_ok(), frontier == "response");
        assert_eq!(
            delivery.framed_calls(),
            if frontier == "drain" { 1 } else { 2 }
        );
        // A poisoned stream cannot be revived even if readiness later arrives.
        hold.release();
        assert!(delivery
            .write_response(
                &mut writer,
                &mut reader,
                &mut String::new(),
                &rpc_result(uuid::Uuid::nil(), serde_json::json!({"mode":"deadman"}))
            )
            .await
            .is_err());
        drop(writer);
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(
            bytes.len(),
            if frontier == "drain" { 7 } else { frame_size }
        );
    }
}

#[tokio::test]
async fn full_visible_frame_with_missing_producer_ack_is_hard_teardown() {
    let (server, client) = UnixStream::pair().unwrap();
    let (read, mut writer) = server.into_split();
    let mut reader = BufReader::new(read);
    let mut client = BufReader::new(client);
    let (delivery, session) = context(Instant::now() + Duration::from_secs(5), false);
    let (written, receipt) = tokio::sync::oneshot::channel();
    drop(receipt);
    let event =
        serde_json::to_value(chanvoy_core::WaitFollowEvent::armed(session.wait_id, None)).unwrap();
    let record = crate::wait::FollowStreamRecord {
        method: chanvoy_core::WAIT_FOLLOW_V1_EVENT_METHOD,
        event,
        written,
    };
    assert!(delivery
        .write_record(&mut writer, &mut reader, &mut String::new(), record)
        .await
        .is_err());
    let mut line = String::new();
    client.read_line(&mut line).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(line.trim_end()).unwrap()["params"]["mode"],
        "armed"
    );
    assert!(!delivery.armed_committed());
    assert_eq!(delivery.cause(), Some(StopCause::Transport));
    assert_eq!(delivery.admit().await, Err(StopCause::Transport));
}

#[tokio::test]
async fn either_missing_required_stream_field_refuses_before_any_bytes() {
    for missing_deadline in [false, true] {
        let (server, mut client) = UnixStream::pair().unwrap();
        let (read, mut writer) = server.into_split();
        writer.as_ref().writable().await.unwrap();
        let mut reader = BufReader::new(read);
        let delivery = ChannelDelivery::new(CancellationToken::new());
        let session = WaitSession::for_test("wait_0123456789abcdef0123456789abcdef", None, false);
        if missing_deadline {
            delivery.publish_session(&session);
        } else {
            delivery.publish_deadline(Instant::now() + Duration::from_secs(5));
        }
        let (written, receipt) = tokio::sync::oneshot::channel();
        let event =
            serde_json::to_value(chanvoy_core::WaitFollowEvent::armed(session.wait_id, None))
                .unwrap();
        assert!(delivery
            .write_record(
                &mut writer,
                &mut reader,
                &mut String::new(),
                crate::wait::FollowStreamRecord {
                    method: chanvoy_core::WAIT_FOLLOW_V1_EVENT_METHOD,
                    event,
                    written,
                }
            )
            .await
            .is_err());
        assert!(receipt.await.unwrap().is_err());
        drop(writer);
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
        assert!(!delivery.armed_committed());
    }
}
