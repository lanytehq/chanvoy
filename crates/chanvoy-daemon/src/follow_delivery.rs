//! Operation-local delivery controls for the channel-follow RPC surface.
//! Dedicated DM, inbox and fan-in writers do not use this context.
use std::future::{poll_fn, Future};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::wait_owner::WaitSession;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StopCause {
    Deadline,
    Canceled,
    Replaced,
    Transport,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Gate {
    Closed,
    Open,
    Failed,
}

#[derive(Clone, Default)]
struct Published {
    deadline: Option<Instant>,
    session: Option<WaitSession>,
}

struct Inner {
    published: watch::Sender<Published>,
    gate: watch::Sender<Gate>,
    cause: Mutex<Option<StopCause>>,
    client_gone: CancellationToken,
    failed: CancellationToken,
    admission_stopped: AtomicBool,
    armed_committed: AtomicBool,
    #[cfg(test)]
    ack_hold: Mutex<Option<Arc<tokio::sync::Notify>>>,
    #[cfg(test)]
    framed_calls: std::sync::atomic::AtomicUsize,
}

/// Allocated by the channel RPC, initialized by its wait wrapper. Publication
/// never holds a state lock over provider, gate or transport I/O.
#[derive(Clone)]
pub(crate) struct ChannelDelivery(Arc<Inner>);

impl ChannelDelivery {
    pub(crate) fn new(client_gone: CancellationToken) -> Self {
        Self(Arc::new(Inner {
            published: watch::channel(Published::default()).0,
            gate: watch::channel(Gate::Closed).0,
            cause: Mutex::new(None),
            client_gone,
            failed: CancellationToken::new(),
            admission_stopped: AtomicBool::new(false),
            armed_committed: AtomicBool::new(false),
            #[cfg(test)]
            ack_hold: Mutex::new(None),
            #[cfg(test)]
            framed_calls: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    pub(crate) fn publish_deadline(&self, deadline: Instant) {
        self.0.published.send_modify(|published| {
            assert!(
                published.deadline.is_none(),
                "channel deadline published twice"
            );
            published.deadline = Some(deadline);
        });
    }

    pub(crate) fn publish_session(&self, session: &WaitSession) {
        self.0.published.send_modify(|published| {
            assert!(
                published.session.is_none(),
                "channel session published twice"
            );
            published.session = Some(session.clone());
        });
    }

    #[cfg(test)]
    pub(crate) fn published_deadline(&self) -> Option<Instant> {
        self.0.published.borrow().deadline
    }

    #[cfg(test)]
    pub(crate) fn session_published(&self) -> bool {
        self.0.published.borrow().session.is_some()
    }

    #[cfg(test)]
    pub(crate) fn framed_calls(&self) -> usize {
        self.0.framed_calls.load(Ordering::SeqCst)
    }

    fn latch(&self, cause: StopCause) -> StopCause {
        let mut current = self.0.cause.lock().expect("channel cause");
        *current.get_or_insert(cause)
    }

    pub(crate) fn cause(&self) -> Option<StopCause> {
        if self.0.failed.is_cancelled() {
            return Some(StopCause::Transport);
        }
        if let Some(cause) = *self.0.cause.lock().expect("channel cause") {
            return Some(cause);
        }
        let published = self.0.published.borrow().clone();
        let cause = if let Some(session) = published.session.filter(|s| s.cancel.is_cancelled()) {
            Some(if session.replaced_by_id().is_empty() {
                StopCause::Canceled
            } else {
                StopCause::Replaced
            })
        } else if self.0.client_gone.is_cancelled() {
            Some(StopCause::Canceled)
        } else if published
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Some(StopCause::Deadline)
        } else {
            None
        };
        cause.map(|cause| self.latch(cause))
    }

    pub(crate) async fn stopped(&self) -> StopCause {
        let mut changed = self.0.published.subscribe();
        loop {
            if let Some(cause) = self.cause() {
                return cause;
            }
            let published = changed.borrow_and_update().clone();
            let deadline = async {
                match published.deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };
            let replaced = async {
                match published.session {
                    Some(session) => session.cancel.cancelled().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = self.0.failed.cancelled() => {},
                _ = self.0.client_gone.cancelled() => {},
                _ = replaced => {},
                _ = deadline => {},
                _ = changed.changed() => {},
            }
        }
    }

    pub(crate) fn client_disconnected(&self) {
        self.0.client_gone.cancel();
    }

    pub(crate) fn poison(&self) {
        self.0.failed.cancel();
        self.0.gate.send_replace(Gate::Failed);
    }

    pub(crate) fn close_gate(&self) {
        self.0.gate.send_replace(Gate::Failed);
    }

    pub(crate) fn armed_committed(&self) -> bool {
        self.0.armed_committed.load(Ordering::SeqCst)
    }

    /// Called only after the producer consumes the successful real Armed ACK.
    pub(crate) fn open_gate(&self) {
        self.0.armed_committed.store(true, Ordering::SeqCst);
        if !self.0.failed.is_cancelled() {
            self.0.gate.send_replace(Gate::Open);
        }
    }

    pub(crate) async fn admit(&self) -> Result<(), StopCause> {
        let mut gate = self.0.gate.subscribe();
        loop {
            let current = *gate.borrow_and_update();
            if current == Gate::Open {
                // This check is the per-entry admission frontier. Callback
                // entry alone does not admit later items from the same burst.
                if let Some(cause) = self.cause() {
                    self.0.admission_stopped.store(true, Ordering::SeqCst);
                    return Err(cause);
                }
                return Ok(());
            }
            if current == Gate::Failed {
                return Err(StopCause::Transport);
            }
            tokio::select! {
                biased;
                _ = gate.changed() => {},
                cause = self.stopped() => {
                    self.0.admission_stopped.store(true, Ordering::SeqCst);
                    return Err(cause);
                }
            }
        }
    }

    pub(crate) fn admission_stopped(&self) -> bool {
        self.0.admission_stopped.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn hold_ack(&self, hold: Arc<tokio::sync::Notify>) {
        *self.0.ack_hold.lock().expect("ack hold") = Some(hold);
    }

    async fn framed_write<W: AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        frame: &[u8],
    ) -> io::Result<()> {
        writer.write_all(frame).await?;
        writer.flush().await?;
        #[cfg(test)]
        {
            let hold = self.0.ack_hold.lock().expect("ack hold").take();
            if let Some(hold) = hold {
                hold.notified().await;
            }
        }
        Ok(())
    }

    /// Channel-only RPC writer/producer driver. While a record is selected,
    /// the actual writer reads this same deadline/session context directly;
    /// it never relies on polling `follow` or a cancellation forwarder.
    pub(crate) async fn serve<W, R, F>(
        &self,
        writer: &mut W,
        reader: &mut R,
        id: uuid::Uuid,
        mut records: tokio::sync::mpsc::Receiver<crate::wait::FollowStreamRecord>,
        follow: F,
    ) -> Result<(), crate::DaemonError>
    where
        W: AsyncWrite + Unpin,
        R: AsyncBufRead + Unpin,
        F: Future<Output = Result<chanvoy_core::WaitFollowResult, chanvoy_core::CoreError>>,
    {
        tokio::pin!(follow);
        let mut eof_buf = String::new();
        let mut client_eof = false;
        loop {
            tokio::select! {
                biased;
                Some(record) = records.recv() => {
                    self.write_record(writer, reader, &mut eof_buf, record).await?;
                }
                result = &mut follow => {
                    // Only already-enqueued work can be drained. A failed
                    // selected writer exits above and drops the producer.
                    let queued = records.len();
                    for _ in 0..queued {
                        if let Ok(record) = records.try_recv() {
                            self.write_record(writer, reader, &mut eof_buf, record).await?;
                        }
                    }
                    let response = match result {
                        Ok(result) => chanvoy_core::rpc_result(id, crate::to_value(result)),
                        Err(error) => {
                            let error = crate::DaemonError::from(error);
                            let (code, message, data) = crate::error_payload(&error);
                            chanvoy_core::rpc_error_with_data(id, code, message, data)
                        }
                    };
                    self.write_response(writer, reader, &mut eof_buf, &response).await?;
                    return Ok(());
                }
                peek = reader.read_line(&mut eof_buf), if !client_eof => {
                    if peek.is_err() {
                        self.poison();
                        peek?;
                    }
                    self.client_disconnected();
                    client_eof = true;
                }
            }
        }
    }

    pub(crate) async fn write_record<W, R>(
        &self,
        writer: &mut W,
        reader: &mut R,
        eof_buf: &mut String,
        record: crate::wait::FollowStreamRecord,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        R: AsyncBufRead + Unpin,
    {
        let notification = chanvoy_core::JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: record.method.to_string(),
            params: record.event,
        };
        let result = match serde_json::to_vec(&notification) {
            Ok(mut frame) => {
                frame.push(b'\n');
                self.write(writer, reader, eof_buf, &frame, false).await
            }
            Err(error) => {
                self.poison();
                Err(io::Error::other(error))
            }
        };
        // This is the sole positive producer ACK frontier. The whole
        // frame/newline/flush (and test ACK barrier) has completed first.
        let ack = record
            .written
            .send(result.as_ref().map(|_| ()).map_err(ToString::to_string));
        if result.is_ok() && ack.is_err() {
            self.poison();
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "channel write acknowledgement unavailable",
            ));
        }
        result
    }

    pub(crate) async fn write_response<W, R, T>(
        &self,
        writer: &mut W,
        reader: &mut R,
        eof_buf: &mut String,
        response: &T,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        R: AsyncBufRead + Unpin,
        T: serde::Serialize,
    {
        let mut frame = serde_json::to_vec(response).map_err(|error| {
            self.poison();
            io::Error::other(error)
        })?;
        frame.push(b'\n');
        self.write(writer, reader, eof_buf, &frame, true).await
    }

    /// Whole newline-delimited frame, one preserved write future. After a
    /// cause, poll transport completion once; Pending poisons, never retries.
    pub(crate) async fn write<W, R>(
        &self,
        writer: &mut W,
        reader: &mut R,
        eof_buf: &mut String,
        frame: &[u8],
        completion: bool,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        R: AsyncBufRead + Unpin,
    {
        #[cfg(test)]
        self.0.framed_calls.fetch_add(1, Ordering::SeqCst);
        if self.0.failed.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "channel transport unusable",
            ));
        }
        let published = self.0.published.borrow().clone();
        if !completion && (published.deadline.is_none() || published.session.is_none()) {
            self.poison();
            return Err(io::Error::other("channel delivery context unavailable"));
        }
        let write = self.framed_write(writer, frame);
        tokio::pin!(write);
        // A validation refusal before deadline publication has no admitted
        // operation budget. Its response is ready-only, never an unbounded
        // write or a newly invented fallback deadline.
        let mut result = if self.cause().is_some() || published.deadline.is_none() {
            ready_only(write.as_mut()).await
        } else {
            tokio::select! {
                biased;
                result = &mut write => result,
                _ = self.stopped() => ready_only(write.as_mut()).await,
                read = reader.read_line(eof_buf) => {
                    match read {
                        Ok(_) => {
                            self.client_disconnected();
                            ready_only(write.as_mut()).await
                        }
                        Err(error) => Err(error),
                    }
                }
            }
        };
        if result.is_ok() {
            // Finish a ready complete frame, then observe a simultaneously
            // ready EOF before the producer can admit another entry. This
            // peeks once without consuming input or awaiting readiness.
            result = poll_fn(|cx| {
                Poll::Ready(match std::pin::Pin::new(&mut *reader).poll_fill_buf(cx) {
                    Poll::Ready(Ok(bytes)) => {
                        if bytes.is_empty() {
                            self.client_disconnected();
                        }
                        Ok(())
                    }
                    Poll::Ready(Err(error)) => Err(error),
                    Poll::Pending => Ok(()),
                })
            })
            .await;
        }
        if result.is_err() {
            self.poison();
        }
        result
    }
}

async fn ready_only<F: Future<Output = io::Result<()>>>(future: F) -> io::Result<()> {
    tokio::pin!(future);
    poll_fn(|cx| {
        Poll::Ready(match future.as_mut().poll(cx) {
            Poll::Ready(result) => result,
            Poll::Pending => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "channel completion not immediately available",
            )),
        })
    })
    .await
}

#[cfg(test)]
#[path = "follow_delivery_tests.rs"]
pub(crate) mod tests;
