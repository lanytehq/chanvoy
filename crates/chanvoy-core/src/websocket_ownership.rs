//! One managed websocket execution, with independent actual-task collection.
use crate::request_ownership::{RequestAdmission, RequestCompletion};
use crate::{CoreError, MattermostWs};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

type Child = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WsCollectionError {
    #[error("websocket outer collection unconfirmed")]
    OuterJoinUnconfirmed,
    #[error("websocket recovery collection unconfirmed")]
    RecoveryJoinUnconfirmed,
    #[error("websocket collector unavailable")]
    CollectorUnavailable,
}

#[must_use = "inspect execution and collection before runtime cleanup"]
pub struct WsFinish {
    execution: Result<(), CoreError>,
    collection: Result<(), WsCollectionError>,
}
impl WsFinish {
    pub fn into_parts(self) -> (Result<(), CoreError>, Result<(), WsCollectionError>) {
        (self.execution, self.collection)
    }
}

#[must_use = "signal stop and collect before runtime cleanup"]
pub struct ManagedWs {
    stop: watch::Sender<bool>,
    receipt: oneshot::Receiver<WsFinish>,
}
impl ManagedWs {
    pub fn request_stop(&self) {
        self.stop.send_replace(true);
    }
    pub async fn finish(mut self) -> WsFinish {
        self.request_stop();
        match (&mut self.receipt).await {
            Ok(receipt) => receipt,
            Err(_) => WsFinish {
                execution: Err(failure("websocket execution completion unknown")),
                collection: Err(WsCollectionError::CollectorUnavailable),
            },
        }
    }
}
impl Drop for ManagedWs {
    fn drop(&mut self) {
        self.request_stop();
    }
}

pub(crate) fn failure(message: &'static str) -> CoreError {
    std::io::Error::other(message).into()
}

#[derive(Clone)]
pub(crate) struct Operations {
    admission: Arc<dyn RequestAdmission>,
    stop: watch::Receiver<bool>,
    error: Arc<Mutex<Option<CoreError>>>,
}
impl Operations {
    pub(crate) fn stopped(&self) -> bool {
        *self.stop.borrow() || self.stop.has_changed().is_err()
    }
    pub(crate) fn record(&self, error: CoreError) {
        let mut first = self.error.lock().expect("websocket execution result");
        if first.is_none() {
            *first = Some(error);
        }
    }
    pub(crate) fn reserve(&self) -> Result<Operation, CoreError> {
        if self.stopped() {
            return Err(CoreError::RuntimeAdmissionClosed);
        }
        match self.admission.reserve_request() {
            Ok(owner) => Ok(Operation {
                owner: Some(owner),
                operations: self.clone(),
            }),
            Err(CoreError::RuntimeAdmissionClosed) => Err(CoreError::RuntimeAdmissionClosed),
            Err(error) => {
                self.record(error);
                Err(failure("websocket operational admission failed"))
            }
        }
    }
    pub(crate) async fn delay(&self, duration: std::time::Duration) -> bool {
        if self.stopped() {
            return false;
        }
        let mut stop = self.stop.clone();
        tokio::select! {
            biased;
            _ = stop.changed() => false,
            _ = tokio::time::sleep(duration) => !self.stopped(),
        }
    }
}

pub(crate) struct Operation {
    owner: Option<Box<dyn RequestCompletion>>,
    operations: Operations,
}
impl Operation {
    pub(crate) fn complete(mut self) {
        if let Some(owner) = self.owner.take() {
            if let Err(error) = owner.confirm_done() {
                self.operations.record(error);
            }
        }
    }
    pub(crate) fn unresolved(mut self) {
        drop(self.owner.take());
        self.operations
            .record(failure("websocket author lookup completion unconfirmed"));
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        if self.owner.is_some() {
            self.operations
                .record(failure("websocket operation completion unconfirmed"));
        }
        // Dropping the consuming completion handle never confirms its owner.
    }
}

pub(crate) struct Execution {
    pub(crate) operations: Operations,
    children: mpsc::UnboundedSender<Child>,
}
impl Execution {
    pub(crate) fn register(&self, child: impl Future<Output = ()> + Send + 'static) {
        // Registration precedes polling. The sole producer cannot register
        // after its actual join; the coordinator drains the queue after sealing.
        if self.children.send(Box::pin(child)).is_err() {
            self.operations
                .record(failure("websocket recovery registration unavailable"));
        }
    }
}

/// Reserve before constructing a selected output, then retain it through the
/// actual sink send/flush. Stop never races an already-admitted send.
pub(crate) async fn send_owned<S, F>(
    execution: Option<&Execution>,
    sink: &mut S,
    message: F,
) -> Result<Result<(), S::Error>, CoreError>
where
    S: futures_util::Sink<crate::WsMessage> + Unpin,
    F: FnOnce() -> crate::WsMessage,
{
    use futures_util::SinkExt;
    let owner = crate::ws_reserve(execution)?;
    let result = sink.send(message()).await;
    crate::ws_complete(owner, crate::LookupDisposition::Returned);
    Ok(result)
}

struct Collectors(JoinSet<Result<(), tokio::task::JoinError>>);
impl Drop for Collectors {
    fn drop(&mut self) {
        // Each collector keeps the actual child join even if this coordinator
        // is lost. Losing the receipt refuses cleanup; it never aborts work.
        self.0.detach_all();
    }
}

impl MattermostWs {
    /// Start one execution. The caller must provide exclusive execution access
    /// to this websocket/state: no concurrent legacy/managed runner or external
    /// state mutation. The receipt covers this execution, not arbitrary users
    /// of the public shared state. Requires a living Tokio executor.
    pub fn spawn_managed(self: Arc<Self>, admission: Arc<dyn RequestAdmission>) -> ManagedWs {
        launch(admission, move |shutdown, execution| {
            self.run_execution(shutdown, Some(execution))
        })
    }
}

fn launch<F, Fut>(admission: Arc<dyn RequestAdmission>, work: F) -> ManagedWs
where
    F: FnOnce(watch::Receiver<bool>, Execution) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (stop, shutdown) = watch::channel(false);
    let (receipt_tx, receipt) = oneshot::channel();
    let (children, mut pending) = mpsc::unbounded_channel::<Child>();
    let operations = Operations {
        admission,
        stop: shutdown.clone(),
        error: Arc::new(Mutex::new(None)),
    };
    let execution = Execution {
        operations: operations.clone(),
        children,
    };
    // The coordinator exists before it constructs the actual outer task.
    tokio::spawn(async move {
        let mut outer = tokio::spawn(async move { work(shutdown, execution).await });
        let mut collectors = Collectors(JoinSet::new());
        let mut collection = Ok(());
        let mut joined = false;
        let mut registrations_closed = false;
        loop {
            if joined && registrations_closed && collectors.0.is_empty() {
                break;
            }
            tokio::select! {
                result = &mut outer, if !joined => {
                    joined = true;
                    if let Err(error) = result {
                        drop(error);
                        operations.record(failure("websocket outer execution unconfirmed"));
                        if collection.is_ok() { collection = Err(WsCollectionError::OuterJoinUnconfirmed); }
                    }
                    // No producer remains after actual join. Pending queued
                    // registrations are still drained before issuing receipt.
                    pending.close();
                }
                child = pending.recv(), if !registrations_closed => {
                    match child {
                        Some(child) => {
                            let actual = tokio::spawn(child);
                            collectors.0.spawn(actual);
                        }
                        None => registrations_closed = true,
                    }
                }
                result = collectors.0.join_next(), if !collectors.0.is_empty() => {
                    if !matches!(result, Some(Ok(Ok(())))) {
                        drop(result);
                        operations.record(failure("websocket recovery execution unconfirmed"));
                        if collection.is_ok() { collection = Err(WsCollectionError::RecoveryJoinUnconfirmed); }
                    }
                }
            }
        }
        let execution = operations
            .error
            .lock()
            .expect("websocket execution result")
            .take()
            .map_or(Ok(()), Err);
        let _ = receipt_tx.send(WsFinish {
            execution,
            collection,
        });
    });
    ManagedWs { stop, receipt }
}

#[cfg(test)]
mod tests;
