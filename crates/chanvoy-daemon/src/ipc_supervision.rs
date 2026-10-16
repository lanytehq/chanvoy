//! Process-owned IPC infrastructure with observed task termination.
//!
//! Idle reconnect machinery has no permanent operational arena owner. The IPC
//! request adapter owns actual requests, sends, audits and registrations. A
//! collector retains the actual child even when the enclosing daemon future
//! is dropped; cancellation alone is never returned as collected completion.

use crate::DaemonError;
use std::future::Future;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;

#[must_use = "retain until actual IPC completion before runtime cleanup"]
pub(crate) struct IpcTask {
    cancel: CancellationToken,
    completion: Option<oneshot::Receiver<Result<(), JoinError>>>,
}

impl IpcTask {
    pub(crate) fn spawn<F, Fut>(parent: &CancellationToken, work: F) -> Self
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cancel = parent.child_token();
        let child = tokio::spawn(work(cancel.clone()));
        let (send, completion) = oneshot::channel();
        tokio::spawn(async move {
            let result = child.await;
            // Final collector action: the receipt follows the actual join.
            // Losing the receiver does not abort or confirm operational work.
            let _ = send.send(result);
        });
        Self {
            cancel,
            completion: Some(completion),
        }
    }

    pub(crate) async fn finish<T>(
        mut self,
        outcome: Result<T, DaemonError>,
        cleanup: impl FnOnce() -> Result<(), DaemonError>,
    ) -> Result<T, DaemonError> {
        self.cancel.cancel();
        let collected = match self.completion.take().expect("one IPC collector").await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(DaemonError::Io(std::io::Error::other(error))),
            Err(error) => Err(DaemonError::Io(std::io::Error::other(error))),
        };
        let value = outcome?;
        collected?;
        cleanup()?;
        Ok(value)
    }
}

impl Drop for IpcTask {
    fn drop(&mut self) {
        // A dropped parent requests cooperative shutdown. The collector still
        // owns the join; no runtime artifact cleanup follows this drop path.
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests;
