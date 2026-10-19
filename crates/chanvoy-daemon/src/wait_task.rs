//! Cancellation children remain owned until their actual join completes.
//! The detached collector has a separate pre-spawn cleanup owner.

use crate::wait_owner::{AdmissionError, WaitOwnerRegistry};
use chanvoy_core::CoreError;
use std::future::Future;
use std::sync::Arc;
use tokio::task::AbortHandle;

pub(crate) struct OwnedTask {
    child: AbortHandle,
    #[cfg(test)]
    collector: Option<tokio::task::JoinHandle<()>>,
}

impl OwnedTask {
    pub(crate) fn spawn<F>(registry: &Arc<WaitOwnerRegistry>, work: F) -> Result<Self, CoreError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self::spawn_inner(
            registry,
            work,
            #[cfg(test)]
            CollectorHooks::default(),
        )
    }

    fn spawn_inner<F>(
        registry: &Arc<WaitOwnerRegistry>,
        work: F,
        #[cfg(test)] hooks: CollectorHooks,
    ) -> Result<Self, CoreError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (child_owner, collector_owner) =
            registry.task_cleanup_pair().map_err(|error| match error {
                AdmissionError::Closed => CoreError::RuntimeAdmissionClosed,
                AdmissionError::Exhausted => CoreError::RuntimeOwnershipExhausted,
                AdmissionError::WrongInstance | AdmissionError::OwnerChanged => {
                    std::io::Error::other("task cleanup owner changed").into()
                }
            })?;
        // Both owners exist and the arena lock has been released before either
        // spawn. Unwinding or lost handles cannot erase these reservations.
        let child = tokio::spawn(work);
        let abort = child.abort_handle();
        let collector = tokio::spawn(async move {
            let result = child.await;
            #[cfg(test)]
            let outcome = match &result {
                Ok(()) => Terminal::Completed,
                Err(error) if error.is_cancelled() => Terminal::Canceled,
                Err(_) => Terminal::Panicked,
            };
            if let Err(error) = &result {
                if error.is_panic() {
                    tracing::warn!("wait cancellation child panicked; termination collected");
                }
            }
            // Dispose of a potential panic payload while collection is owned.
            // Joining proves termination, not successful execution.
            drop(result);
            if child_owner.confirm_done().is_err() {
                return; // Collector remains unconfirmed on ownership mismatch.
            }
            #[cfg(test)]
            {
                if let Some(joined) = hooks.joined {
                    joined.notify_one();
                }
                if let Some(release) = hooks.release {
                    release.notified().await;
                }
                if let Some(result) = hooks.result {
                    let _ = result.send(outcome);
                }
            }
            // Final synchronous owned action. No await, send, spawn or deferred
            // owned cleanup follows successful collector confirmation.
            let _ = collector_owner.confirm_done();
        });
        #[cfg(not(test))]
        drop(collector); // Detach collection, not its retained ownership.
        Ok(Self {
            child: abort,
            #[cfg(test)]
            collector: Some(collector),
        })
    }
}

impl Drop for OwnedTask {
    fn drop(&mut self) {
        self.child.abort(); // Request only; the collector still awaits the join.
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
enum Terminal {
    Completed,
    Canceled,
    Panicked,
}

#[cfg(test)]
#[derive(Default)]
struct CollectorHooks {
    joined: Option<Arc<tokio::sync::Notify>>,
    release: Option<Arc<tokio::sync::Notify>>,
    result: Option<tokio::sync::oneshot::Sender<Terminal>>,
}

#[cfg(test)]
mod tests;
