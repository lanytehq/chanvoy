//! Admitted startup provider work, retained through actual child collection.

use crate::request_frontier::admission_error;
use crate::wait_owner::{AdmissionError, WaitOwnerRegistry};
use crate::{AppState, DaemonError};
use std::future::Future;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Default)]
#[must_use = "collect startup work before daemon runtime cleanup"]
pub(crate) struct StartupTasks {
    collectors: Vec<JoinHandle<Result<(), DaemonError>>>,
}

impl StartupTasks {
    pub(crate) fn spawn<F, Fut>(
        &mut self,
        registry: &Arc<WaitOwnerRegistry>,
        work: F,
    ) -> Result<bool, DaemonError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), DaemonError>> + Send + 'static,
    {
        let request = match registry.request_permit() {
            Ok(request) => request,
            Err(AdmissionError::Closed) => return Ok(false),
            Err(error) => return Err(admission_error(error)),
        };
        let (child_owner, collector_owner) = match registry.task_cleanup_pair() {
            Ok(pair) => pair,
            Err(error) => {
                // Neither child nor factory exists yet: this exact unstarted
                // request has known inverse completion, not a timeout waiver.
                request.confirm_done().map_err(admission_error)?;
                return Err(admission_error(error));
            }
        };
        let child = tokio::spawn(async move { work().await });
        let collector = tokio::spawn(async move {
            let outcome = match child.await {
                Ok(outcome) => outcome,
                Err(error) => {
                    // Drop a possible panic payload while cleanup is owned.
                    drop(error);
                    Err(
                        std::io::Error::other("startup provider task did not finish normally")
                            .into(),
                    )
                }
            };
            let outcome = match outcome {
                Ok(()) => request.confirm_done().map_err(admission_error),
                Err(error) => Err(error), // Unknown work retains its request.
            };
            let child_done = child_owner.confirm_done().map_err(admission_error);
            if child_done.is_err() {
                // An ownership mismatch cannot release the collector owner.
                return outcome.and(child_done);
            }
            // Final synchronous owned action. No await, send or spawn follows.
            let collected = collector_owner.confirm_done().map_err(admission_error);
            outcome.and(collected)
        });
        self.collectors.push(collector);
        Ok(true)
    }

    pub(crate) fn spawn_probe(&mut self, state: Arc<AppState>) -> Result<bool, DaemonError> {
        let registry = Arc::clone(&state.wait_owners);
        self.spawn(&registry, move || async move {
            let probe = state.probe_identity().await;
            tracing::info!(profile=%state.profile.name,outcome=?probe.outcome,
                "post-bind identity probe completed");
            // Every typed probe outcome is a completed assessment. This does
            // not promote remote health or identity from an unknown result.
            Ok(())
        })
    }

    pub(crate) fn spawn_migration(&mut self, state: Arc<AppState>) -> Result<bool, DaemonError> {
        let registry = Arc::clone(&state.wait_owners);
        self.spawn(&registry, move || async move {
            crate::run_attention_migration(
                state.profile.name.clone(),
                Arc::clone(&state.attention_state),
                state.client.clone(),
                crate::ATTENTION_MIGRATION_TIMEOUT,
            )
            .await
            .map_err(DaemonError::from)
        })
    }

    pub(crate) async fn finish<T>(
        self,
        mut outcome: Result<T, DaemonError>,
    ) -> Result<T, DaemonError> {
        for collector in self.collectors {
            let collected = match collector.await {
                Ok(result) => result,
                Err(error) => {
                    drop(error);
                    Err(std::io::Error::other("startup work collection unconfirmed").into())
                }
            };
            // Collect every actual task even when an earlier task/body failed.
            if outcome.is_ok() {
                if let Err(error) = collected {
                    outcome = Err(error);
                }
            }
        }
        outcome
    }
}

#[cfg(test)]
mod tests;
