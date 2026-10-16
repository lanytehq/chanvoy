//! Process-owned client connections, collected without aborting admitted work.

use crate::DaemonError;
use std::future::Future;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

#[must_use = "collect accepted clients before runtime artifact cleanup"]
pub(crate) struct ClientTasks {
    stop: CancellationToken,
    collectors: JoinSet<Result<(), DaemonError>>,
}

impl ClientTasks {
    pub(crate) fn new(parent: &CancellationToken) -> Self {
        Self {
            stop: parent.child_token(),
            collectors: JoinSet::new(),
        }
    }

    pub(crate) fn spawn<F, Fut>(&mut self, work: F)
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), DaemonError>> + Send + 'static,
    {
        let stop = self.stop.clone();
        let child = tokio::spawn(async move { work(stop).await });
        self.collectors.spawn(async move {
            match child.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => {
                    // A collected client protocol/transport failure remains a
                    // warning. Its operational owners are never confirmed here.
                    tracing::warn!(%error, "client request failed");
                    Ok(())
                }
                Err(error) => Err(join_error(error)),
            }
        });
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.collectors.is_empty()
    }

    /// Cancellation-safe while selected alongside accept/shutdown. Completed
    /// collectors are reaped during serving, not retained until daemon exit.
    pub(crate) async fn collect_next(&mut self) -> Result<(), DaemonError> {
        match self.collectors.join_next().await {
            Some(result) => result.map_err(join_error)?,
            None => Ok(()),
        }
    }

    pub(crate) async fn finish<T>(
        mut self,
        mut outcome: Result<T, DaemonError>,
    ) -> Result<T, DaemonError> {
        self.stop.cancel();
        while !self.is_empty() {
            let collected = self.collect_next().await;
            if outcome.is_ok() {
                if let Err(error) = collected {
                    outcome = Err(error);
                }
            }
        }
        outcome
    }
}

fn join_error(error: JoinError) -> DaemonError {
    // Destroy a possible panic payload before reporting collection failure.
    drop(error);
    std::io::Error::other("client task collection unconfirmed").into()
}

impl Drop for ClientTasks {
    fn drop(&mut self) {
        self.stop.cancel();
        // JoinSet normally aborts on drop. Each detached collector must retain
        // its actual child join instead; drop itself proves no completion.
        self.collectors.detach_all();
    }
}

#[cfg(test)]
mod tests;
